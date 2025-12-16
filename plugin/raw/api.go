package raw

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/gobwas/ws"
	"github.com/langhuihui/gomem"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"m7s.live/v5"
	"m7s.live/v5/pb"
	"m7s.live/v5/pkg/codec"
	"m7s.live/v5/pkg/format"
	rawpb "m7s.live/v5/plugin/raw/pb"
)

// 编译期检查：确保 *RawPlugin 实现了 pb.ApiServer
var _ rawpb.ApiServer = (*RawPlugin)(nil)

func (p *RawPlugin) SubscribeRaw(req *rawpb.SubscribeRawRequest, stream rawpb.Api_SubscribeRawServer) error {
	ctx := stream.Context()
	_, ok := p.Server.Streams.SafeGet(req.StreamPath)
	if !ok {
		p.Error("stream not found:", "streamPath", req.StreamPath)
		return status.Errorf(codes.NotFound, "stream not found: %s", req.StreamPath)
	}

	subscriber, err := p.Subscribe(ctx, req.StreamPath)
	if err != nil {
		return status.Errorf(codes.Unknown, "subscribe unknown error: %s", req.StreamPath)
	}

	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// 创建并注册 RawSubscriber
	rawSub := &RawSubscriber{
		streamPath: req.StreamPath,
		ctx:        subCtx,
		cancel:     cancel,
	}

	// 注册到插件管理
	p.mu.Lock()
	if p.subscribers == nil {
		p.subscribers = make(map[string]*RawSubscriber)
	}
	p.subscribers[req.StreamPath] = rawSub
	p.mu.Unlock()

	// 确保在函数结束时清理
	defer func() {
		p.mu.Lock()
		delete(p.subscribers, req.StreamPath)
		p.mu.Unlock()
	}()

	write := func(frameType rawpb.FrameType, ts int64, rawdata []byte, isIdr bool, cctx codec.ICodecCtx) error {
		select {
		case <-subCtx.Done():
			return subCtx.Err() // 返回错误以停止 PlayBlock
		default:
		}

		var codecInfo *rawpb.CodecInfo
		// 获取编码格式和分辨率信息
		if cctx != nil {
			switch _ctx := cctx.(type) {
			case *codec.H264Ctx:
				codecInfo = &rawpb.CodecInfo{
					CodecName: "h264",
					Width:     int64(_ctx.Width()),
					Height:    int64(_ctx.Height()),
					Fps:       int64(_ctx.FPS()),
				}
			case *codec.H265Ctx:
				codecInfo = &rawpb.CodecInfo{
					CodecName: "h265",
					Width:     int64(_ctx.Width()),
					Height:    int64(_ctx.Height()),
					Fps:       int64(_ctx.FPS()),
				}
			default:

			}
		}

		resp := &rawpb.RawFrame{
			StreamPath: req.StreamPath,
			FrameType:  frameType,
			Timestamp:  ts,
			NalData:    rawdata,
			IsKeyframe: isIdr,
			CodecInfo:  codecInfo,
			Size:       int64(len(rawdata)),
		}
		err = stream.Send(resp)
		if err != nil {
			p.Error(err.Error())
		}
		return err
	}

	err = m7s.PlayBlock(subscriber, func(audio *format.RawAudio) error {
		return nil
	}, func(video *format.AnnexB) (err error) {
		return write(2, time.Now().UTC().UnixMicro(), video.ToBytes(), video.IDR, video.ICodecCtx)
	})

	if err != nil {
		if err == context.Canceled {
			p.Info("stream canceled by client", "streamPath", req.StreamPath)
		} else {
			p.Error("stream error", "streamPath", req.StreamPath, "error", err)
		}
	}
	return err
}

func (p *RawPlugin) UnsubscribeRaw(ctx context.Context, req *rawpb.SubscribeRawRequest) (*pb.SuccessResponse, error) {
	if req.StreamPath == "" {
		return nil, status.Error(codes.InvalidArgument, "streamPath cannot be empty")
	}

	p.mu.RLock()
	rawSub, exists := p.subscribers[req.StreamPath]
	p.mu.RUnlock()

	if !exists {
		return nil, status.Errorf(codes.NotFound, "stream not subscribe: %s", req.StreamPath)
	}

	// 取消该订阅的 context
	// 1. subCtx 的 Done channel 被关闭
	//<-subCtx.Done()  // 立即返回
	// 2. subCtx.Err() 变为非 nil
	//err := subCtx.Err()  // 返回 context.Canceled
	// 3. 所有派生自 subCtx 的子 context 同时被取消
	//    (subSubCtx, subSubSubCtx 等全部收到取消信号)
	// 4. 与该 context 关联的 timer/timeout 被清理
	rawSub.cancel()
	return &pb.SuccessResponse{}, nil
}

func (p *RawPlugin) UnsubscribeAllRaw(ctx context.Context, req *emptypb.Empty) (*pb.SuccessResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, rawSub := range p.subscribers {
		rawSub.cancel()
	}
	return &pb.SuccessResponse{}, nil
}

func (p *RawPlugin) GetSubscribeRawList(ctx context.Context, req *emptypb.Empty) (*rawpb.SubscribeListResponse, error) {
	p.mu.RLock()
	streamPaths := make([]string, 0, len(p.subscribers))
	for streamPath := range p.subscribers {
		streamPaths = append(streamPaths, streamPath)
	}
	p.mu.RUnlock()

	// 按字母顺序排序（可选，便于阅读）
	sort.Strings(streamPaths)

	return &rawpb.SubscribeListResponse{
		StreamPath: streamPaths,
	}, nil
}

func (p *RawPlugin) RegisterHandler() map[string]http.HandlerFunc {
	return map[string]http.HandlerFunc{
		"/{streamPath...}": p.wsraw,
	}
}

func (p *RawPlugin) wsraw(rw http.ResponseWriter, r *http.Request) {
	subscriber, err := p.Subscribe(r.Context(), r.PathValue("streamPath"))
	defer func() {
		if err != nil {
			http.Error(rw, err.Error(), http.StatusInternalServerError)
		}
	}()
	if err != nil {
		return
	}

	var conn net.Conn
	conn, err = subscriber.CheckWebSocket(rw, r)
	if err != nil {
		return
	}
	if conn == nil {
		err = errors.New("no websocket connection.")
		return
	}

	var sendBuffer = net.Buffers{}
	write := func(mem gomem.Memory) (err error) {
		err = ws.WriteHeader(conn, ws.Header{
			Fin:    true,
			OpCode: ws.OpBinary,
			Length: int64(mem.Size),
		})
		if err != nil {
			return
		}

		sendBuffer = append(sendBuffer, mem.Buffers...)
		_, err = sendBuffer.WriteTo(conn)
		if err != nil {
			p.Error(err.Error())
		}
		return err
	}

	m7s.PlayBlock(subscriber, func(audio *format.RawAudio) (err error) {
		return nil
	}, func(video *format.AnnexB) (err error) {
		return write(video.Memory)
	})
}
