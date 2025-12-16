package raw

import (
	"context"
	"sync"

	"m7s.live/v5"
	"m7s.live/v5/plugin/raw/pb"
)

var _ = m7s.InstallPlugin[RawPlugin](m7s.PluginMeta{
	//DefaultYaml:         defaultConfig,
	ServiceDesc:         &pb.Api_ServiceDesc,
	RegisterGRPCHandler: pb.RegisterApiHandler,
})

type RawPlugin struct {
	pb.UnimplementedApiServer
	m7s.Plugin

	subscribers map[string]*RawSubscriber // streamPath -> subscriber
	mu          sync.RWMutex              // 保护 subscribers map
}

type RawSubscriber struct {
	streamPath string
	ctx        context.Context
	cancel     context.CancelFunc
}

func (p *RawPlugin) Start() (err error) {
	return nil
}

//// 实现 ITCPPlugin 接口
//func (p *RawPlugin) OnTCPConnect(conn *net.TCPConn) task.ITask {
//	// 手动处理 TCP 连接，将数据转发到 gRPC 服务
//	fmt.Println("111111111111111111111111111111111111")
//	p.Info("remote", conn.RemoteAddr().String())
//	return &RawTCPHandler{conn: conn, plugin: p}
//}
//
//type RawTCPHandler struct {
//	task.Work
//	conn   *net.TCPConn
//	plugin *RawPlugin
//}
//
//func (h *RawTCPHandler) Start() error {
//	// 处理 TCP 连接，可以在这里实现自定义协议
//	// 或者将数据转发到 gRPC 服务
//	return nil
//}
