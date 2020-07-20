package io.chank.reactor.seata.core.rpc;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.core.rpc.RemotingBootstrap;
import io.seata.core.rpc.netty.NettyServerConfig;
import io.seata.core.rpc.netty.v1.ProtocolV1Decoder;
import io.seata.core.rpc.netty.v1.ProtocolV1Encoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.tcp.TcpServer;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

public class NettyServerBootstrap implements RemotingBootstrap {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyServerBootstrap.class);

    private final EventLoopGroup eventLoopGroupWorker;
    private final EventLoopGroup eventLoopGroupBoss;
    private final NettyServerConfig nettyServerConfig;
    private ChannelHandler[] channelHandlers;
    private int listenPort;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private final TcpServer server = TcpServer.create();

    public NettyServerBootstrap(NettyServerConfig nettyServerConfig) {
        this.nettyServerConfig = nettyServerConfig;
        if (NettyServerConfig.enableEpoll()) {
            this.eventLoopGroupBoss = new EpollEventLoopGroup(nettyServerConfig.getBossThreadSize(), new NamedThreadFactory(nettyServerConfig.getBossThreadPrefix(), nettyServerConfig.getBossThreadSize()));
            this.eventLoopGroupWorker = new EpollEventLoopGroup(nettyServerConfig.getServerWorkerThreads(), new NamedThreadFactory(nettyServerConfig.getWorkerThreadPrefix(), nettyServerConfig.getServerWorkerThreads()));
        } else {
            this.eventLoopGroupBoss = new NioEventLoopGroup(nettyServerConfig.getBossThreadSize(), new NamedThreadFactory(nettyServerConfig.getBossThreadPrefix(), nettyServerConfig.getBossThreadSize()));
            this.eventLoopGroupWorker = new NioEventLoopGroup(nettyServerConfig.getServerWorkerThreads(), new NamedThreadFactory(nettyServerConfig.getWorkerThreadPrefix(), nettyServerConfig.getServerWorkerThreads()));
        }

        this.listenPort = nettyServerConfig.getDefaultListenPort();
    }

    public void start() {
        this.server
                .bootstrap(b -> b.option(ChannelOption.SO_BACKLOG, nettyServerConfig.getSoBackLogSize()))
                .bootstrap(b -> b.option(ChannelOption.SO_REUSEADDR, true))
                .bootstrap(b -> b.childOption(ChannelOption.SO_KEEPALIVE, true))
                .bootstrap(b -> b.childOption(ChannelOption.TCP_NODELAY, true))
                .bootstrap(b -> b.childOption(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSendBufSize()))
                .bootstrap(b -> b.childOption(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketResvBufSize()))
                .bootstrap(b -> b.childOption(ChannelOption.WRITE_BUFFER_WATER_MARK,
                        new WriteBufferWaterMark(nettyServerConfig.getWriteBufferLowWaterMark(),
                                nettyServerConfig.getWriteBufferHighWaterMark())))
                .bootstrap(b -> b.localAddress(new InetSocketAddress(listenPort)))
                .bootstrap(b -> b.childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new IdleStateHandler(nettyServerConfig.getChannelMaxReadIdleSeconds(), 0, 0))
                                .addLast(new ProtocolV1Decoder())
                                .addLast(new ProtocolV1Encoder());
                        if (null != channelHandlers) {
                            ch.pipeline().addLast(channelHandlers);
                        }
                    }
                }));
    }

    public void shutdown() {
    }

}
