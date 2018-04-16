package org.elasticsearch.http.nio;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.netty4.AbstractHttpServerTransport;
import org.elasticsearch.nio.AcceptingSelector;
import org.elasticsearch.nio.AcceptorEventHandler;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.NioGroup;
import org.elasticsearch.nio.NioServerSocketChannel;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.SocketEventHandler;
import org.elasticsearch.nio.SocketSelector;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.nio.NioTransport;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_BIND_HOST;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_INITIAL_LINE_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PORT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_RESET_COOKIES;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_KEEP_ALIVE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_NO_DELAY;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_REUSE_ADDRESS;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_TCP_SEND_BUFFER_SIZE;

public class NioHttpServerTransport extends AbstractHttpServerTransport {

    private static final String TRANSPORT_WORKER_THREAD_NAME_PREFIX = "http_nio_transport_worker";
    private static final String TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX = "http_nio_transport_acceptor";

    private final BigArrays bigArrays;
    private final ThreadPool threadPool;
    private final NamedXContentRegistry xContentRegistry;
    private final Dispatcher dispatcher;

    private final int maxChunkSize;
    private final int maxHeaderSize;
    private final int maxInitialLineLength;
    private final boolean resetCookies;
    private final boolean detailedErrorsEnabled;
    private final boolean compression;
    private final int compressionLevel;

    private final PortsRange port;
    private final String[] bindHosts;
    private final String[] publishHosts;
    private final boolean tcpNoDelay;
    private final boolean tcpKeepAlive;
    private final boolean reuseAddress;
    private final int tcpSendBufferSize;
    private final int tcpReceiveBufferSize;

    private NioGroup nioGroup;
    private final List<NioServerSocketChannel> serverChannels = new ArrayList<>();

    public NioHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, ThreadPool threadPool,
                                  NamedXContentRegistry xContentRegistry, HttpServerTransport.Dispatcher dispatcher) {
        super(settings, networkService, threadPool, dispatcher);
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        this.dispatcher = dispatcher;

        this.maxChunkSize = Math.toIntExact(SETTING_HTTP_MAX_CHUNK_SIZE.get(settings).getBytes());
        this.maxHeaderSize = Math.toIntExact(SETTING_HTTP_MAX_HEADER_SIZE.get(settings).getBytes());
        this.maxInitialLineLength =  Math.toIntExact(SETTING_HTTP_MAX_INITIAL_LINE_LENGTH.get(settings).getBytes());
        this.resetCookies = SETTING_HTTP_RESET_COOKIES.get(settings);
        this.detailedErrorsEnabled = SETTING_HTTP_DETAILED_ERRORS_ENABLED.get(settings);
        this.compression = SETTING_HTTP_COMPRESSION.get(settings);
        this.compressionLevel = SETTING_HTTP_COMPRESSION_LEVEL.get(settings);

//        this.workerCount = SETTING_HTTP_WORKER_COUNT.get(settings);
        this.port = SETTING_HTTP_PORT.get(settings);
        // we can't make the network.bind_host a fallback since we already fall back to http.host hence the extra conditional here
        List<String> httpBindHost = SETTING_HTTP_BIND_HOST.get(settings);
        this.bindHosts = (httpBindHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_BINDHOST_SETTING.get(settings) : httpBindHost)
            .toArray(Strings.EMPTY_ARRAY);
        // we can't make the network.publish_host a fallback since we already fall back to http.host hence the extra conditional here
        List<String> httpPublishHost = SETTING_HTTP_PUBLISH_HOST.get(settings);
        this.publishHosts = (httpPublishHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_PUBLISHHOST_SETTING.get(settings)
            : httpPublishHost).toArray(Strings.EMPTY_ARRAY);
        this.tcpNoDelay = SETTING_HTTP_TCP_NO_DELAY.get(settings);
        this.tcpKeepAlive = SETTING_HTTP_TCP_KEEP_ALIVE.get(settings);
        this.reuseAddress = SETTING_HTTP_TCP_REUSE_ADDRESS.get(settings);
        this.tcpSendBufferSize = Math.toIntExact(SETTING_HTTP_TCP_SEND_BUFFER_SIZE.get(settings).getBytes());
        this.tcpReceiveBufferSize = Math.toIntExact(SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE.get(settings).getBytes());


        logger.debug("using max_chunk_size[{}], max_header_size[{}], max_initial_line_length[{}], max_content_length[{}]",
            maxChunkSize, maxHeaderSize, maxInitialLineLength, maxContentLength);
    }



    void dispatchBadRequest(NioHttpRequest httpRequest, NioHttpChannel channel, Throwable cause) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            dispatcher.dispatchBadRequest(httpRequest, channel, threadContext, cause);
        }
    }

    void dispatchRequest(NioHttpRequest httpRequest, NioHttpChannel channel) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            dispatcher.dispatchRequest(httpRequest, channel, threadContext);
        }
    }

    BigArrays getBigArrays() {
        return bigArrays;
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            int acceptorCount = 0;
            nioGroup = new NioGroup(logger, daemonThreadFactory(this.settings, TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX), acceptorCount,
                AcceptorEventHandler::new, daemonThreadFactory(this.settings, TRANSPORT_WORKER_THREAD_NAME_PREFIX),
                NioTransport.NIO_WORKER_COUNT.get(settings), SocketEventHandler::new);
//            this.serverOpenChannels = new Netty4OpenChannelsHandler(logger);

//            serverBootstrap = new ServerBootstrap();
//
//            serverBootstrap.group(new NioEventLoopGroup(workerCount, daemonThreadFactory(settings,
//                HTTP_SERVER_WORKER_THREAD_NAME_PREFIX)));
//            serverBootstrap.channel(io.netty.channel.socket.nio.NioServerSocketChannel.class);
//
//            serverBootstrap.childHandler(configureServerChannelHandler());
//
//            serverBootstrap.childOption(ChannelOption.TCP_NODELAY, SETTING_HTTP_TCP_NO_DELAY.get(settings));
//            serverBootstrap.childOption(ChannelOption.SO_KEEPALIVE, SETTING_HTTP_TCP_KEEP_ALIVE.get(settings));
//
//            final ByteSizeValue tcpSendBufferSize = SETTING_HTTP_TCP_SEND_BUFFER_SIZE.get(settings);
//            if (tcpSendBufferSize.getBytes() > 0) {
//                serverBootstrap.childOption(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.getBytes()));
//            }
//
//            final ByteSizeValue tcpReceiveBufferSize = SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE.get(settings);
//            if (tcpReceiveBufferSize.getBytes() > 0) {
//                serverBootstrap.childOption(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.getBytes()));
//            }
//
//            serverBootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);
//            serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR, recvByteBufAllocator);
//
//            final boolean reuseAddress = SETTING_HTTP_TCP_REUSE_ADDRESS.get(settings);
//            serverBootstrap.option(ChannelOption.SO_REUSEADDR, reuseAddress);
//            serverBootstrap.childOption(ChannelOption.SO_REUSEADDR, reuseAddress);

            if (logger.isInfoEnabled()) {
                logger.info("{}", boundAddress);
            }
            success = true;
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        } finally {
            if (success == false) {
                doStop(); // otherwise we leak threads since we never moved to started
            }
        }

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {}

    @Override
    protected TransportAddress bindAddress(InetAddress hostAddress) {
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = port.iterate(portNumber -> {
            try {
                synchronized (serverChannels) {
                    NioServerSocketChannel channel = nioGroup.bindServerChannel(new InetSocketAddress(hostAddress, portNumber), null);
                    serverChannels.add(channel);
                    boundSocket.set(channel.getLocalAddress());
                }
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
            return true;
        });
        if (!success) {
            throw new BindHttpException("Failed to bind to [" + port.getPortRangeString() + "]", lastException.get());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Bound http to address {{}}", NetworkAddress.format(boundSocket.get()));
        }
        return new TransportAddress(boundSocket.get());
    }

    @Override
    public HttpStats stats() {
        return new HttpStats(0, 0);
    }

    private class HttpChannelFactory extends ChannelFactory<NioServerSocketChannel, NioSocketChannel> {

        private HttpChannelFactory() {
            super(new RawChannelFactory(tcpNoDelay, tcpKeepAlive, reuseAddress, tcpSendBufferSize, tcpReceiveBufferSize));
        }

        @Override
        public NioSocketChannel createChannel(SocketSelector selector, SocketChannel channel) throws IOException {
            return null;
        }

        @Override
        public NioServerSocketChannel createServerChannel(AcceptingSelector selector, ServerSocketChannel channel) throws IOException {
            return null;
        }
    }
}
