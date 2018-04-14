package org.elasticsearch.http.nio;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.nio.AcceptingSelector;
import org.elasticsearch.nio.ChannelFactory;
import org.elasticsearch.nio.NioServerSocketChannel;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.SocketSelector;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_BIND_HOST;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_COMPRESSION_LEVEL;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CHUNK_SIZE;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
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

public class NioHttpServerTransport extends AbstractLifecycleComponent implements HttpServerTransport {

    private final NetworkService networkService;
    private final BigArrays bigArrays;
    private final ThreadPool threadPool;
    private final NamedXContentRegistry xContentRegistry;
    private final Dispatcher dispatcher;

    private final int maxContentLength;
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

    public NioHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, ThreadPool threadPool,
                                  NamedXContentRegistry xContentRegistry, HttpServerTransport.Dispatcher dispatcher) {
        super(settings);
        this.networkService = networkService;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        this.dispatcher = dispatcher;

        this.maxContentLength = Math.toIntExact(SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).getBytes());
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

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {

    }

    @Override
    public BoundTransportAddress boundAddress() {
        return null;
    }

    @Override
    public HttpInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new HttpInfo(boundTransportAddress, maxContentLength);
    }

    @Override
    public HttpStats stats() {
        return null;
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
