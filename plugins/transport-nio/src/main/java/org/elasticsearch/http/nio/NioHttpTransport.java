/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.http.nio;

import com.carrotsearch.hppc.IntHashSet;
import com.carrotsearch.hppc.IntSet;
import io.netty.handler.timeout.ReadTimeoutException;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.http.BindHttpException;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.http.netty4.Netty4HttpRequest;
import org.elasticsearch.http.netty4.Netty4HttpServerTransport;
import org.elasticsearch.http.netty4.cors.Netty4CorsConfig;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.nio.AcceptingSelector;
import org.elasticsearch.transport.nio.NioSelectors;
import org.elasticsearch.transport.nio.NioShutdown;
import org.elasticsearch.transport.nio.NioTransport;
import org.elasticsearch.transport.nio.OpenChannels;
import org.elasticsearch.transport.nio.SocketEventHandler;
import org.elasticsearch.transport.nio.SocketSelector;
import org.elasticsearch.transport.nio.channel.ChannelFactory;
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;
import org.elasticsearch.transport.nio.channel.TcpWriteContext;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.common.settings.Setting.boolSetting;
import static org.elasticsearch.common.settings.Setting.intSetting;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_BIND_HOST;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_DETAILED_ERRORS_ENABLED;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PORT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_PORT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_RESET_COOKIES;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_PIPELINING;
import static org.elasticsearch.transport.nio.NioTransport.TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX;
import static org.elasticsearch.transport.nio.NioTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX;

public class NioHttpTransport extends AbstractLifecycleComponent implements HttpServerTransport {

    public static final Setting<Integer> NIO_HTTP_WORKER_COUNT =
        new Setting<>("transport.nio.http.worker_count",
            (s) -> Integer.toString(EsExecutors.numberOfProcessors(s) * 2),
            (s) -> Setting.parseInt(s, 1, "transport.nio.worker_count"), Setting.Property.NodeScope);
    public static final Setting<Integer> NIO_HTTP_ACCEPTOR_COUNT =
        intSetting("transport.nio.http.acceptor_count", 1, 1, Setting.Property.NodeScope);
    public static final Setting<Boolean> SETTING_HTTP_TCP_NO_DELAY =
        boolSetting("http.tcp_no_delay", NetworkService.TCP_NO_DELAY, Setting.Property.NodeScope);
    public static final Setting<Boolean> SETTING_HTTP_TCP_KEEP_ALIVE =
        boolSetting("http.tcp.keep_alive", NetworkService.TCP_KEEP_ALIVE, Setting.Property.NodeScope);
    public static final Setting<Boolean> SETTING_HTTP_TCP_REUSE_ADDRESS =
        boolSetting("http.tcp.reuse_address", NetworkService.TCP_REUSE_ADDRESS, Setting.Property.NodeScope);
    public static final Setting<ByteSizeValue> SETTING_HTTP_TCP_SEND_BUFFER_SIZE =
        Setting.byteSizeSetting("http.tcp.send_buffer_size", NetworkService.TCP_SEND_BUFFER_SIZE, Setting.Property.NodeScope);
    public static final Setting<ByteSizeValue> SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE =
        Setting.byteSizeSetting("http.tcp.receive_buffer_size", NetworkService.TCP_RECEIVE_BUFFER_SIZE, Setting.Property.NodeScope);

    private final Netty4CorsConfig corsConfig;
    private final PortsRange port;
    private final NetworkService networkService;
    private final BigArrays bigArrays;
    private final ThreadPool threadPool;
    private final NamedXContentRegistry xContentRegistry;
    private final Dispatcher dispatcher;
    private final String[] publishHosts;
    private final String bindHosts[];
    private final int maxContentLength;
    private final boolean tcpNoDelay;
    private final boolean tcpKeepAlive;
    private final boolean tcpReuseAddress;
    private final int tcpSendBufferSize;
    private final int tcpReceiveBufferSize;
    private final boolean resetCookies;
    private final boolean detailedErrorsEnabled;
    private final boolean pipelining;

    private int acceptorNumber;
    private OpenChannels openChannels;
    private ArrayList<SocketSelector> socketSelectors;
    private ArrayList<AcceptingSelector> acceptors;
    private BoundTransportAddress boundAddress;
    private volatile Consumer<NioSocketChannel> contextSetter;

    public NioHttpTransport(Settings settings, NetworkService networkService, BigArrays bigArrays, ThreadPool threadPool,
                            NamedXContentRegistry xContentRegistry, Dispatcher dispatcher) {
        super(settings);
        this.networkService = networkService;
        this.bigArrays = bigArrays;
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        this.dispatcher = dispatcher;
        List<String> httpBindHost = SETTING_HTTP_BIND_HOST.get(settings);
        this.bindHosts = (httpBindHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_BINDHOST_SETTING.get(settings) : httpBindHost)
            .toArray(Strings.EMPTY_ARRAY);
        this.port = SETTING_HTTP_PORT.get(settings);
        // we can't make the network.publish_host a fallback since we already fall back to http.host hence the extra conditional here
        List<String> httpPublishHost = SETTING_HTTP_PUBLISH_HOST.get(settings);
        this.publishHosts = (httpPublishHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_PUBLISHHOST_SETTING.get(settings) : httpPublishHost)
            .toArray(Strings.EMPTY_ARRAY);
        maxContentLength = Math.toIntExact(SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings).getBytes());
        detailedErrorsEnabled = SETTING_HTTP_DETAILED_ERRORS_ENABLED.get(settings);
        pipelining = SETTING_PIPELINING.get(settings);
        resetCookies = SETTING_HTTP_RESET_COOKIES.get(settings);
        corsConfig = Netty4CorsConfig.buildCorsConfig(settings);

        tcpNoDelay = SETTING_HTTP_TCP_NO_DELAY.get(settings);
        tcpKeepAlive = SETTING_HTTP_TCP_KEEP_ALIVE.get(settings);
        tcpReuseAddress = SETTING_HTTP_TCP_REUSE_ADDRESS.get(settings);
        tcpSendBufferSize = Math.toIntExact(SETTING_HTTP_TCP_SEND_BUFFER_SIZE.get(settings).getBytes());
        tcpReceiveBufferSize = Math.toIntExact(SETTING_HTTP_TCP_RECEIVE_BUFFER_SIZE.get(settings).getBytes());
    }

    @Override
    protected void doStart() {
        boolean success = false;
        try {
            NioHttpNettyAdaptor nettyAdaptor = new NioHttpNettyAdaptor(logger, settings, this::exceptionCaught, corsConfig,
                maxContentLength);
            NioHttpRequestHandler handler = new NioHttpRequestHandler(this, xContentRegistry, detailedErrorsEnabled,
                threadPool.getThreadContext(), pipelining, corsConfig, resetCookies);
            contextSetter = (c) -> {
                ESEmbeddedChannel adaptor = nettyAdaptor.getAdaptor(c);
                c.setContexts(new HttpReadContext(c, adaptor, handler), new HttpWriteContext(c, adaptor));
            };

            this.openChannels = new OpenChannels(logger);

            socketSelectors = NioSelectors.socketSelectors(settings, () -> new SocketEventHandler(logger, this::exceptionCaught),
                NIO_HTTP_WORKER_COUNT.get(settings), NioTransport.TRANSPORT_WORKER_THREAD_NAME_PREFIX);

            acceptors = NioSelectors.acceptingSelectors(logger, settings, openChannels, socketSelectors,
                NIO_HTTP_ACCEPTOR_COUNT.get(settings), NioTransport.TRANSPORT_ACCEPTOR_THREAD_NAME_PREFIX);

            this.boundAddress = createBoundHttpAddress();
            if (logger.isInfoEnabled()) {
                logger.info("{}", boundAddress);
            }
            success = true;
        } finally {
            if (success == false) {
                doStop(); // otherwise we leak threads since we never moved to started
            }
        }
    }

    @Override
    protected void doStop() {
        NioShutdown nioShutdown = new NioShutdown(logger);
        nioShutdown.orderlyShutdown(openChannels, null, acceptors, socketSelectors);
    }

    @Override
    protected void doClose() throws IOException {
    }

    @Override
    public BoundTransportAddress boundAddress() {
        return this.boundAddress;
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
        long serverChannelsCount = openChannels.serverChannelsCount();
        return new HttpStats(openChannels == null ? 0 : serverChannelsCount, openChannels == null
            ? 0 : openChannels.getAcceptedChannels().size() + serverChannelsCount);
    }

    public void dispatchRequest(Netty4HttpRequest httpRequest, RestChannel channel) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            dispatcher.dispatchRequest(httpRequest, channel, threadContext);
        }
    }

    public void dispatchBadRequest(Netty4HttpRequest httpRequest, RestChannel channel, Throwable cause) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            dispatcher.dispatchBadRequest(httpRequest, channel, threadContext, cause);
        }
    }

    protected void exceptionCaught(NioSocketChannel channel, Throwable cause) {
        // UH? Read timeout logged as connection? Also no read timeout handler currently
        if (cause instanceof ReadTimeoutException) {
            if (logger.isTraceEnabled()) {
                logger.trace("Connection timeout [{}]", channel.getRemoteAddress());
            }
            channel.closeAsync();
        } else {
            if (!lifecycle.started()) {
                // ignore
                return;
            }
            // TODO: Does channel need to implement toString?
            if (!NetworkExceptionHelper.isCloseConnectionException(cause)) {
                logger.warn(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "caught exception while handling client http traffic, closing connection {}", channel),
                    cause);
                channel.closeAsync();
            } else {
                logger.debug(
                    (Supplier<?>) () -> new ParameterizedMessage(
                        "caught exception while handling client http traffic, closing connection {}", channel),
                    cause);
                channel.closeAsync();
            }
        }
    }

    private BoundTransportAddress createBoundHttpAddress() {
        // Bind and start to accept incoming connections.
        InetAddress hostAddresses[];
        try {
            hostAddresses = networkService.resolveBindHostAddresses(bindHosts);
        } catch (IOException e) {
            throw new BindHttpException("Failed to resolve host [" + Arrays.toString(bindHosts) + "]", e);
        }

        List<TransportAddress> boundAddresses = new ArrayList<>(hostAddresses.length);
        for (InetAddress address : hostAddresses) {
            boundAddresses.add(bindAddress(address));
        }

        final InetAddress publishInetAddress;
        try {
            publishInetAddress = networkService.resolvePublishHostAddresses(publishHosts);
        } catch (Exception e) {
            throw new BindTransportException("Failed to resolve publish address", e);
        }

        final int publishPort = resolvePublishPort(settings, boundAddresses, publishInetAddress);
        final InetSocketAddress publishAddress = new InetSocketAddress(publishInetAddress, publishPort);
        return new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[0]), new TransportAddress(publishAddress));
    }

    private static int resolvePublishPort(Settings settings, List<TransportAddress> boundAddresses, InetAddress publishInetAddress) {
        int publishPort = SETTING_HTTP_PUBLISH_PORT.get(settings);

        if (publishPort < 0) {
            for (TransportAddress boundAddress : boundAddresses) {
                InetAddress boundInetAddress = boundAddress.address().getAddress();
                if (boundInetAddress.isAnyLocalAddress() || boundInetAddress.equals(publishInetAddress)) {
                    publishPort = boundAddress.getPort();
                    break;
                }
            }
        }

        // if no matching boundAddress found, check if there is a unique port for all bound addresses
        if (publishPort < 0) {
            final IntSet ports = new IntHashSet();
            for (TransportAddress boundAddress : boundAddresses) {
                ports.add(boundAddress.getPort());
            }
            if (ports.size() == 1) {
                publishPort = ports.iterator().next().value;
            }
        }

        if (publishPort < 0) {
            throw new BindHttpException("Failed to auto-resolve http publish port, multiple bound addresses " + boundAddresses +
                " with distinct ports and none of them matched the publish address (" + publishInetAddress + "). " +
                "Please specify a unique port by setting " + SETTING_HTTP_PORT.getKey() + " or " + SETTING_HTTP_PUBLISH_PORT.getKey());
        }
        return publishPort;
    }

    private TransportAddress bindAddress(final InetAddress hostAddress) {
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = port.iterate(portNumber -> {
            try {
                synchronized (this) {
                    AcceptingSelector selector = acceptors.get(++acceptorNumber % NioTransport.NIO_ACCEPTOR_COUNT.get(settings));
                    InetSocketAddress address = new InetSocketAddress(hostAddress, portNumber);

                    ChannelFactory.RawChannelFactory rawChannelFactory = new ChannelFactory.RawChannelFactory(tcpNoDelay, tcpKeepAlive,
                        tcpReuseAddress, tcpSendBufferSize, tcpReceiveBufferSize);
                    ChannelFactory channelFactory = new ChannelFactory(rawChannelFactory, contextSetter);
                    NioServerSocketChannel serverChannel = channelFactory.openNioServerSocketChannel("http-server", address, selector);
                    boundSocket.set(serverChannel.getLocalAddress());
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
}
