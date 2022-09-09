/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.NetworkExceptionHelper;
import org.elasticsearch.common.transport.PortsRange;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.CancelledKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_BIND_HOST;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_CONTENT_LENGTH;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PORT;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_HOST;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_PUBLISH_PORT;

public abstract class AbstractHttpServerTransport extends AbstractLifecycleComponent implements HttpServerTransport {
    private static final Logger logger = LogManager.getLogger(AbstractHttpServerTransport.class);

    protected final Settings settings;
    public final HttpHandlingSettings handlingSettings;
    protected final NetworkService networkService;
    protected final Recycler<BytesRef> recycler;
    protected final ThreadPool threadPool;
    protected final Dispatcher dispatcher;
    protected final CorsHandler corsHandler;
    private final XContentParserConfiguration parserConfig;

    protected final PortsRange port;
    protected final ByteSizeValue maxContentLength;
    private final String[] bindHosts;
    private final String[] publishHosts;

    private volatile BoundTransportAddress boundAddress;
    private final AtomicLong totalChannelsAccepted = new AtomicLong();
    private final Set<HttpChannel> httpChannels = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final PlainActionFuture<Void> allClientsClosedListener = PlainActionFuture.newFuture();
    private final RefCounted refCounted = AbstractRefCounted.of(() -> allClientsClosedListener.onResponse(null));
    private final Set<HttpServerChannel> httpServerChannels = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final HttpClientStatsTracker httpClientStatsTracker;

    private final HttpTracer httpLogger;
    private final Tracer tracer;

    private volatile long slowLogThresholdMs;

    protected AbstractHttpServerTransport(
        Settings settings,
        NetworkService networkService,
        Recycler<BytesRef> recycler,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        Dispatcher dispatcher,
        ClusterSettings clusterSettings,
        Tracer tracer
    ) {
        this.settings = settings;
        this.networkService = networkService;
        this.recycler = recycler;
        this.threadPool = threadPool;
        this.parserConfig = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry)
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        this.dispatcher = dispatcher;
        this.handlingSettings = HttpHandlingSettings.fromSettings(settings);
        this.corsHandler = CorsHandler.fromSettings(settings);

        // we can't make the network.bind_host a fallback since we already fall back to http.host hence the extra conditional here
        List<String> httpBindHost = SETTING_HTTP_BIND_HOST.get(settings);
        this.bindHosts = (httpBindHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_BIND_HOST_SETTING.get(settings) : httpBindHost).toArray(
            Strings.EMPTY_ARRAY
        );
        // we can't make the network.publish_host a fallback since we already fall back to http.host hence the extra conditional here
        List<String> httpPublishHost = SETTING_HTTP_PUBLISH_HOST.get(settings);
        this.publishHosts = (httpPublishHost.isEmpty() ? NetworkService.GLOBAL_NETWORK_PUBLISH_HOST_SETTING.get(settings) : httpPublishHost)
            .toArray(Strings.EMPTY_ARRAY);

        this.port = SETTING_HTTP_PORT.get(settings);

        this.maxContentLength = SETTING_HTTP_MAX_CONTENT_LENGTH.get(settings);
        this.tracer = tracer;
        this.httpLogger = new HttpTracer(settings, clusterSettings);
        clusterSettings.addSettingsUpdateConsumer(
            TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING,
            slowLogThreshold -> this.slowLogThresholdMs = slowLogThreshold.getMillis()
        );
        slowLogThresholdMs = TransportSettings.SLOW_OPERATION_THRESHOLD_SETTING.get(settings).getMillis();
        httpClientStatsTracker = new HttpClientStatsTracker(settings, clusterSettings, threadPool);
    }

    public Recycler<BytesRef> recycler() {
        return recycler;
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
        return new HttpInfo(boundTransportAddress, maxContentLength.getBytes());
    }

    @Override
    public HttpStats stats() {
        return new HttpStats(httpClientStatsTracker.getClientStats(), httpChannels.size(), totalChannelsAccepted.get());
    }

    protected void bindServer() {
        // Bind and start to accept incoming connections.
        final InetAddress[] hostAddresses;
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
        TransportAddress publishAddress = new TransportAddress(new InetSocketAddress(publishInetAddress, publishPort));
        this.boundAddress = new BoundTransportAddress(boundAddresses.toArray(new TransportAddress[0]), publishAddress);
        logger.info("{}", boundAddress);
    }

    private TransportAddress bindAddress(final InetAddress hostAddress) {
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = port.iterate(portNumber -> {
            try {
                synchronized (httpServerChannels) {
                    HttpServerChannel httpServerChannel = bind(new InetSocketAddress(hostAddress, portNumber));
                    httpServerChannels.add(httpServerChannel);
                    boundSocket.set(httpServerChannel.getLocalAddress());
                }
            } catch (Exception e) {
                lastException.set(e);
                return false;
            }
            return true;
        });
        if (success == false) {
            throw new BindHttpException("Failed to bind to " + NetworkAddress.format(hostAddress, port), lastException.get());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Bound http to address {{}}", NetworkAddress.format(boundSocket.get()));
        }
        return new TransportAddress(boundSocket.get());
    }

    protected abstract HttpServerChannel bind(InetSocketAddress hostAddress) throws Exception;

    @Override
    protected void doStop() {
        synchronized (httpServerChannels) {
            if (httpServerChannels.isEmpty() == false) {
                try {
                    CloseableChannel.closeChannels(new ArrayList<>(httpServerChannels), true);
                } catch (Exception e) {
                    logger.warn("exception while closing channels", e);
                } finally {
                    httpServerChannels.clear();
                }
            }
        }
        try {
            refCounted.decRef();
            CloseableChannel.closeChannels(new ArrayList<>(httpChannels), true);
        } catch (Exception e) {
            logger.warn("unexpected exception while closing http channels", e);
        }

        try {
            allClientsClosedListener.get();
        } catch (Exception e) {
            assert false : e;
            logger.warn("unexpected exception while waiting for http channels to close", e);
        }
        stopInternal();
    }

    @Override
    protected void doClose() {}

    /**
     * Called to tear down internal resources
     */
    protected abstract void stopInternal();

    // package private for tests
    static int resolvePublishPort(Settings settings, List<TransportAddress> boundAddresses, InetAddress publishInetAddress) {
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
            final Set<Integer> ports = new HashSet<>();
            for (TransportAddress boundAddress : boundAddresses) {
                ports.add(boundAddress.getPort());
            }
            if (ports.size() == 1) {
                publishPort = ports.iterator().next();
            }
        }

        if (publishPort < 0) {
            throw new BindHttpException(
                "Failed to auto-resolve http publish port, multiple bound addresses "
                    + boundAddresses
                    + " with distinct ports and none of them matched the publish address ("
                    + publishInetAddress
                    + "). "
                    + "Please specify a unique port by setting "
                    + SETTING_HTTP_PORT.getKey()
                    + " or "
                    + SETTING_HTTP_PUBLISH_PORT.getKey()
            );
        }
        return publishPort;
    }

    public void onException(HttpChannel channel, Exception e) {
        try {
            if (lifecycle.started() == false) {
                // just close and ignore - we are already stopped and just need to make sure we release all resources
                return;
            }
            if (NetworkExceptionHelper.getCloseConnectionExceptionLevel(e, false) != Level.OFF) {
                logger.trace(
                    () -> format("close connection exception caught while handling client http traffic, closing connection %s", channel),
                    e
                );
            } else if (NetworkExceptionHelper.isConnectException(e)) {
                logger.trace(
                    () -> format("connect exception caught while handling client http traffic, closing connection %s", channel),
                    e
                );
            } else if (e instanceof HttpReadTimeoutException) {
                logger.trace(() -> format("http read timeout, closing connection %s", channel), e);
            } else if (e instanceof CancelledKeyException) {
                logger.trace(
                    () -> format("cancelled key exception caught while handling client http traffic, closing connection %s", channel),
                    e
                );
            } else {
                logger.warn(() -> format("caught exception while handling client http traffic, closing connection %s", channel), e);
            }
        } finally {
            CloseableChannel.closeChannel(channel);
        }
    }

    protected static void onServerException(HttpServerChannel channel, Exception e) {
        logger.error(() -> "exception from http server channel caught on transport layer [channel=" + channel + "]", e);
    }

    protected void serverAcceptedChannel(HttpChannel httpChannel) {
        boolean addedOnThisCall = httpChannels.add(httpChannel);
        assert addedOnThisCall : "Channel should only be added to http channel set once";
        refCounted.incRef();
        httpChannel.addCloseListener(ActionListener.wrap(() -> {
            httpChannels.remove(httpChannel);
            refCounted.decRef();
        }));
        totalChannelsAccepted.incrementAndGet();
        httpClientStatsTracker.addClientStats(httpChannel);
        logger.trace(() -> format("Http channel accepted: %s", httpChannel));
    }

    /**
     * This method handles an incoming http request.
     *
     * @param httpRequest that is incoming
     * @param httpChannel that received the http request
     */
    public void incomingRequest(final HttpRequest httpRequest, final HttpChannel httpChannel) {
        httpClientStatsTracker.updateClientStats(httpRequest, httpChannel);
        final long startTime = threadPool.rawRelativeTimeInMillis();
        try {
            handleIncomingRequest(httpRequest, httpChannel, httpRequest.getInboundException());
        } finally {
            final long took = threadPool.rawRelativeTimeInMillis() - startTime;
            networkService.getHandlingTimeTracker().addHandlingTime(took);
            final long logThreshold = slowLogThresholdMs;
            if (logThreshold > 0 && took > logThreshold) {
                logger.warn(
                    "handling request [{}][{}][{}][{}] took [{}ms] which is above the warn threshold of [{}ms]",
                    httpRequest.header(Task.X_OPAQUE_ID_HTTP_HEADER),
                    httpRequest.method(),
                    httpRequest.uri(),
                    httpChannel,
                    took,
                    logThreshold
                );
            }
        }
    }

    // Visible for testing
    void dispatchRequest(final RestRequest restRequest, final RestChannel channel, final Throwable badRequestCause) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            if (badRequestCause != null) {
                dispatcher.dispatchBadRequest(channel, threadContext, badRequestCause);
            } else {
                dispatcher.dispatchRequest(restRequest, channel, threadContext);
            }
        }
    }

    private void handleIncomingRequest(final HttpRequest httpRequest, final HttpChannel httpChannel, final Exception exception) {
        if (exception == null) {
            HttpResponse earlyResponse = corsHandler.handleInbound(httpRequest);
            if (earlyResponse != null) {
                httpChannel.sendResponse(earlyResponse, earlyResponseListener(httpRequest, httpChannel));
                httpRequest.release();
                return;
            }
        }

        Exception badRequestCause = exception;

        /*
         * We want to create a REST request from the incoming request from Netty. However, creating this request could fail if there
         * are incorrectly encoded parameters, or the Content-Type header is invalid. If one of these specific failures occurs, we
         * attempt to create a REST request again without the input that caused the exception (e.g., we remove the Content-Type header,
         * or skip decoding the parameters). Once we have a request in hand, we then dispatch the request as a bad request with the
         * underlying exception that caused us to treat the request as bad.
         */
        final RestRequest restRequest;
        {
            RestRequest innerRestRequest;
            try {
                innerRestRequest = RestRequest.request(parserConfig, httpRequest, httpChannel);
            } catch (final RestRequest.MediaTypeHeaderException e) {
                badRequestCause = ExceptionsHelper.useOrSuppress(badRequestCause, e);
                innerRestRequest = requestWithoutFailedHeader(httpRequest, httpChannel, badRequestCause, e.getFailedHeaderNames());
            } catch (final RestRequest.BadParameterException e) {
                badRequestCause = ExceptionsHelper.useOrSuppress(badRequestCause, e);
                innerRestRequest = RestRequest.requestWithoutParameters(parserConfig, httpRequest, httpChannel);
            }
            restRequest = innerRestRequest;
        }

        final HttpTracer maybeHttpLogger = httpLogger.maybeLogRequest(restRequest, exception);

        /*
         * We now want to create a channel used to send the response on. However, creating this channel can fail if there are invalid
         * parameter values for any of the filter_path, human, or pretty parameters. We detect these specific failures via an
         * IllegalArgumentException from the channel constructor and then attempt to create a new channel that bypasses parsing of these
         * parameter values.
         */
        final RestChannel channel;
        {
            RestChannel innerChannel;
            ThreadContext threadContext = threadPool.getThreadContext();
            try {
                innerChannel = new DefaultRestChannel(
                    httpChannel,
                    httpRequest,
                    restRequest,
                    recycler,
                    handlingSettings,
                    threadContext,
                    corsHandler,
                    maybeHttpLogger,
                    tracer
                );
            } catch (final IllegalArgumentException e) {
                badRequestCause = ExceptionsHelper.useOrSuppress(badRequestCause, e);
                final RestRequest innerRequest = RestRequest.requestWithoutParameters(parserConfig, httpRequest, httpChannel);
                innerChannel = new DefaultRestChannel(
                    httpChannel,
                    httpRequest,
                    innerRequest,
                    recycler,
                    handlingSettings,
                    threadContext,
                    corsHandler,
                    httpLogger,
                    tracer
                );
            }
            channel = innerChannel;
        }

        dispatchRequest(restRequest, channel, badRequestCause);
    }

    private RestRequest requestWithoutFailedHeader(
        HttpRequest httpRequest,
        HttpChannel httpChannel,
        Exception badRequestCause,
        Set<String> failedHeaderNames
    ) {
        assert failedHeaderNames.size() > 0;
        HttpRequest httpRequestWithoutHeader = httpRequest;
        for (String failedHeaderName : failedHeaderNames) {
            httpRequestWithoutHeader = httpRequestWithoutHeader.removeHeader(failedHeaderName);
        }
        try {
            return RestRequest.request(parserConfig, httpRequestWithoutHeader, httpChannel);
        } catch (final RestRequest.MediaTypeHeaderException e) {
            badRequestCause = ExceptionsHelper.useOrSuppress(badRequestCause, e);
            return requestWithoutFailedHeader(httpRequestWithoutHeader, httpChannel, badRequestCause, e.getFailedHeaderNames());
        } catch (final RestRequest.BadParameterException e) {
            badRequestCause.addSuppressed(e);
            return RestRequest.requestWithoutParameters(parserConfig, httpRequestWithoutHeader, httpChannel);
        }
    }

    private static ActionListener<Void> earlyResponseListener(HttpRequest request, HttpChannel httpChannel) {
        if (HttpUtils.shouldCloseConnection(request)) {
            return ActionListener.wrap(() -> CloseableChannel.closeChannel(httpChannel));
        } else {
            return ActionListener.noop();
        }
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }
}
