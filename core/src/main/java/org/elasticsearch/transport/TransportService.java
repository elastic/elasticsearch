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

package org.elasticsearch.transport;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;

/**
 *
 */
public class TransportService extends AbstractLifecycleComponent<TransportService> {

    public static final String DIRECT_RESPONSE_PROFILE = ".direct";

    private final AtomicBoolean started = new AtomicBoolean(false);
    protected final Transport transport;
    protected final ThreadPool threadPool;

    volatile ImmutableMap<String, RequestHandlerRegistry> requestHandlers = ImmutableMap.of();
    final Object requestHandlerMutex = new Object();

    final ConcurrentMapLong<RequestHolder> clientHandlers = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    final AtomicLong requestIds = new AtomicLong();

    final CopyOnWriteArrayList<TransportConnectionListener> connectionListeners = new CopyOnWriteArrayList<>();

    // An LRU (don't really care about concurrency here) that holds the latest timed out requests so if they
    // do show up, we can print more descriptive information about them
    final Map<Long, TimeoutInfoHolder> timeoutInfoHandlers = Collections.synchronizedMap(new LinkedHashMap<Long, TimeoutInfoHolder>(100, .75F, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > 100;
        }
    });

    private final TransportService.Adapter adapter;

    // tracer log

    public static final String SETTING_TRACE_LOG_INCLUDE = "transport.tracer.include";
    public static final String SETTING_TRACE_LOG_EXCLUDE = "transport.tracer.exclude";

    private final ESLogger tracerLog;

    volatile String[] tracerLogInclude;
    volatile String[] tracelLogExclude;
    private final ApplySettings settingsListener = new ApplySettings();

    /** if set will call requests sent to this id to shortcut and executed locally */
    volatile DiscoveryNode localNode = null;

    public TransportService(Transport transport, ThreadPool threadPool) {
        this(EMPTY_SETTINGS, transport, threadPool);
    }

    @Inject
    public TransportService(Settings settings, Transport transport, ThreadPool threadPool) {
        super(settings);
        this.transport = transport;
        this.threadPool = threadPool;
        this.tracerLogInclude = settings.getAsArray(SETTING_TRACE_LOG_INCLUDE, Strings.EMPTY_ARRAY, true);
        this.tracelLogExclude = settings.getAsArray(SETTING_TRACE_LOG_EXCLUDE, new String[]{"internal:discovery/zen/fd*", TransportLivenessAction.NAME}, true);
        tracerLog = Loggers.getLogger(logger, ".tracer");
        adapter = createAdapter();
    }

    /**
     * makes the transport service aware of the local node. this allows it to optimize requests sent
     * from the local node to it self and by pass the network stack/ serialization
     */
    public void setLocalNode(DiscoveryNode localNode) {
        this.localNode = localNode;
    }

    // for testing
    DiscoveryNode getLocalNode() {
        return localNode;
    }

    protected Adapter createAdapter() {
        return new Adapter();
    }

    // These need to be optional as they don't exist in the context of a transport client
    @Inject(optional = true)
    public void setDynamicSettings(NodeSettingsService nodeSettingsService) {
        nodeSettingsService.addListener(settingsListener);
    }


    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            String[] newTracerLogInclude = settings.getAsArray(SETTING_TRACE_LOG_INCLUDE, TransportService.this.tracerLogInclude, true);
            String[] newTracerLogExclude = settings.getAsArray(SETTING_TRACE_LOG_EXCLUDE, TransportService.this.tracelLogExclude, true);
            if (newTracerLogInclude == TransportService.this.tracerLogInclude && newTracerLogExclude == TransportService.this.tracelLogExclude) {
                return;
            }
            if (Arrays.equals(newTracerLogInclude, TransportService.this.tracerLogInclude) &&
                    Arrays.equals(newTracerLogExclude, TransportService.this.tracelLogExclude)) {
                return;
            }
            TransportService.this.tracerLogInclude = newTracerLogInclude;
            TransportService.this.tracelLogExclude = newTracerLogExclude;
            logger.info("tracer log updated to use include: {}, exclude: {}", newTracerLogInclude, newTracerLogExclude);
        }
    }

    // used for testing
    public void applySettings(Settings settings) {
        settingsListener.onRefreshSettings(settings);
    }

    @Override
    protected void doStart() {
        adapter.rxMetric.clear();
        adapter.txMetric.clear();
        transport.transportServiceAdapter(adapter);
        transport.start();
        if (transport.boundAddress() != null && logger.isInfoEnabled()) {
            logger.info("{}", transport.boundAddress());
        }
        boolean setStarted = started.compareAndSet(false, true);
        assert setStarted : "service was already started";
    }

    @Override
    protected void doStop() {
        final boolean setStopped = started.compareAndSet(true, false);
        assert setStopped : "service has already been stopped";
        try {
            transport.stop();
        } finally {
            // in case the transport is not connected to our local node (thus cleaned on node disconnect)
            // make sure to clean any leftover on going handles
            for (Map.Entry<Long, RequestHolder> entry : clientHandlers.entrySet()) {
                final RequestHolder holderToNotify = clientHandlers.remove(entry.getKey());
                if (holderToNotify != null) {
                    // callback that an exception happened, but on a different thread since we don't
                    // want handlers to worry about stack overflows
                    threadPool.generic().execute(new Runnable() {
                        @Override
                        public void run() {
                            holderToNotify.handler().handleException(new TransportException("transport stopped, action: " + holderToNotify.action()));
                        }
                    });
                }
            }
        }
    }

    @Override
    protected void doClose() {
        transport.close();
    }

    public boolean addressSupported(Class<? extends TransportAddress> address) {
        return transport.addressSupported(address);
    }

    public TransportInfo info() {
        BoundTransportAddress boundTransportAddress = boundAddress();
        if (boundTransportAddress == null) {
            return null;
        }
        return new TransportInfo(boundTransportAddress, transport.profileBoundAddresses());
    }

    public TransportStats stats() {
        return new TransportStats(transport.serverOpen(), adapter.rxMetric.count(), adapter.rxMetric.sum(), adapter.txMetric.count(), adapter.txMetric.sum());
    }

    public BoundTransportAddress boundAddress() {
        return transport.boundAddress();
    }

    public List<String> getLocalAddresses() {
        return transport.getLocalAddresses();
    }

    public boolean nodeConnected(DiscoveryNode node) {
        return node.equals(localNode) || transport.nodeConnected(node);
    }

    public void connectToNode(DiscoveryNode node) throws ConnectTransportException {
        if (node.equals(localNode)) {
            return;
        }
        transport.connectToNode(node);
    }

    public void connectToNodeLight(DiscoveryNode node) throws ConnectTransportException {
        if (node.equals(localNode)) {
            return;
        }
        transport.connectToNodeLight(node);
    }

    public void disconnectFromNode(DiscoveryNode node) {
        if (node.equals(localNode)) {
            return;
        }
        transport.disconnectFromNode(node);
    }

    public void addConnectionListener(TransportConnectionListener listener) {
        connectionListeners.add(listener);
    }

    public void removeConnectionListener(TransportConnectionListener listener) {
        connectionListeners.remove(listener);
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action, TransportRequest request,
                                                                          TransportResponseHandler<T> handler) throws TransportException {
        return submitRequest(node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> TransportFuture<T> submitRequest(DiscoveryNode node, String action, TransportRequest request,
                                                                          TransportRequestOptions options, TransportResponseHandler<T> handler) throws TransportException {
        PlainTransportFuture<T> futureHandler = new PlainTransportFuture<>(handler);
        sendRequest(node, action, request, options, futureHandler);
        return futureHandler;
    }

    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action, final TransportRequest request,
                                                          final TransportResponseHandler<T> handler) {
        sendRequest(node, action, request, TransportRequestOptions.EMPTY, handler);
    }

    public <T extends TransportResponse> void sendRequest(final DiscoveryNode node, final String action, final TransportRequest request,
                                                          final TransportRequestOptions options, TransportResponseHandler<T> handler) {
        if (node == null) {
            throw new IllegalStateException("can't send request to a null node");
        }
        final long requestId = newRequestId();
        final TimeoutHandler timeoutHandler;
        try {

            if (options.timeout() == null) {
                timeoutHandler = null;
            } else {
                timeoutHandler = new TimeoutHandler(requestId);
            }
            clientHandlers.put(requestId, new RequestHolder<>(handler, node, action, timeoutHandler));
            if (started.get() == false) {
                // if we are not started the exception handling will remove the RequestHolder again and calls the handler to notify the caller.
                // it will only notify if the toStop code hasn't done the work yet.
                throw new TransportException("TransportService is closed stopped can't send request");
            }
            if (timeoutHandler != null) {
                assert options.timeout() != null;
                timeoutHandler.future = threadPool.schedule(options.timeout(), ThreadPool.Names.GENERIC, timeoutHandler);
            }
            if (node.equals(localNode)) {
                sendLocalRequest(requestId, action, request);
            } else {
                transport.sendRequest(node, requestId, action, request, options);
            }
        } catch (final Throwable e) {
            // usually happen either because we failed to connect to the node
            // or because we failed serializing the message
            final RequestHolder holderToNotify = clientHandlers.remove(requestId);
            // If holderToNotify == null then handler has already been taken care of.
            if (holderToNotify != null) {
                holderToNotify.cancelTimeout();
                // callback that an exception happened, but on a different thread since we don't
                // want handlers to worry about stack overflows
                final SendRequestTransportException sendRequestException = new SendRequestTransportException(node, action, e);
                threadPool.executor(ThreadPool.Names.GENERIC).execute(new Runnable() {
                    @Override
                    public void run() {
                        holderToNotify.handler().handleException(sendRequestException);
                    }
                });
            }
        }
    }

    private void sendLocalRequest(long requestId, final String action, final TransportRequest request) {
        final DirectResponseChannel channel = new DirectResponseChannel(logger, localNode, action, requestId, adapter, threadPool);
        try {
            final RequestHandlerRegistry reg = adapter.getRequestHandler(action);
            if (reg == null) {
                throw new ActionNotFoundTransportException("Action [" + action + "] not found");
            }
            final String executor = reg.getExecutor();
            if (ThreadPool.Names.SAME.equals(executor)) {
                //noinspection unchecked
                reg.getHandler().messageReceived(request, channel);
            } else {
                threadPool.executor(executor).execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        //noinspection unchecked
                        reg.getHandler().messageReceived(request, channel);
                    }

                    @Override
                    public boolean isForceExecution() {
                        return reg.isForceExecution();
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            channel.sendResponse(e);
                        } catch (Throwable e1) {
                            logger.warn("failed to notify channel of error message for action [" + action + "]", e1);
                            logger.warn("actual exception", e);
                        }
                    }
                });
            }

        } catch (Throwable e) {
            try {
                channel.sendResponse(e);
            } catch (Throwable e1) {
                logger.warn("failed to notify channel of error message for action [" + action + "]", e1);
                logger.warn("actual exception", e1);
            }
        }

    }

    private boolean shouldTraceAction(String action) {
        if (tracerLogInclude.length > 0) {
            if (Regex.simpleMatch(tracerLogInclude, action) == false) {
                return false;
            }
        }
        if (tracelLogExclude.length > 0) {
            return !Regex.simpleMatch(tracelLogExclude, action);
        }
        return true;
    }

    private long newRequestId() {
        return requestIds.getAndIncrement();
    }

    public TransportAddress[] addressesFromString(String address, int perAddressLimit) throws Exception {
        return transport.addressesFromString(address, perAddressLimit);
    }

    /**
     * Registers a new request handler
     * @param action The action the request handler is associated with
     * @param requestFactory a callable to be used construct new instances for streaming
     * @param executor The executor the request handling will be executed on
     * @param handler The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> requestFactory, String executor, TransportRequestHandler<Request> handler) {
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(action, requestFactory, handler, executor, false);
        registerRequestHandler(reg);
    }

    /**
     * Registers a new request handler
     * @param action The action the request handler is associated with
     * @param request The request class that will be used to constrcut new instances for streaming
     * @param executor The executor the request handling will be executed on
     * @param forceExecution Force execution on the executor queue and never reject it
     * @param handler The handler itself that implements the request handling
     */
    public <Request extends TransportRequest> void registerRequestHandler(String action, Supplier<Request> request, String executor, boolean forceExecution, TransportRequestHandler<Request> handler) {
        RequestHandlerRegistry<Request> reg = new RequestHandlerRegistry<>(action, request, handler, executor, forceExecution);
        registerRequestHandler(reg);
    }

    protected <Request extends TransportRequest> void registerRequestHandler(RequestHandlerRegistry<Request> reg) {
        synchronized (requestHandlerMutex) {
            RequestHandlerRegistry replaced = requestHandlers.get(reg.getAction());
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).put(reg.getAction(), reg).immutableMap();
            if (replaced != null) {
                logger.warn("registered two transport handlers for action {}, handlers: {}, {}", reg.getAction(), reg.getHandler(), replaced.getHandler());
            }
        }
    }

    public void removeHandler(String action) {
        synchronized (requestHandlerMutex) {
            requestHandlers = MapBuilder.newMapBuilder(requestHandlers).remove(action).immutableMap();
        }
    }

    protected RequestHandlerRegistry getRequestHandler(String action) {
        return requestHandlers.get(action);
    }

    protected class Adapter implements TransportServiceAdapter {

        final MeanMetric rxMetric = new MeanMetric();
        final MeanMetric txMetric = new MeanMetric();

        @Override
        public void received(long size) {
            rxMetric.inc(size);
        }

        @Override
        public void sent(long size) {
            txMetric.inc(size);
        }

        @Override
        public void onRequestSent(DiscoveryNode node, long requestId, String action, TransportRequest request, TransportRequestOptions options) {
            if (traceEnabled() && shouldTraceAction(action)) {
                traceRequestSent(node, requestId, action, options);
            }
        }

        protected boolean traceEnabled() {
            return tracerLog.isTraceEnabled();
        }

        @Override
        public void onResponseSent(long requestId, String action, TransportResponse response, TransportResponseOptions options) {
            if (traceEnabled() && shouldTraceAction(action)) {
                traceResponseSent(requestId, action);
            }
        }

        @Override
        public void onResponseSent(long requestId, String action, Throwable t) {
            if (traceEnabled() && shouldTraceAction(action)) {
                traceResponseSent(requestId, action, t);
            }
        }

        protected void traceResponseSent(long requestId, String action, Throwable t) {
            tracerLog.trace("[{}][{}] sent error response (error: [{}])", requestId, action, t.getMessage());
        }

        @Override
        public void onRequestReceived(long requestId, String action) {
            if (traceEnabled() && shouldTraceAction(action)) {
                traceReceivedRequest(requestId, action);
            }
        }

        @Override
        public RequestHandlerRegistry getRequestHandler(String action) {
            return requestHandlers.get(action);
        }

        @Override
        public TransportResponseHandler onResponseReceived(final long requestId) {
            RequestHolder holder = clientHandlers.remove(requestId);
            if (holder == null) {
                checkForTimeout(requestId);
                return null;
            }
            holder.cancelTimeout();
            if (traceEnabled() && shouldTraceAction(holder.action())) {
                traceReceivedResponse(requestId, holder.node(), holder.action());
            }
            return holder.handler();
        }

        protected void checkForTimeout(long requestId) {
            // lets see if its in the timeout holder, but sync on mutex to make sure any ongoing timeout handling has finished
            final DiscoveryNode sourceNode;
            final String action;
            assert clientHandlers.get(requestId) == null;
            TimeoutInfoHolder timeoutInfoHolder = timeoutInfoHandlers.remove(requestId);
            if (timeoutInfoHolder != null) {
                long time = System.currentTimeMillis();
                logger.warn("Received response for a request that has timed out, sent [{}ms] ago, timed out [{}ms] ago, action [{}], node [{}], id [{}]", time - timeoutInfoHolder.sentTime(), time - timeoutInfoHolder.timeoutTime(), timeoutInfoHolder.action(), timeoutInfoHolder.node(), requestId);
                action = timeoutInfoHolder.action();
                sourceNode = timeoutInfoHolder.node();
            } else {
                logger.warn("Transport response handler not found of id [{}]", requestId);
                action = null;
                sourceNode = null;
            }
            // call tracer out of lock
            if (traceEnabled() == false) {
                return;
            }
            if (action == null) {
                assert sourceNode == null;
                traceUnresolvedResponse(requestId);
            } else if (shouldTraceAction(action)) {
                traceReceivedResponse(requestId, sourceNode, action);
            }
        }

        @Override
        public void raiseNodeConnected(final DiscoveryNode node) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    for (TransportConnectionListener connectionListener : connectionListeners) {
                        connectionListener.onNodeConnected(node);
                    }
                }
            });
        }

        @Override
        public void raiseNodeDisconnected(final DiscoveryNode node) {
            try {
                for (final TransportConnectionListener connectionListener : connectionListeners) {
                    threadPool.generic().execute(new Runnable() {
                        @Override
                        public void run() {
                            connectionListener.onNodeDisconnected(node);
                        }
                    });
                }
                for (Map.Entry<Long, RequestHolder> entry : clientHandlers.entrySet()) {
                    RequestHolder holder = entry.getValue();
                    if (holder.node().equals(node)) {
                        final RequestHolder holderToNotify = clientHandlers.remove(entry.getKey());
                        if (holderToNotify != null) {
                            // callback that an exception happened, but on a different thread since we don't
                            // want handlers to worry about stack overflows
                            threadPool.generic().execute(new Runnable() {
                                @Override
                                public void run() {
                                    holderToNotify.handler().handleException(new NodeDisconnectedException(node, holderToNotify.action()));
                                }
                            });
                        }
                    }
                }
            } catch (EsRejectedExecutionException ex) {
                logger.debug("Rejected execution on NodeDisconnected", ex);
            }
        }

        protected void traceReceivedRequest(long requestId, String action) {
            tracerLog.trace("[{}][{}] received request", requestId, action);
        }

        protected void traceResponseSent(long requestId, String action) {
            tracerLog.trace("[{}][{}] sent response", requestId, action);
        }

        protected void traceReceivedResponse(long requestId, DiscoveryNode sourceNode, String action) {
            tracerLog.trace("[{}][{}] received response from [{}]", requestId, action, sourceNode);
        }

        protected void traceUnresolvedResponse(long requestId) {
            tracerLog.trace("[{}] received response but can't resolve it to a request", requestId);
        }

        protected void traceRequestSent(DiscoveryNode node, long requestId, String action, TransportRequestOptions options) {
            tracerLog.trace("[{}][{}] sent to [{}] (timeout: [{}])", requestId, action, node, options.timeout());
        }

    }

    class TimeoutHandler implements Runnable {

        private final long requestId;

        private final long sentTime = System.currentTimeMillis();

        volatile ScheduledFuture future;

        TimeoutHandler(long requestId) {
            this.requestId = requestId;
        }

        @Override
        public void run() {
            // we get first to make sure we only add the TimeoutInfoHandler if needed.
            final RequestHolder holder = clientHandlers.get(requestId);
            if (holder != null) {
                // add it to the timeout information holder, in case we are going to get a response later
                long timeoutTime = System.currentTimeMillis();
                timeoutInfoHandlers.put(requestId, new TimeoutInfoHolder(holder.node(), holder.action(), sentTime, timeoutTime));
                // now that we have the information visible via timeoutInfoHandlers, we try to remove the request id
                final RequestHolder removedHolder = clientHandlers.remove(requestId);
                if (removedHolder != null) {
                    assert removedHolder == holder : "two different holder instances for request [" + requestId + "]";
                    removedHolder.handler().handleException(new ReceiveTimeoutTransportException(holder.node(), holder.action(), "request_id [" + requestId + "] timed out after [" + (timeoutTime - sentTime) + "ms]"));
                } else {
                    // response was processed, remove timeout info.
                    timeoutInfoHandlers.remove(requestId);
                }
            }
        }

        /**
         * cancels timeout handling. this is a best effort only to avoid running it. remove the requestId from {@link #clientHandlers}
         * to make sure this doesn't run.
         */
        public void cancel() {
            assert clientHandlers.get(requestId) == null : "cancel must be called after the requestId [" + requestId + "] has been removed from clientHandlers";
            FutureUtils.cancel(future);
        }
    }

    static class TimeoutInfoHolder {

        private final DiscoveryNode node;
        private final String action;
        private final long sentTime;
        private final long timeoutTime;

        TimeoutInfoHolder(DiscoveryNode node, String action, long sentTime, long timeoutTime) {
            this.node = node;
            this.action = action;
            this.sentTime = sentTime;
            this.timeoutTime = timeoutTime;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String action() {
            return action;
        }

        public long sentTime() {
            return sentTime;
        }

        public long timeoutTime() {
            return timeoutTime;
        }
    }

    static class RequestHolder<T extends TransportResponse> {

        private final TransportResponseHandler<T> handler;

        private final DiscoveryNode node;

        private final String action;

        private final TimeoutHandler timeoutHandler;

        RequestHolder(TransportResponseHandler<T> handler, DiscoveryNode node, String action, TimeoutHandler timeoutHandler) {
            this.handler = handler;
            this.node = node;
            this.action = action;
            this.timeoutHandler = timeoutHandler;
        }

        public TransportResponseHandler<T> handler() {
            return handler;
        }

        public DiscoveryNode node() {
            return this.node;
        }

        public String action() {
            return this.action;
        }

        public void cancelTimeout() {
            if (timeoutHandler != null) {
                timeoutHandler.cancel();
            }
        }
    }

    static class DirectResponseChannel implements TransportChannel {
        final ESLogger logger;
        final DiscoveryNode localNode;
        final private String action;
        final private long requestId;
        final TransportServiceAdapter adapter;
        final ThreadPool threadPool;

        public DirectResponseChannel(ESLogger logger, DiscoveryNode localNode, String action, long requestId, TransportServiceAdapter adapter, ThreadPool threadPool) {
            this.logger = logger;
            this.localNode = localNode;
            this.action = action;
            this.requestId = requestId;
            this.adapter = adapter;
            this.threadPool = threadPool;
        }

        @Override
        public String action() {
            return action;
        }

        @Override
        public String getProfileName() {
            return DIRECT_RESPONSE_PROFILE;
        }

        @Override
        public void sendResponse(TransportResponse response) throws IOException {
            sendResponse(response, TransportResponseOptions.EMPTY);
        }

        @Override
        public void sendResponse(final TransportResponse response, TransportResponseOptions options) throws IOException {
            final TransportResponseHandler handler = adapter.onResponseReceived(requestId);
            // ignore if its null, the adapter logs it
            if (handler != null) {
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processResponse(handler, response);
                } else {
                    threadPool.executor(executor).execute(new Runnable() {
                        @SuppressWarnings({"unchecked"})
                        @Override
                        public void run() {
                            processResponse(handler, response);
                        }
                    });
                }
            }
        }

        @SuppressWarnings("unchecked")
        protected void processResponse(TransportResponseHandler handler, TransportResponse response) {
            try {
                handler.handleResponse(response);
            } catch (Throwable e) {
                processException(handler, wrapInRemote(new ResponseHandlerFailureTransportException(e)));
            }
        }

        @Override
        public void sendResponse(Throwable error) throws IOException {
            final TransportResponseHandler handler = adapter.onResponseReceived(requestId);
            // ignore if its null, the adapter logs it
            if (handler != null) {
                final RemoteTransportException rtx = wrapInRemote(error);
                final String executor = handler.executor();
                if (ThreadPool.Names.SAME.equals(executor)) {
                    processException(handler, rtx);
                } else {
                    threadPool.executor(handler.executor()).execute(new Runnable() {
                        @SuppressWarnings({"unchecked"})
                        @Override
                        public void run() {
                            processException(handler, rtx);
                        }
                    });
                }
            }
        }

        protected RemoteTransportException wrapInRemote(Throwable t) {
            if (t instanceof RemoteTransportException) {
                return (RemoteTransportException) t;
            }
            return new RemoteTransportException(localNode.name(), localNode.getAddress(), action, t);
        }

        protected void processException(final TransportResponseHandler handler, final RemoteTransportException rtx) {
            try {
                handler.handleException(rtx);
            } catch (Throwable e) {
                logger.error("failed to handle exception for action [{}], handler [{}]", e, action, handler);
            }
        }
    }

}
