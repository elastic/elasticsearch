/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link ExchangeService} is responsible for exchanging pages between exchange sinks and sources on the same or different nodes.
 * It holds a map of {@link ExchangeSinkHandler} instances for each node in the cluster to serve {@link ExchangeRequest}s
 * To connect exchange sources to exchange sinks,
 * use {@link ExchangeSourceHandler#addRemoteSink(RemoteSink, boolean, Runnable, int, ActionListener)}.
 */
public final class ExchangeService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(ExchangeService.class);

    public static final String EXCHANGE_ACTION_NAME = "indices:data/read/esql/exchange";
    public static final String OPEN_EXCHANGE_ACTION_NAME = "indices:data/read/esql/open_exchange";

    /**
     * Legacy action names retained for backward compatibility during rolling upgrades.
     * Nodes running older versions still send and register handlers under the {@code internal:} prefix
     * (within-cluster) and the {@code cluster:internal:} prefix (cross-cluster via RCS/CPS).
     * These constants and their handler registrations can be removed once the minimum supported
     * version includes {@link ExchangeRequest#ESQL_EXCHANGE_INDICES_CONTEXT}.
     */
    public static final String LEGACY_EXCHANGE_ACTION_NAME = "internal:data/read/esql/exchange";
    public static final String LEGACY_EXCHANGE_ACTION_NAME_FOR_CCS = "cluster:internal:data/read/esql/exchange";
    public static final String LEGACY_OPEN_EXCHANGE_ACTION_NAME = "internal:data/read/esql/open_exchange";
    private static final String LEGACY_OPEN_EXCHANGE_ACTION_NAME_FOR_CCS = "cluster:internal:data/read/esql/open_exchange";

    /**
     * The time interval for an exchange sink handler to be considered inactive and subsequently
     * removed from the exchange service if no sinks are attached (i.e., no computation uses that sink handler).
     */
    public static final String INACTIVE_SINKS_INTERVAL_SETTING = "esql.exchange.sink_inactive_interval";
    public static final TimeValue INACTIVE_SINKS_INTERVAL_DEFAULT = TimeValue.timeValueMinutes(5);

    private final ThreadPool threadPool;
    private final Executor executor;
    private final BlockFactory blockFactory;

    private final Map<String, ExchangeSinkHandler> sinks = ConcurrentCollections.newConcurrentMap();
    /**
     * Tracks the set of indices that each exchange sink was opened for.
     * When an {@link ExchangeRequest} arrives to fetch pages, the indices it carries must match
     * the indices recorded here. This prevents an entity with mTLS access from consuming pages
     * from an exchange sink opened by a different user's query by guessing the session ID.
     */
    private final Map<String, Set<String>> sinkExpectedIndices = ConcurrentCollections.newConcurrentMap();
    private final Map<String, ExchangeSourceHandler> exchangeSources = ConcurrentCollections.newConcurrentMap();

    public ExchangeService(Settings settings, ThreadPool threadPool, String executorName, BlockFactory blockFactory) {
        this.threadPool = threadPool;
        this.executor = threadPool.executor(executorName);
        this.blockFactory = blockFactory;
        final var inactiveInterval = settings.getAsTime(INACTIVE_SINKS_INTERVAL_SETTING, INACTIVE_SINKS_INTERVAL_DEFAULT);
        // Run the reaper every half of the keep_alive interval
        this.threadPool.scheduleWithFixedDelay(
            new InactiveSinksReaper(threadPool, inactiveInterval),
            TimeValue.timeValueMillis(Math.max(1, inactiveInterval.millis() / 2)),
            executor
        );
    }

    public void registerTransportHandler(TransportService transportService) {
        var exchangeAction = new ExchangeTransportAction();
        var openExchangeAction = new OpenExchangeRequestHandler();
        // Register under the new indices: prefix
        transportService.registerRequestHandler(EXCHANGE_ACTION_NAME, this.executor, ExchangeRequest::new, exchangeAction);
        transportService.registerRequestHandler(OPEN_EXCHANGE_ACTION_NAME, this.executor, OpenExchangeRequest::new, openExchangeAction);
        // Register legacy handlers so that older nodes (which still send internal: or cluster:internal: actions)
        // can reach this node during a rolling upgrade. Remove once minimum version includes ESQL_EXCHANGE_INDICES_CONTEXT.
        transportService.registerRequestHandler(LEGACY_EXCHANGE_ACTION_NAME, this.executor, ExchangeRequest::new, exchangeAction);
        transportService.registerRequestHandler(LEGACY_EXCHANGE_ACTION_NAME_FOR_CCS, this.executor, ExchangeRequest::new, exchangeAction);
        transportService.registerRequestHandler(
            LEGACY_OPEN_EXCHANGE_ACTION_NAME,
            this.executor,
            OpenExchangeRequest::new,
            openExchangeAction
        );
        transportService.registerRequestHandler(
            LEGACY_OPEN_EXCHANGE_ACTION_NAME_FOR_CCS,
            this.executor,
            OpenExchangeRequest::new,
            openExchangeAction
        );
    }

    /**
     * Creates an {@link ExchangeSinkHandler} for the specified exchange id.
     *
     * @throws IllegalStateException if a sink handler for the given id already exists
     */
    public ExchangeSinkHandler createSinkHandler(String exchangeId, int maxBufferSize) {
        ExchangeSinkHandler sinkHandler = new ExchangeSinkHandler(blockFactory, maxBufferSize, threadPool.relativeTimeInMillisSupplier());
        if (sinks.putIfAbsent(exchangeId, sinkHandler) != null) {
            throw new IllegalStateException("sink exchanger for id [" + exchangeId + "] already exists");
        }
        return sinkHandler;
    }

    /**
     * Returns an exchange sink handler for the given id.
     */
    public ExchangeSinkHandler getSinkHandler(String exchangeId) {
        ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
        if (sinkHandler == null) {
            throw new ResourceNotFoundException("sink exchanger for id [{}] doesn't exist", exchangeId);
        }
        return sinkHandler;
    }

    /**
     * Validates that the given indices match the indices the exchange sink was opened for.
     * This prevents an exchange sink opened for index X from being populated by a query for index Y.
     * The check ensures that when a compute request (e.g. {@code ClusterComputeRequest} or {@code DataNodeRequest})
     * links to an exchange sink, the query's indices are consistent with the sink's expected indices.
     * <p>
     * If no expected indices were recorded for the exchange (i.e. the sink was opened by a node on a transport
     * version older than {@link ExchangeRequest#ESQL_EXCHANGE_INDICES_CONTEXT}), the check is bypassed.
     * When expected indices <em>are</em> recorded, the caller must supply non-empty query indices;
     * omitting them is treated as a lookup failure.
     *
     * @param exchangeId the exchange/session id
     * @param queryIndices the indices from the compute request
     * @throws ResourceNotFoundException if the exchange sink's indices do not match the query indices
     */
    public void validateSinkIndices(String exchangeId, String[] queryIndices) {
        final Set<String> expectedIndices = sinkExpectedIndices.get(exchangeId);
        if (expectedIndices == null) {
            // The exchange was opened by a node on a transport version that does not send indices.
            // Skip the indices check for backward compatibility.
            return;
        }
        if (queryIndices == null || queryIndices.length == 0) {
            throw new ResourceNotFoundException("exchange sink [" + exchangeId + "] not found for empty indices");
        }
        final Set<String> requestIndices = Set.copyOf(Arrays.asList(queryIndices));
        if (expectedIndices.equals(requestIndices) == false) {
            throw new ResourceNotFoundException("exchange sink [" + exchangeId + "] not found for indices " + requestIndices);
        }
    }

    /**
     * Removes the exchange sink handler associated with the given exchange id.
     * Will abort the sink handler if the given failure is not null.
     */
    public void finishSinkHandler(String exchangeId, @Nullable Exception failure) {
        final ExchangeSinkHandler sinkHandler = sinks.remove(exchangeId);
        sinkExpectedIndices.remove(exchangeId);
        if (sinkHandler != null) {
            if (failure != null) {
                sinkHandler.onFailure(failure);
            }
            assert sinkHandler.isFinished() : "Exchange sink " + exchangeId + " wasn't finished yet";
        }
    }

    /**
     * Opens a remote sink handler on the remote node for the given session ID.
     */
    public static void openExchange(
        TransportService transportService,
        Transport.Connection connection,
        String sessionId,
        int exchangeBuffer,
        String[] indices,
        IndicesOptions indicesOptions,
        Executor responseExecutor,
        ActionListener<Void> listener
    ) {
        final String actionName = connection.getTransportVersion().supports(ExchangeRequest.ESQL_EXCHANGE_INDICES_CONTEXT)
            ? OPEN_EXCHANGE_ACTION_NAME
            : LEGACY_OPEN_EXCHANGE_ACTION_NAME;
        transportService.sendRequest(
            connection,
            actionName,
            new OpenExchangeRequest(sessionId, exchangeBuffer, indices, indicesOptions),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(listener.map(unused -> null), in -> ActionResponse.Empty.INSTANCE, responseExecutor)
        );
    }

    /**
     * Remember the exchange source handler for the given session ID.
     * This can be used for async/stop requests.
     */
    public void addExchangeSourceHandler(String sessionId, ExchangeSourceHandler sourceHandler) {
        exchangeSources.put(sessionId, sourceHandler);
    }

    public ExchangeSourceHandler removeExchangeSourceHandler(String sessionId) {
        return exchangeSources.remove(sessionId);
    }

    /**
     * Finishes the session early, i.e., before all sources are finished.
     * It is called by async/stop API and should be called on the node that coordinates the async request.
     * It will close all sources and return the results - unlike cancel, this does not discard the results.
     */
    public void finishSessionEarly(String sessionId, ActionListener<Void> listener) {
        ExchangeSourceHandler exchangeSource = removeExchangeSourceHandler(sessionId);
        if (exchangeSource != null) {
            exchangeSource.finishEarly(false, listener);
        } else {
            listener.onResponse(null);
        }
    }

    static class OpenExchangeRequest extends AbstractTransportRequest implements IndicesRequest.Replaceable {
        private final String sessionId;
        private final int exchangeBuffer;
        private String[] indices;
        private final IndicesOptions indicesOptions;

        OpenExchangeRequest(String sessionId, int exchangeBuffer, String[] indices, IndicesOptions indicesOptions) {
            this.sessionId = sessionId;
            this.exchangeBuffer = exchangeBuffer;
            this.indices = indices;
            this.indicesOptions = indicesOptions;
        }

        OpenExchangeRequest(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readString();
            this.exchangeBuffer = in.readVInt();
            if (in.getTransportVersion().supports(ExchangeRequest.ESQL_EXCHANGE_INDICES_CONTEXT)) {
                this.indices = in.readStringArray();
                this.indicesOptions = IndicesOptions.readIndicesOptions(in);
            } else {
                this.indices = new String[0];
                this.indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeVInt(exchangeBuffer);
            if (out.getTransportVersion().supports(ExchangeRequest.ESQL_EXCHANGE_INDICES_CONTEXT)) {
                out.writeStringArray(indices);
                indicesOptions.writeIndicesOptions(out);
            }
        }

        @Override
        public String[] indices() {
            return indices;
        }

        @Override
        public IndicesRequest indices(String... indices) {
            this.indices = indices;
            return this;
        }

        @Override
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }
    }

    private class OpenExchangeRequestHandler implements TransportRequestHandler<OpenExchangeRequest> {
        @Override
        public void messageReceived(OpenExchangeRequest request, TransportChannel channel, Task task) throws Exception {
            createSinkHandler(request.sessionId, request.exchangeBuffer);
            if (request.indices() != null && request.indices().length > 0) {
                sinkExpectedIndices.put(request.sessionId, Set.copyOf(Arrays.asList(request.indices())));
            }
            channel.sendResponse(ActionResponse.Empty.INSTANCE);
        }
    }

    private class ExchangeTransportAction implements TransportRequestHandler<ExchangeRequest> {
        @Override
        public void messageReceived(ExchangeRequest request, TransportChannel channel, Task exchangeTask) {
            final String exchangeId = request.exchangeId();
            final ActionListener<ExchangeResponse> listener = new ChannelActionListener<>(channel);
            // Validate that the indices in the request match the indices the exchange was opened for.
            // This prevents an entity with mTLS access from consuming pages from an exchange sink
            // that belongs to another user's query by guessing the session ID.
            final Set<String> expectedIndices = sinkExpectedIndices.get(exchangeId);
            if (expectedIndices != null) {
                final Set<String> requestIndices = request.indices() != null ? Set.copyOf(Arrays.asList(request.indices())) : Set.of();
                if (expectedIndices.equals(requestIndices) == false) {
                    listener.onFailure(
                        new ResourceNotFoundException("exchange sink [" + exchangeId + "] not found for indices " + requestIndices)
                    );
                    return;
                }
            }
            final ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
            if (sinkHandler == null) {
                listener.onResponse(new ExchangeResponse(blockFactory, null, true));
            } else {
                final CancellableTask task = (CancellableTask) exchangeTask;
                task.addListener(() -> sinkHandler.onFailure(new TaskCancelledException("request cancelled " + task.getReasonCancelled())));
                sinkHandler.fetchPageAsync(request.sourcesFinished(), listener);
            }
        }
    }

    private final class InactiveSinksReaper extends AbstractRunnable {
        private final TimeValue keepAlive;
        private final ThreadPool threadPool;

        InactiveSinksReaper(ThreadPool threadPool, TimeValue keepAlive) {
            this.keepAlive = keepAlive;
            this.threadPool = threadPool;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("unexpected error when closing inactive sinks", e);
            assert false : e;
        }

        @Override
        public void onRejection(Exception e) {
            if (e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown()) {
                logger.debug("rejected execution when closing inactive sinks");
            } else {
                onFailure(e);
            }
        }

        @Override
        public boolean isForceExecution() {
            // mustn't reject this task even if the queue is full
            return true;
        }

        @Override
        protected void doRun() {
            assert Transports.assertNotTransportThread("reaping inactive exchanges can be expensive");
            assert ThreadPool.assertNotScheduleThread("reaping inactive exchanges can be expensive");
            logger.debug("start removing inactive sinks");
            final long nowInMillis = threadPool.relativeTimeInMillis();
            for (Map.Entry<String, ExchangeSinkHandler> e : sinks.entrySet()) {
                ExchangeSinkHandler sink = e.getValue();
                if (sink.hasData() && sink.hasListeners()) {
                    continue;
                }
                long elapsedInMillis = nowInMillis - sink.lastUpdatedTimeInMillis();
                if (elapsedInMillis > keepAlive.millis()) {
                    TimeValue elapsedTime = TimeValue.timeValueMillis(elapsedInMillis);
                    logger.debug("removed sink {} inactive for {}", e.getKey(), elapsedTime);
                    finishSinkHandler(
                        e.getKey(),
                        new ElasticsearchTimeoutException("Exchange sink {} has been inactive for {}", e.getKey(), elapsedTime)
                    );
                }
            }
        }
    }

    /**
     * Creates a new {@link RemoteSink} that fetches pages from an exchange sink located on the remote node.
     *
     * @param parentTask       the parent task that initialized the ESQL request
     * @param exchangeId       the exchange ID
     * @param transportService the transport service
     * @param conn             the connection to the remote node where the remote exchange sink is located
     * @param indices          the indices associated with this exchange for authorization
     * @param indicesOptions   the indices options for authorization
     */
    public RemoteSink newRemoteSink(
        Task parentTask,
        String exchangeId,
        TransportService transportService,
        Transport.Connection conn,
        String[] indices,
        IndicesOptions indicesOptions
    ) {
        return new TransportRemoteSink(transportService, blockFactory, conn, parentTask, exchangeId, indices, indicesOptions, executor);
    }

    static final class TransportRemoteSink implements RemoteSink {
        final TransportService transportService;
        final BlockFactory blockFactory;
        final Transport.Connection connection;
        final Task parentTask;
        final String exchangeId;
        final String[] indices;
        final IndicesOptions indicesOptions;
        final Executor responseExecutor;

        final AtomicLong estimatedPageSizeInBytes = new AtomicLong(0L);
        final AtomicReference<SubscribableListener<Void>> completionListenerRef = new AtomicReference<>(null);

        TransportRemoteSink(
            TransportService transportService,
            BlockFactory blockFactory,
            Transport.Connection connection,
            Task parentTask,
            String exchangeId,
            String[] indices,
            IndicesOptions indicesOptions,
            Executor responseExecutor
        ) {
            this.transportService = transportService;
            this.blockFactory = blockFactory;
            this.connection = connection;
            this.parentTask = parentTask;
            this.exchangeId = exchangeId;
            this.indices = indices;
            this.indicesOptions = indicesOptions;
            this.responseExecutor = responseExecutor;
        }

        @Override
        public void fetchPageAsync(boolean allSourcesFinished, ActionListener<ExchangeResponse> listener) {
            if (allSourcesFinished) {
                close(listener.map(unused -> new ExchangeResponse(blockFactory, null, true)));
                return;
            }
            // already finished
            SubscribableListener<Void> completionListener = completionListenerRef.get();
            if (completionListener != null) {
                completionListener.addListener(listener.map(unused -> new ExchangeResponse(blockFactory, null, true)));
                return;
            }
            doFetchPageAsync(false, ActionListener.wrap(r -> {
                if (r.finished()) {
                    completionListenerRef.compareAndSet(null, SubscribableListener.nullSuccess());
                }
                listener.onResponse(r);
            }, e -> close(ActionListener.running(() -> listener.onFailure(e)))));
        }

        private void doFetchPageAsync(boolean allSourcesFinished, ActionListener<ExchangeResponse> listener) {
            final long reservedBytes = allSourcesFinished ? 0 : estimatedPageSizeInBytes.get();
            if (reservedBytes > 0) {
                // This doesn't fully protect ESQL from OOM, but reduces the likelihood.
                try {
                    blockFactory.breaker().addEstimateBytesAndMaybeBreak(reservedBytes, "fetch page");
                } catch (Exception e) {
                    assert e instanceof CircuitBreakingException : new AssertionError(e);
                    listener.onFailure(e);
                    return;
                }
                listener = ActionListener.runAfter(listener, () -> blockFactory.breaker().addWithoutBreaking(-reservedBytes));
            }
            final String actionName = connection.getTransportVersion().supports(ExchangeRequest.ESQL_EXCHANGE_INDICES_CONTEXT)
                ? EXCHANGE_ACTION_NAME
                : LEGACY_EXCHANGE_ACTION_NAME;
            transportService.sendChildRequest(
                connection,
                actionName,
                new ExchangeRequest(exchangeId, allSourcesFinished, indices, indicesOptions),
                parentTask,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, in -> {
                    try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                        final ExchangeResponse resp = new ExchangeResponse(bsi);
                        final long responseBytes = resp.ramBytesUsedByPage();
                        estimatedPageSizeInBytes.getAndUpdate(curr -> Math.max(responseBytes, curr / 2));
                        return resp;
                    }
                }, responseExecutor)
            );
        }

        @Override
        public void close(ActionListener<Void> listener) {
            final SubscribableListener<Void> candidate = new SubscribableListener<>();
            final SubscribableListener<Void> actual = completionListenerRef.updateAndGet(
                curr -> Objects.requireNonNullElse(curr, candidate)
            );
            actual.addListener(listener);
            if (candidate == actual) {
                doFetchPageAsync(true, ActionListener.wrap(r -> {
                    final Page page = r.takePage();
                    if (page != null) {
                        page.releaseBlocks();
                    }
                    candidate.onResponse(null);
                }, e -> candidate.onResponse(null)));
            }
        }
    }

    // For testing
    public boolean isEmpty() {
        return sinks.isEmpty();
    }

    public Set<String> sinkKeys() {
        return sinks.keySet();
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        doStop();
    }

    @Override
    public String toString() {
        return "ExchangeService{" + "sinks=" + sinks.keySet() + '}';
    }
}
