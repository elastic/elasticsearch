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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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

    // TODO: Make this a child action of the data node transport to ensure that exchanges
    // are accessed only by the user initialized the session.
    public static final String EXCHANGE_ACTION_NAME = "internal:data/read/esql/exchange";
    public static final String EXCHANGE_ACTION_NAME_FOR_CCS = "cluster:internal:data/read/esql/exchange";

    public static final String OPEN_EXCHANGE_ACTION_NAME = "internal:data/read/esql/open_exchange";
    private static final String OPEN_EXCHANGE_ACTION_NAME_FOR_CCS = "cluster:internal:data/read/esql/open_exchange";

    public static final String BATCH_EXCHANGE_STATUS_ACTION_NAME = "internal:data/read/esql/batch_exchange_status";

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
     * Exchange ids removed because the coordinator sent {@code sourcesFinished}. A compute request that has not yet
     * resolved its sink treats this as "do not start a driver" rather than a missing exchange. Entries expire so a
     * node does not retain one id per early-finished query. The deadline is fixed, not the inactive-sink interval:
     * tests shorten that interval to a few seconds, which is shorter than a data-node request can take to arrive.
     */
    private final Map<String, Long> sourcesFinishedSinks = ConcurrentCollections.newConcurrentMap();
    private static final long SOURCES_FINISHED_REMEMBER_NANOS = TimeUnit.MINUTES.toNanos(5);
    /**
     * From this version on, a coordinator does not open the exchange sink on a data node ahead of its compute request:
     * the data node creates the sink when the compute request arrives ({@link #createSinkHandlerIfNotFinished}), and a
     * fetch flagged {@link ExchangeRequest#waitForSink()} that gets there first waits for it ({@link #parkFetch}).
     */
    public static final TransportVersion SINK_CREATED_BY_DATA_REQUEST = TransportVersion.fromName("esql_data_node_exchange_protocol");
    /**
     * Exchange ids whose sink finished and was removed, kept for a while so that a {@link ExchangeRequest#waitForSink()}
     * fetch that arrives after the removal, typically the one sent right after the last page, is told the sink is finished
     * rather than parked for a sink that will never be created again. Entries expire; see {@link #FINISHED_REMEMBER_NANOS}.
     */
    private final Map<String, Long> finishedSinks = ConcurrentCollections.newConcurrentMap();
    private static final long FINISHED_REMEMBER_NANOS = TimeUnit.MINUTES.toNanos(1);
    /** Fetches waiting for their sink to be created by the compute request; guarded by {@link #pendingFetchesLock}. */
    private final Map<String, List<PendingFetch>> pendingFetches = new HashMap<>();
    private final Object pendingFetchesLock = new Object();

    private record PendingFetch(ActionListener<ExchangeResponse> listener, long receivedNanos) {}

    private final Map<String, ExchangeSourceHandler> exchangeSources = ConcurrentCollections.newConcurrentMap();
    private final Map<String, LocalExchange> localExchanges = ConcurrentCollections.newConcurrentMap();
    // Registry for bidirectional batch exchange servers, keyed by serverToClientId
    private final Map<String, BidirectionalBatchExchangeServer> batchExchangeServers = ConcurrentCollections.newConcurrentMap();

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
        transportService.registerRequestHandler(EXCHANGE_ACTION_NAME, this.executor, ExchangeRequest::new, new ExchangeTransportAction());
        transportService.registerRequestHandler(
            OPEN_EXCHANGE_ACTION_NAME,
            this.executor,
            OpenExchangeRequest::new,
            new OpenExchangeRequestHandler()
        );

        // This allows the system user access this action when executed over CCS and the API key based security model is in use
        transportService.registerRequestHandler(
            EXCHANGE_ACTION_NAME_FOR_CCS,
            this.executor,
            ExchangeRequest::new,
            new ExchangeTransportAction()
        );
        transportService.registerRequestHandler(
            OPEN_EXCHANGE_ACTION_NAME_FOR_CCS,
            this.executor,
            OpenExchangeRequest::new,
            new OpenExchangeRequestHandler()
        );

        // Register batch exchange status handler once (singleton pattern)
        // This handler routes requests to the appropriate server based on exchangeId
        transportService.registerRequestHandler(
            BATCH_EXCHANGE_STATUS_ACTION_NAME,
            this.executor,
            BatchExchangeStatusRequest::new,
            new TransportRequestHandler<BatchExchangeStatusRequest>() {
                @Override
                public void messageReceived(BatchExchangeStatusRequest request, TransportChannel channel, Task task) throws Exception {
                    final String exchangeId = request.exchangeId();
                    BidirectionalBatchExchangeServer server = batchExchangeServers.get(exchangeId);
                    if (server != null) {
                        server.handleBatchExchangeStatusRequest(request, channel, task);
                    } else {
                        logger.warn("Received BatchExchangeStatusRequest for unknown exchangeId={}", exchangeId);
                        channel.sendResponse(
                            new BatchExchangeStatusResponse(new ResourceNotFoundException("exchange [{}] not found", exchangeId))
                        );
                    }
                }
            }
        );
    }

    /**
     * Register a bidirectional batch exchange server.
     * The server will receive BatchExchangeStatusRequest messages for its exchangeId.
     */
    public void registerBatchExchangeServer(String serverToClientId, BidirectionalBatchExchangeServer server) {
        if (batchExchangeServers.putIfAbsent(serverToClientId, server) != null) {
            throw new IllegalStateException("batch exchange server for id [" + serverToClientId + "] already exists");
        }
    }

    /**
     * Unregister a bidirectional batch exchange server.
     */
    public void unregisterBatchExchangeServer(String serverToClientId) {
        batchExchangeServers.remove(serverToClientId);
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
        attachPendingFetches(exchangeId, sinkHandler);
        return sinkHandler;
    }

    /**
     * The sink for a compute request that creates its own sink (see {@link #SINK_CREATED_BY_DATA_REQUEST}): the sink
     * already open for {@code exchangeId} when the coordinator opened it ahead of the request, a new one otherwise, or
     * {@code null} when the coordinator has already finished this exchange, in which case the caller completes its
     * request empty and starts no driver, as with {@link #getSinkHandlerIfOpen}.
     */
    @Nullable
    public ExchangeSinkHandler createSinkHandlerIfNotFinished(String exchangeId, int maxBufferSize) {
        ExchangeSinkHandler existing = sinks.get(exchangeId);
        if (existing != null) {
            return existing;
        }
        if (sourcesFinishedSinks.containsKey(exchangeId)) {
            finishPendingFetches(exchangeId);
            return null;
        }
        ExchangeSinkHandler sinkHandler = new ExchangeSinkHandler(blockFactory, maxBufferSize, threadPool.relativeTimeInMillisSupplier());
        ExchangeSinkHandler raced = sinks.putIfAbsent(exchangeId, sinkHandler);
        if (raced != null) {
            return raced;
        }
        attachPendingFetches(exchangeId, sinkHandler);
        return sinkHandler;
    }

    /** Hands the fetches that arrived before the sink to the sink that now exists. */
    private void attachPendingFetches(String exchangeId, ExchangeSinkHandler sinkHandler) {
        final List<PendingFetch> waiting;
        synchronized (pendingFetchesLock) {
            waiting = pendingFetches.remove(exchangeId);
        }
        if (waiting != null) {
            for (PendingFetch fetch : waiting) {
                sinkHandler.fetchPageAsync(false, fetch.listener());
            }
        }
    }

    /**
     * Holds a fetch for a sink that the compute request of the same exchange has not created yet. The fetch is served
     * as soon as the sink is created, fails when the fetch's task is cancelled, and times out with the inactive sink
     * interval otherwise, so a compute request that never arrives cannot pin it forever.
     */
    private void parkFetch(String exchangeId, ActionListener<ExchangeResponse> listener, CancellableTask task) {
        final ActionListener<ExchangeResponse> once = ActionListener.notifyOnce(listener);
        final PendingFetch fetch = new PendingFetch(once, System.nanoTime());
        synchronized (pendingFetchesLock) {
            // Re-check under the lock: a sink created, or finished, between the caller's lookup and here drains the parked
            // fetches under this lock, so a fetch parked after that would never be answered.
            ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
            if (sinkHandler != null) {
                sinkHandler.fetchPageAsync(false, once);
                return;
            }
            if (mustWaitForSink(exchangeId) == false) {
                once.onResponse(new ExchangeResponse(blockFactory, null, true));
                return;
            }
            pendingFetches.computeIfAbsent(exchangeId, k -> new ArrayList<>()).add(fetch);
        }
        task.addListener(() -> {
            if (removePendingFetch(exchangeId, fetch)) {
                once.onFailure(new TaskCancelledException("request cancelled " + task.getReasonCancelled()));
            }
        });
    }

    /** Answers the fetches parked for an exchange that will not get a sink anymore. */
    private void finishPendingFetches(String exchangeId) {
        final List<PendingFetch> waiting;
        synchronized (pendingFetchesLock) {
            waiting = pendingFetches.remove(exchangeId);
        }
        if (waiting != null) {
            for (PendingFetch fetch : waiting) {
                fetch.listener().onResponse(new ExchangeResponse(blockFactory, null, true));
            }
        }
    }

    private boolean removePendingFetch(String exchangeId, PendingFetch fetch) {
        synchronized (pendingFetchesLock) {
            List<PendingFetch> waiting = pendingFetches.get(exchangeId);
            if (waiting == null || waiting.remove(fetch) == false) {
                return false;
            }
            if (waiting.isEmpty()) {
                pendingFetches.remove(exchangeId);
            }
            return true;
        }
    }

    private void expirePendingFetches(long keepAliveNanos) {
        final long now = System.nanoTime();
        final List<PendingFetch> expired = new ArrayList<>();
        synchronized (pendingFetchesLock) {
            for (var it = pendingFetches.entrySet().iterator(); it.hasNext();) {
                var entry = it.next();
                entry.getValue().removeIf(fetch -> {
                    if (now - fetch.receivedNanos() > keepAliveNanos) {
                        expired.add(fetch);
                        return true;
                    }
                    return false;
                });
                if (entry.getValue().isEmpty()) {
                    it.remove();
                }
            }
        }
        for (PendingFetch fetch : expired) {
            fetch.listener().onFailure(new ElasticsearchTimeoutException("Exchange sink was not created in time for a waiting fetch"));
        }
    }

    /**
     * Whether a fetch flagged {@link ExchangeRequest#waitForSink()} for an exchange with no sink must wait for the sink:
     * yes unless the sink has already finished, or the coordinator finished the exchange before the sink was created.
     */
    private boolean mustWaitForSink(String exchangeId) {
        return finishedSinks.containsKey(exchangeId) == false && sourcesFinishedSinks.containsKey(exchangeId) == false;
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
     * The open sink for {@code exchangeId}, or {@code null} when the coordinator already finished it with
     * {@code sourcesFinished}. {@code null} means the caller must complete its compute request successfully and
     * must not start a driver: the coordinator has stopped reading, and failing here with
     * {@link ResourceNotFoundException} would replace the original query error.
     * <p>
     * A sink that was never opened still throws {@link ResourceNotFoundException}. The marker is left in place
     * until it expires, so a transport retry of the same compute request also completes empty.
     */
    @Nullable
    public ExchangeSinkHandler getSinkHandlerIfOpen(String exchangeId) {
        ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
        if (sinkHandler != null) {
            return sinkHandler;
        }
        if (sourcesFinishedSinks.containsKey(exchangeId)) {
            return null;
        }
        throw new ResourceNotFoundException("sink exchanger for id [{}] doesn't exist", exchangeId);
    }

    /**
     * Gets an existing {@link ExchangeSinkHandler} for the specified exchange id, or creates one if it doesn't exist.
     * This is useful when the sink handler may have been pre-registered (e.g., for test setup coordination).
     */
    public ExchangeSinkHandler getOrCreateSinkHandler(String exchangeId, int maxBufferSize) {
        return sinks.computeIfAbsent(
            exchangeId,
            id -> new ExchangeSinkHandler(blockFactory, maxBufferSize, threadPool.relativeTimeInMillisSupplier())
        );
    }

    /**
     * Removes the exchange sink handler associated with the given exchange id.
     * W will abort the sink handler if the given failure is not null.
     */
    public void finishSinkHandler(String exchangeId, @Nullable Exception failure) {
        // Recorded before the removal so that a fetch which no longer finds the sink always finds this record (see
        // mustWaitForSink), then any fetch parked in between is finished as well.
        finishedSinks.put(exchangeId, System.nanoTime() + FINISHED_REMEMBER_NANOS);
        final ExchangeSinkHandler sinkHandler = sinks.remove(exchangeId);
        finishPendingFetches(exchangeId);
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
        Executor responseExecutor,
        ActionListener<Void> listener
    ) {
        transportService.sendRequest(
            connection,
            OPEN_EXCHANGE_ACTION_NAME,
            new OpenExchangeRequest(sessionId, exchangeBuffer),
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

    public void addLocalExchange(String sessionId, LocalExchange exchange) {
        if (localExchanges.putIfAbsent(sessionId, exchange) != null) {
            throw new IllegalStateException("local exchange for session [" + sessionId + "] already exists");
        }
    }

    public LocalExchange removeLocalExchange(String sessionId) {
        return localExchanges.remove(sessionId);
    }

    /**
     * Finishes the session early, i.e., before all sources are finished.
     * It is called by async/stop API and should be called on the node that coordinates the async request.
     * It will close all sources and return the results - unlike cancel, this does not discard the results.
     *
     * @param listener receives {@code true} when an active exchange source was found and closed by this call,
     *                 {@code false} when no session was registered (typically because the query already finished).
     *                 Callers use this to distinguish "STOP actually interrupted execution" from "STOP was a no-op
     *                 because the query had already completed".
     */
    public void finishSessionEarly(String sessionId, ActionListener<Boolean> listener) {
        ExchangeSourceHandler exchangeSource = removeExchangeSourceHandler(sessionId);
        if (exchangeSource != null) {
            exchangeSource.finishEarly(false, listener.map(v -> Boolean.TRUE));
            return;
        }
        LocalExchange localExchange = removeLocalExchange(sessionId);
        if (localExchange != null) {
            localExchange.finish(false);
            listener.onResponse(Boolean.TRUE);
            return;
        }
        listener.onResponse(Boolean.FALSE);
    }

    private static class OpenExchangeRequest extends AbstractTransportRequest {
        private final String sessionId;
        private final int exchangeBuffer;

        OpenExchangeRequest(String sessionId, int exchangeBuffer) {
            this.sessionId = sessionId;
            this.exchangeBuffer = exchangeBuffer;
        }

        OpenExchangeRequest(StreamInput in) throws IOException {
            super(in);
            this.sessionId = in.readString();
            this.exchangeBuffer = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sessionId);
            out.writeVInt(exchangeBuffer);
        }
    }

    private class OpenExchangeRequestHandler implements TransportRequestHandler<OpenExchangeRequest> {
        @Override
        public void messageReceived(OpenExchangeRequest request, TransportChannel channel, Task task) throws Exception {
            createSinkHandler(request.sessionId, request.exchangeBuffer);
            channel.sendResponse(ActionResponse.Empty.INSTANCE);
        }
    }

    private class ExchangeTransportAction implements TransportRequestHandler<ExchangeRequest> {
        @Override
        public void messageReceived(ExchangeRequest request, TransportChannel channel, Task exchangeTask) {
            final String exchangeId = request.exchangeId();
            ActionListener<ExchangeResponse> listener = new ChannelActionListener<>(channel);
            final ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
            if (sinkHandler == null) {
                if (request.waitForSink() && request.sourcesFinished()) {
                    // The coordinator is done before the compute request created the sink: the request must not create it
                    // (createSinkHandlerIfNotFinished), and fetches parked for it are finished right here.
                    rememberSourcesFinished(exchangeId);
                    finishPendingFetches(exchangeId);
                    listener.onResponse(new ExchangeResponse(blockFactory, null, true));
                } else if (request.waitForSink() && mustWaitForSink(exchangeId)) {
                    parkFetch(exchangeId, listener, (CancellableTask) exchangeTask);
                } else {
                    listener.onResponse(new ExchangeResponse(blockFactory, null, true));
                }
            } else {
                final CancellableTask task = (CancellableTask) exchangeTask;
                task.addListener(() -> sinkHandler.onFailure(new TaskCancelledException("request cancelled " + task.getReasonCancelled())));
                // Remember the id before the fetch runs. fetchPageAsync(sourcesFinished) finishes the buffer and can
                // remove the handler from a completion listener on this thread; a compute request arriving in that
                // window must still see the marker rather than a sink that was never opened.
                if (request.sourcesFinished()) {
                    rememberSourcesFinished(request.exchangeId());
                }
                sinkHandler.fetchPageAsync(request.sourcesFinished(), listener);
                if (request.sourcesFinished()) {
                    ExchangeSinkHandler current = sinks.get(exchangeId);
                    // Null failure: the buffer is already finished, and onFailure would notify listeners again.
                    if (current != null && current.isFinished()) {
                        finishSinkHandler(exchangeId, null);
                    }
                }
            }
        }
    }

    private void rememberSourcesFinished(String exchangeId) {
        sourcesFinishedSinks.put(exchangeId, System.nanoTime() + SOURCES_FINISHED_REMEMBER_NANOS);
    }

    private void expireSourcesFinishedSinks() {
        long now = System.nanoTime();
        sourcesFinishedSinks.entrySet().removeIf(entry -> entry.getValue() <= now);
        finishedSinks.entrySet().removeIf(entry -> entry.getValue() <= now);
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
            expireSourcesFinishedSinks();
            expirePendingFetches(keepAlive.nanos());
        }
    }

    /**
     * Creates a new {@link RemoteSink} that fetches pages from an exchange sink located on the remote node.
     *
     * @param parentTask       the parent task that initialized the ESQL request
     * @param exchangeId       the exchange ID
     * @param transportService the transport service
     * @param conn             the connection to the remote node where the remote exchange sink is located
     */
    public RemoteSink newRemoteSink(Task parentTask, String exchangeId, TransportService transportService, Transport.Connection conn) {
        return newRemoteSink(parentTask, exchangeId, transportService, conn, false);
    }

    /**
     * Same as {@link #newRemoteSink(Task, String, TransportService, Transport.Connection)}, except that when {@code inProcessIfLocal}
     * is set and the connection targets this node, pages are fetched from the sink directly instead of through the transport layer.
     * Callers keep the transport path when they need every fetch to be visible to the transport layer, for instance when a
     * query pragma turns the same-node shortcuts off.
     */
    public RemoteSink newRemoteSink(
        Task parentTask,
        String exchangeId,
        TransportService transportService,
        Transport.Connection conn,
        boolean inProcessIfLocal
    ) {
        return newRemoteSink(parentTask, exchangeId, transportService, conn, inProcessIfLocal, false);
    }

    /**
     * Same as {@link #newRemoteSink(Task, String, TransportService, Transport.Connection, boolean)}; {@code sinkCreatedByCompute}
     * flags the fetches with {@link ExchangeRequest#waitForSink()} because the remote sink is created by the compute request
     * rather than opened ahead of it (see {@link #SINK_CREATED_BY_DATA_REQUEST}).
     */
    public RemoteSink newRemoteSink(
        Task parentTask,
        String exchangeId,
        TransportService transportService,
        Transport.Connection conn,
        boolean inProcessIfLocal,
        boolean sinkCreatedByCompute
    ) {
        boolean local = inProcessIfLocal
            && transportService.getLocalNode() != null
            && transportService.getLocalNode().equals(conn.getNode());
        return new TransportRemoteSink(
            transportService,
            blockFactory,
            conn,
            parentTask,
            exchangeId,
            executor,
            local ? this : null,
            sinkCreatedByCompute
        );
    }

    /**
     * Serves a page fetch for a sink on this node without going through the transport layer: the same steps as
     * {@link ExchangeTransportAction#messageReceived} minus the per-request child task, for a coordinator that is also
     * the data node. The caller's own task cancellation still reaches the sink through the compute running on it.
     */
    void fetchPageLocally(String exchangeId, boolean sourcesFinished, ActionListener<ExchangeResponse> listener) {
        final ExchangeSinkHandler sinkHandler = sinks.get(exchangeId);
        if (sinkHandler == null) {
            listener.onResponse(new ExchangeResponse(blockFactory, null, true));
            return;
        }
        if (sourcesFinished) {
            rememberSourcesFinished(exchangeId);
        }
        sinkHandler.fetchPageAsync(sourcesFinished, listener);
        if (sourcesFinished) {
            ExchangeSinkHandler current = sinks.get(exchangeId);
            if (current != null && current.isFinished()) {
                finishSinkHandler(exchangeId, null);
            }
        }
    }

    static final class TransportRemoteSink implements RemoteSink {
        final TransportService transportService;
        final BlockFactory blockFactory;
        final Transport.Connection connection;
        final Task parentTask;
        final String exchangeId;
        final Executor responseExecutor;
        /** Non-null when the sink lives on this node: pages are then fetched in-process instead of over the transport. */
        @Nullable
        final ExchangeService localService;
        final boolean waitForSink;

        final AtomicLong estimatedPageSizeInBytes = new AtomicLong(0L);
        final AtomicReference<SubscribableListener<Void>> completionListenerRef = new AtomicReference<>(null);
        // Guarded by `this`: the fetches whose response is still outstanding, the pages received so far, and the total
        // the remote side announced through allPagesSent (-1 until it does).
        private final Map<Long, ActionListener<ExchangeResponse>> inFlight = new HashMap<>();
        private long nextFetchId = 0;
        private long pagesReceived = 0;
        private long expectedPages = -1;

        TransportRemoteSink(
            TransportService transportService,
            BlockFactory blockFactory,
            Transport.Connection connection,
            Task parentTask,
            String exchangeId,
            Executor responseExecutor,
            @Nullable ExchangeService localService,
            boolean waitForSink
        ) {
            this.waitForSink = waitForSink;
            this.transportService = transportService;
            this.blockFactory = blockFactory;
            this.connection = connection;
            this.parentTask = parentTask;
            this.exchangeId = exchangeId;
            this.responseExecutor = responseExecutor;
            this.localService = localService;
        }

        @Override
        public void allPagesSent(long pages) {
            final List<ActionListener<ExchangeResponse>> toFinish;
            synchronized (this) {
                expectedPages = pages;
                if (pagesReceived < pages) {
                    return;
                }
                toFinish = new ArrayList<>(inFlight.values());
                inFlight.clear();
            }
            // Every page has arrived: the fetches still in flight can only bring back the finished flag, so answer them here
            // and let their eventual responses go.
            completionListenerRef.compareAndSet(null, SubscribableListener.nullSuccess());
            for (ActionListener<ExchangeResponse> l : toFinish) {
                l.onResponse(new ExchangeResponse(blockFactory, null, true));
            }
        }

        /**
         * Tracks the fetch so that {@link #allPagesSent} can answer it early, and marks the response finished when it
         * brings the last announced page. Returns null when the fetch was answered early, in which case the response
         * carries no page.
         */
        private ActionListener<ExchangeResponse> trackFetch(ActionListener<ExchangeResponse> listener) {
            final long id;
            synchronized (this) {
                id = nextFetchId++;
                inFlight.put(id, listener);
            }
            return ActionListener.wrap(r -> {
                final ActionListener<ExchangeResponse> target;
                final boolean lastPage;
                synchronized (this) {
                    target = inFlight.remove(id);
                    if (r.hasPage()) {
                        pagesReceived++;
                    }
                    lastPage = expectedPages >= 0 && pagesReceived >= expectedPages;
                }
                if (target == null) {
                    assert r.hasPage() == false : "page arrived after the remote sink announced all its pages were received";
                    return;
                }
                if (lastPage && r.finished() == false) {
                    target.onResponse(new ExchangeResponse(blockFactory, r.takePage(), true));
                } else {
                    target.onResponse(r);
                }
            }, e -> {
                final ActionListener<ExchangeResponse> target;
                synchronized (this) {
                    target = inFlight.remove(id);
                }
                if (target != null) {
                    target.onFailure(e);
                }
            });
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
            doFetchPageAsync(false, trackFetch(ActionListener.wrap(r -> {
                if (r.finished()) {
                    completionListenerRef.compareAndSet(null, SubscribableListener.nullSuccess());
                }
                listener.onResponse(r);
            }, e -> close(ActionListener.running(() -> listener.onFailure(e))))));
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
            if (localService != null) {
                final ActionListener<ExchangeResponse> finalListener = listener;
                localService.fetchPageLocally(exchangeId, allSourcesFinished, new ActionListener<>() {
                    @Override
                    public void onResponse(ExchangeResponse response) {
                        final long responseBytes = response.ramBytesUsedByPage();
                        estimatedPageSizeInBytes.getAndUpdate(curr -> Math.max(responseBytes, curr / 2));
                        // The sink releases the response once this returns (respondAndRelease), so hold a reference across the
                        // hand-off, and deliver on the response executor like a transport response would: the consumers
                        // assume the search pool.
                        response.mustIncRef();
                        new ThreadedActionListener<>(responseExecutor, ActionListener.runAfter(finalListener, response::decRef)).onResponse(
                            response
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        finalListener.onFailure(e);
                    }
                });
                return;
            }
            transportService.sendChildRequest(
                connection,
                EXCHANGE_ACTION_NAME,
                new ExchangeRequest(exchangeId, allSourcesFinished, waitForSink),
                parentTask,
                TransportRequestOptions.EMPTY,
                new ActionListenerResponseHandler<>(listener, in -> {
                    try (BlockStreamInput bsi = new BlockStreamInput(in, blockFactory)) {
                        final ExchangeResponse resp = new ExchangeResponse(bsi);
                        final long responseBytes = resp.ramBytesUsedByPage();
                        estimatedPageSizeInBytes.getAndUpdate(curr -> Math.max(responseBytes, curr / 2));
                        // Old remotes send ESQL warnings as transport response headers on every exchange
                        // page fetch. Strip them here — warnings are delivered through the structured
                        // DriverCompletionInfo.warnings path, and leaving them in the thread context would
                        // cause duplicates when ResponseHeadersCollector merges them back later.
                        transportService.getThreadPool().getThreadContext().takeResponseHeaders("Warning");
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

    /**
     * Sends a batch exchange status request from client to server.
     * The server will reply after batch processing completes.
     */
    public static void sendBatchExchangeStatusRequest(
        TransportService transportService,
        Transport.Connection connection,
        String exchangeId,
        Executor responseExecutor,
        ActionListener<BatchExchangeStatusResponse> listener
    ) {
        transportService.sendRequest(
            connection,
            BATCH_EXCHANGE_STATUS_ACTION_NAME,
            new BatchExchangeStatusRequest(exchangeId),
            TransportRequestOptions.EMPTY,
            new ActionListenerResponseHandler<>(
                listener,
                in -> new BatchExchangeStatusResponse(in, transportService.getThreadPool().getThreadContext()),
                responseExecutor
            )
        );
    }

    // For testing
    public boolean isEmpty() {
        return sinks.isEmpty();
    }

    public Set<String> sinkKeys() {
        return sinks.keySet();
    }

    /**
     * The registered exchange source ids, for tests that need to check the consumer side too. {@link #isEmpty()} and {@link #sinkKeys()}
     * only cover sinks, so a test asserting on those alone would not notice a leaked source handler.
     */
    public Set<String> sourceKeys() {
        return Sets.union(exchangeSources.keySet(), localExchanges.keySet());
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
