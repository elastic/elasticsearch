/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.compute.operator.Operator.NOT_BLOCKED;

/**
 * Client-side handler for bidirectional batch exchange.
 * <p>
 * The client:
 * <ul>
 *   <li>Sends batches to the server via clientToServer exchange (using ExchangeSink)</li>
 *   <li>Receives results from the server via serverToClient exchange (using ExchangeSource)</li>
 *   <li>Tracks batch completion via {@link #markBatchCompleted(long)}</li>
 * </ul>
 */
public final class BidirectionalBatchExchangeClient extends BidirectionalBatchExchangeBase {
    private static final org.apache.logging.log4j.Logger logger = LogManager.getLogger(BidirectionalBatchExchangeClient.class);

    /**
     * Functional interface for server setup callback.
     * Called when a new server connection needs to be established.
     */
    @FunctionalInterface
    public interface ServerSetupCallback {
        /**
         * Send setup request to the server.
         *
         * @param serverNode the server node to connect to
         * @param clientToServerId the per-server unique client-to-server exchange ID
         * @param serverToClientId the shared server-to-client exchange ID
         * @param listener called with the plan string (nullable) on success, or failure
         */
        void sendSetupRequest(DiscoveryNode serverNode, String clientToServerId, String serverToClientId, ActionListener<String> listener);
    }

    // Worker pool for parallel batch processing
    private final List<Worker> workers = new ArrayList<>();
    private final Map<Long, Worker> batchToWorker = new HashMap<>();
    private final int maxWorkers;
    private int nextWorkerId = 0;
    private int lastSelectedWorkerIndex = 0;  // For round-robin tie-breaking

    // Supplier for getting server nodes for new workers
    private final Supplier<DiscoveryNode> serverNodeSupplier;

    // Shared state (single instance for all workers)
    private ExchangeSourceHandler serverToClientSourceHandler;
    private BatchSortedExchangeSource sortedSource; // Wraps ExchangeSource with sorting

    private final AtomicReference<Exception> failureRef = new AtomicReference<>();
    private final Object sendFinishLock = new Object(); // Synchronizes finish() with transport callbacks
    private ActionListener<Void> batchExchangeStatusListener; // Listener for batch exchange status completion
    private volatile boolean closed = false; // Track if close() has been called (for idempotency)
    // Track batch counts to ensure all batches complete before closing
    private int startedBatchCount = 0;
    private int completedBatchCount = 0;

    // Track pending worker connections to prevent finishing client-to-server exchanges prematurely
    // This ensures workers can't finish before all connections are established
    private final AtomicInteger pendingWorkerConnections = new AtomicInteger(0);
    private volatile boolean finishRequested = false;

    // Server setup callback - called lazily when first page is sent
    private final ServerSetupCallback serverSetupCallback;

    // Callback for when lookup plan is received from server (for profiling)
    // BiConsumer takes (workerKey, planString) where workerKey is "nodeId:workerN"
    @Nullable
    private final java.util.function.BiConsumer<String, String> lookupPlanConsumer;

    /**
     * Create a new BidirectionalBatchExchangeClient.
     *
     * @param sessionId session ID for the client
     * @param exchangeService  the exchange service
     * @param executor         executor for async operations
     * @param maxBufferSize    maximum buffer size for exchanges
     * @param transportService transport service for transport-based remote sink
     * @param task             task for transport-based remote sink
     * @param batchExchangeStatusListener listener that will be called when batch exchange status is received (success or failure)
     * @param settings         settings for exchange configuration
     * @param serverSetupCallback callback to send setup request when a new worker is connected
     * @param lookupPlanConsumer optional callback to receive (workerKey, planString) from server setup response
     * @param maxWorkers maximum number of workers (parallel connections) to create
     * @param serverNodeSupplier supplier for getting server nodes for new workers
     */
    public BidirectionalBatchExchangeClient(
        String sessionId,
        ExchangeService exchangeService,
        Executor executor,
        int maxBufferSize,
        TransportService transportService,
        Task task,
        ActionListener<Void> batchExchangeStatusListener,
        Settings settings,
        ServerSetupCallback serverSetupCallback,
        @Nullable java.util.function.BiConsumer<String, String> lookupPlanConsumer,
        int maxWorkers,
        Supplier<DiscoveryNode> serverNodeSupplier
    ) {
        // Client uses per-worker clientToServerIds, but base class needs a value.
        // Pass the base clientToServerId which is used as a prefix for per-worker IDs.
        super(
            sessionId,
            buildClientToServerId(sessionId),
            buildServerToClientId(sessionId),
            exchangeService,
            executor,
            maxBufferSize,
            transportService,
            task,
            settings
        );
        this.batchExchangeStatusListener = batchExchangeStatusListener;
        this.serverSetupCallback = serverSetupCallback;
        this.lookupPlanConsumer = lookupPlanConsumer;
        this.maxWorkers = maxWorkers;
        this.serverNodeSupplier = serverNodeSupplier;
        logger.debug(
            "[LookupJoinClient] Created BidirectionalBatchExchangeClient: serverToClientId={}, maxBufferSize={}, maxWorkers={}",
            serverToClientId,
            maxBufferSize,
            maxWorkers
        );
        initialize();
    }

    /**
     * Initialize the shared client exchanges.
     * Called automatically from the constructor.
     * Per-server client-to-server exchanges are created lazily in getOrCreateServerConnection().
     */
    private void initialize() {
        logger.debug("[LookupJoinClient] Initializing BidirectionalBatchExchangeClient (shared state only)");

        // Create source handler for server-to-client direction (shared for all servers)
        serverToClientSourceHandler = new ExchangeSourceHandler(maxBufferSize, executor);
        exchangeService.addExchangeSourceHandler(serverToClientId, serverToClientSourceHandler);

        // Create BatchSortedExchangeSource that wraps ExchangeSource with sorting
        // All servers send results to the same source handler, sorted by batchId
        ExchangeSource exchangeSource = serverToClientSourceHandler.createExchangeSource();
        sortedSource = new BatchSortedExchangeSource(exchangeSource);
        logger.debug("[LookupJoinClient] Created shared server-to-client sorted source: exchangeId={}", serverToClientId);
    }

    /**
     * Get the least loaded worker, or create a new one if the pool is not full.
     * Uses least-loaded assignment: picks the worker with the lowest pending batch count.
     * Uses round-robin starting position to avoid bias when multiple workers have equal load.
     * Called only from the single-threaded Driver, so no synchronization needed.
     *
     * @return the worker to use for the next batch
     */
    private Worker getLeastLoadedWorker() {
        // If pool not full, create new worker (pending=0, so it will be picked)
        if (workers.size() < maxWorkers) {
            return createNewWorker();
        }
        // Find worker with lowest pending count, starting from round-robin position
        int size = workers.size();
        int startIdx = (lastSelectedWorkerIndex + 1) % size;
        Worker leastLoaded = workers.get(startIdx);
        int leastLoadedIdx = startIdx;
        int minPending = leastLoaded.pendingBatches;
        for (int i = 1; i < size; i++) {
            int idx = (startIdx + i) % size;
            Worker w = workers.get(idx);
            int pending = w.pendingBatches;
            if (pending == 0) {
                lastSelectedWorkerIndex = idx;
                return w;
            }
            if (pending < minPending) {
                minPending = pending;
                leastLoaded = w;
                leastLoadedIdx = idx;
            }
        }
        lastSelectedWorkerIndex = leastLoadedIdx;
        return leastLoaded;
    }

    /**
     * Create a new worker and add it to the pool.
     * Called only from the single-threaded Driver via getLeastLoadedWorker().
     */
    private Worker createNewWorker() {
        DiscoveryNode serverNode = serverNodeSupplier.get();
        if (serverNode == null) {
            throw new IllegalStateException("serverNodeSupplier returned null - no available server nodes");
        }
        int workerId = nextWorkerId++;
        Worker worker = new Worker(workerId, serverNode, sessionId);
        workers.add(worker);
        // Increment pending connections to prevent finishing client-to-server exchanges
        // until this connection is fully established (addRemoteSink called)
        pendingWorkerConnections.incrementAndGet();
        initializeWorker(worker);
        logger.debug(
            "[LookupJoinClient] Created new worker: workerId={}, serverNode={}, totalWorkers={}",
            workerId,
            serverNode.getId(),
            workers.size()
        );
        return worker;
    }

    /**
     * Initialize a worker: create sink handler and send setup request.
     */
    private void initializeWorker(Worker worker) {
        logger.debug(
            "[LookupJoinClient] Initializing worker: workerId={}, node={}, clientToServerId={}",
            worker.workerId,
            worker.serverNode.getId(),
            worker.clientToServerId
        );

        // Create or get sink handler for client-to-server direction (per-worker)
        // Uses getOrCreateSinkHandler to allow pre-registration of the handler (e.g., for test setup coordination)
        worker.clientToServerSinkHandler = exchangeService.getOrCreateSinkHandler(worker.clientToServerId, maxBufferSize);
        worker.clientToServerSink = worker.clientToServerSinkHandler.createExchangeSink(() -> {});

        // When handler completes (buffer finished), clean up the sink handler
        worker.clientToServerSinkHandler.addCompletionListener(
            ActionListener.wrap(v -> exchangeService.finishSinkHandler(worker.clientToServerId, null), e -> {
                handleFailure("client-to-server exchange for worker " + worker.workerId, e);
                exchangeService.finishSinkHandler(worker.clientToServerId, e);
            })
        );
        logger.debug("[LookupJoinClient] Created client-to-server sink handler: exchangeId={}", worker.clientToServerId);

        // Send setup request to server via callback
        serverSetupCallback.sendSetupRequest(
            worker.serverNode,
            worker.clientToServerId,
            worker.serverToClientId,
            ActionListener.wrap(planString -> {
                try {
                    logger.debug("[LookupJoinClient] Server setup complete for worker={}", worker.workerId);
                    // Pass lookup plan to consumer if provided (with workerKey for tracking)
                    if (lookupPlanConsumer != null && planString != null) {
                        String workerKey = worker.serverNode.getId() + ":worker" + worker.workerId;
                        lookupPlanConsumer.accept(workerKey, planString);
                    }
                    // Connect to server's sink and send batch exchange status request
                    connectToServerSink(worker);
                    worker.setupReadyListener.onResponse(null);
                } catch (Exception e) {
                    logger.error("[LookupJoinClient] Server setup callback failed for worker={}: {}", worker.workerId, e.getMessage());
                    // Decrement pending connections so finish() can proceed
                    int remaining = pendingWorkerConnections.decrementAndGet();
                    logger.debug("[LookupJoinClient] Worker setup callback failed, remaining pending={}", remaining);
                    if (remaining == 0 && finishRequested) {
                        synchronized (sendFinishLock) {
                            doFinish();
                        }
                    }
                    handleFailure("worker setup callback for worker " + worker.workerId, e);
                    worker.setupReadyListener.onFailure(e);
                }
            }, e -> {
                logger.error("[LookupJoinClient] Server setup failed for worker={}: {}", worker.workerId, e.getMessage());
                // Decrement pending connections so finish() can proceed
                int remaining = pendingWorkerConnections.decrementAndGet();
                logger.debug("[LookupJoinClient] Worker setup failed, remaining pending={}", remaining);
                if (remaining == 0 && finishRequested) {
                    synchronized (sendFinishLock) {
                        doFinish();
                    }
                }
                handleFailure("worker setup for worker " + worker.workerId, e);
                worker.setupReadyListener.onFailure(e);
            })
        );
    }

    /**
     * Connect to a server's sink handler for server-to-client exchange.
     * This should be called after the server has created its sink handler.
     */
    private void connectToServerSink(Worker worker) {
        logger.debug(
            "[LookupJoinClient] Connecting to server sink for worker={}, node={}, serverToClientId={}",
            worker.workerId,
            worker.serverNode.getId(),
            worker.serverToClientId
        );
        // Each worker connects to its own server's sink, all feeding into the shared serverToClientSourceHandler
        connectRemoteSink(worker.serverNode, worker.serverToClientId, serverToClientSourceHandler, ActionListener.wrap(nullValue -> {
            // Success - no action needed
        }, failure -> { handleFailure("server-to-client exchange for worker " + worker.workerId, failure); }), "server sink handler");

        // Connection is now established (addRemoteSink was called synchronously in connectRemoteSink)
        // Decrement pending connections and check if finish was requested
        int remaining = pendingWorkerConnections.decrementAndGet();
        logger.debug(
            "[LookupJoinClient] Worker connection established: workerId={}, node={}, remaining pending={}",
            worker.workerId,
            worker.serverNode.getId(),
            remaining
        );
        if (remaining == 0 && finishRequested) {
            // All connections established and finish was requested - now we can finish
            logger.debug("[LookupJoinClient] All pending connections established, executing deferred finish");
            synchronized (sendFinishLock) {
                doFinish();
            }
        }

        // Send batch exchange status request
        sendBatchExchangeStatusRequest(worker);
    }

    /**
     * Send batch exchange status request to a specific worker's server.
     */
    private void sendBatchExchangeStatusRequest(Worker worker) {
        try {
            Transport.Connection connection = transportService.getConnection(worker.serverNode);
            logger.debug(
                "[LookupJoinClient] Sending batch exchange status request for worker={}, node={}, exchangeId={}",
                worker.workerId,
                worker.serverNode.getId(),
                worker.serverToClientId
            );
            ExchangeService.sendBatchExchangeStatusRequest(
                transportService,
                connection,
                worker.serverToClientId,
                executor,
                ActionListener.<BatchExchangeStatusResponse>wrap(response -> {
                    logger.debug(
                        "[LookupJoinClient] Received batch exchange status response for worker={}, success={}",
                        worker.workerId,
                        response.isSuccess()
                    );
                    if (response.isSuccess()) {
                        logger.debug("[LookupJoinClient] Completing serverResponseListener for worker={} (success)", worker.workerId);
                        worker.serverResponseListener.onResponse(null);
                        // Only call global listener if all workers have responded
                        checkAllWorkersResponded();
                    } else {
                        Exception failure = response.getFailure();
                        logger.warn(
                            "[LookupJoinClient] Batch exchange status response indicates failure for worker={}: {}",
                            worker.workerId,
                            failure != null ? failure.getMessage() : "unknown"
                        );
                        handleFailure("batch exchange status response for worker " + worker.workerId, failure);
                        worker.serverResponseListener.onResponse(null);
                    }
                }, failure -> {
                    logger.error(
                        "[LookupJoinClient] Failed to receive batch exchange status response for worker={}: {}",
                        worker.workerId,
                        failure.getMessage()
                    );
                    handleFailure("batch exchange status response (transport error) for worker " + worker.workerId, failure);
                    worker.serverResponseListener.onResponse(null);
                })
            );
            worker.requestSent = true;
            logger.debug("[LookupJoinClient] Batch exchange status request sent for worker={}", worker.workerId);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to send batch exchange status request for worker [" + worker.workerId + "]", e);
        }
    }

    /**
     * Check if all workers have responded successfully, and if so, call the global success listener.
     */
    private void checkAllWorkersResponded() {
        for (Worker worker : workers) {
            if (worker.serverResponseListener.isDone() == false) {
                return; // Not all workers have responded yet
            }
        }
        // All workers responded - call global success listener if no failure
        if (failureRef.get() == null && batchExchangeStatusListener != null) {
            logger.debug("[LookupJoinClient] All workers responded successfully, calling batch exchange status listener");
            try {
                batchExchangeStatusListener.onResponse(null);
            } catch (Exception e) {
                logger.error("[LookupJoinClient] Exception in batch exchange status listener callback", e);
            }
        }
    }

    /**
     * Get the server-to-client source handler.
     * Can be used to connect the server's sink by calling addRemoteSink() directly.
     */
    public ExchangeSourceHandler getServerToClientSourceHandler() {
        return serverToClientSourceHandler;
    }

    /**
     * Get the session ID (streaming session ID) used by this client.
     * @return the session ID
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Check if the exchange client has failed.
     * @return true if a failure has occurred, false otherwise
     */
    public boolean hasFailed() {
        return failureRef.get() != null;
    }

    /**
     * Get the number of workers in the pool.
     * Used for testing to verify parallel worker distribution.
     * @return the number of workers created
     */
    public int getWorkerCount() {
        return workers.size();
    }

    /**
     * Polls a page from the cache.
     * The consumer should call this to retrieve pages and check {@link BatchPage#isLastPageInBatch()}
     * to detect batch completion.
     *
     * @return the next page, or null if no pages are available
     */
    public BatchPage pollPage() {
        return sortedSource.pollPage();
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when a page is available or when finished.
     *
     * @return NOT_BLOCKED if a page is available or finished, otherwise a blocked result
     */
    public IsBlockedResult waitForPage() {
        return sortedSource.waitForReading();
    }

    /**
     * Blocks until a page is ready to be polled OR the exchange is finished.
     * This guarantees that when NOT_BLOCKED is returned, either:
     * <ul>
     *   <li>{@link #hasReadyPages()} == true (pollPage will return a page)</li>
     *   <li>{@link #isPageCacheDone()} == true (no more pages expected)</li>
     * </ul>
     * This prevents busy-spinning when pages arrive out of order in multi-node scenarios.
     *
     * @return NOT_BLOCKED if a page is ready or finished, otherwise a blocked result
     */
    public IsBlockedResult waitUntilPageReady() {
        return sortedSource.waitUntilReady();
    }

    /**
     * Returns true if the sorted source is done (upstream finished and no buffered pages).
     */
    public boolean isPageCacheDone() {
        return sortedSource.isFinished();
    }

    /**
     * Returns true if the client has fully finished - all batches complete AND all worker responses were received.
     * The worker responses confirm whether each worker's server succeeded or failed.
     * If any worker failed, hasFailed() will return true and the failure can be retrieved.
     */
    public boolean isFinished() {
        // Check if all workers have responded
        if (allWorkersResponded() == false) {
            return false;
        }
        // If all sent batches have been completed and all workers responded, we're done.
        int started = startedBatchCount;
        int completed = completedBatchCount;
        if (started > 0 && completed >= started) {
            return true;
        }
        // Also check page cache for edge cases (e.g., no batches sent)
        return isPageCacheDone();
    }

    /**
     * Check if all workers have responded.
     */
    private boolean allWorkersResponded() {
        if (workers.isEmpty()) {
            return true; // No workers created yet - considered done
        }
        for (Worker worker : workers) {
            if (worker.serverResponseListener.isDone() == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when all worker responses are received.
     * Use this to block while waiting for all workers' success/failure confirmation.
     */
    public IsBlockedResult waitForServerResponse() {
        // Find a worker that hasn't responded yet
        for (Worker worker : workers) {
            if (worker.serverResponseListener.isDone() == false) {
                return new IsBlockedResult(worker.serverResponseListener, "waiting for worker response from worker " + worker.workerId);
            }
        }
        return NOT_BLOCKED;
    }

    /**
     * Returns the current number of pages in the cache.
     */
    public int pageCacheSize() {
        return sortedSource.bufferSize();
    }

    /**
     * Returns true if there are pages ready to be output (in correct order).
     * Unlike {@link #pageCacheSize()}, this only returns true when pages are
     * actually ready for consumption, not when they're buffered waiting for
     * out-of-order pages to arrive.
     */
    public boolean hasReadyPages() {
        return sortedSource.hasReadyPages();
    }

    /**
     * Send a BatchPage for processing.
     * The worker is selected using least-loaded assignment strategy.
     * Workers are lazily initialized as needed up to maxWorkers.
     * The batchId should be monotonically increasing for each call.
     * Currently, only single-page batches are supported, so isLastPageInBatch must always be true.
     * Called only from the single-threaded Driver, so no synchronization needed.
     *
     * @param batchPage the batch page to send
     */
    public void sendPage(BatchPage batchPage) {
        checkFailure();
        // Currently only single-page batches are supported
        if (batchPage.isLastPageInBatch() == false) {
            throw new IllegalArgumentException(
                "Multi-page batches are not yet supported. Received page for batch "
                    + batchPage.batchId()
                    + " with isLastPageInBatch=false (pageIndex="
                    + batchPage.pageIndexInBatch()
                    + ")"
            );
        }
        // Get least loaded worker (creates new if pool not full)
        Worker worker = getLeastLoadedWorker();
        // Track pending batches for this worker
        worker.pendingBatches++;
        batchToWorker.put(batchPage.batchId(), worker);
        // Track the number of batches that have been sent
        startedBatchCount++;
        worker.clientToServerSink.addPage(batchPage);
        logger.trace(
            "[LookupJoinClient] Sent batch {} to worker {}, pending={}",
            batchPage.batchId(),
            worker.workerId,
            worker.pendingBatches
        );
    }

    /**
     * Send a marker page to signal batch completion for an empty batch.
     * Called only from the single-threaded Driver, so no synchronization needed.
     *
     * @param batchId the batch ID
     */
    public void sendBatchMarker(long batchId) {
        checkFailure();
        Worker worker = getLeastLoadedWorker();
        worker.pendingBatches++;
        batchToWorker.put(batchId, worker);
        BatchPage marker = BatchPage.createMarker(batchId, 0);
        worker.clientToServerSink.addPage(marker);
    }

    /**
     * Mark a batch as completed. Called by the consumer when it finishes processing a batch.
     * This decrements the pending count for the worker that processed the batch.
     * @param batchId the completed batch ID
     */
    public void markBatchCompleted(long batchId) {
        completedBatchCount++;
        Worker worker = batchToWorker.remove(batchId);
        if (worker != null) {
            int pending = worker.pendingBatches--;
            logger.trace(
                "[LookupJoinClient] Batch {} completed on worker {}, pending={}, total completed={}",
                batchId,
                worker.workerId,
                pending,
                completedBatchCount
            );
        } else {
            logger.trace("[LookupJoinClient] Batch {} completed (worker not found), total completed={}", batchId, completedBatchCount);
        }
    }

    /**
     * Finish all client-to-server exchanges (no more batches will be sent to any worker).
     * <p>
     * If there are pending worker connections (setup request sent but connection not yet established),
     * this method will defer finishing until all connections are established. This prevents workers
     * from finishing prematurely, which would cause the server-to-client exchange to close before
     * all worker connections are added.
     */
    public void finish() {
        synchronized (sendFinishLock) {
            finishRequested = true;
            if (pendingWorkerConnections.get() > 0) {
                // Don't finish yet - wait for all pending connections to be established
                // The last connection to establish will call doFinish()
                logger.debug("[LookupJoinClient] Deferring finish, pending worker connections={}", pendingWorkerConnections.get());
                return;
            }
            doFinish();
        }
    }

    /**
     * Actually finish all client-to-server exchanges.
     * Called from finish() when no connections are pending, or from connectToServerSink() when
     * the last pending connection is established and finish was previously requested.
     * <p>
     * MUST be called while holding sendFinishLock.
     */
    private void doFinish() {
        assert Thread.holdsLock(sendFinishLock) : "doFinish must be called while holding sendFinishLock";
        for (Worker worker : workers) {
            if (worker.clientToServerSink != null && worker.clientToServerSink.isFinished() == false && worker.finished == false) {
                logger.debug(
                    "[LookupJoinClient] Finishing client-to-server exchange for worker={} (no more batches will be sent)",
                    worker.workerId
                );
                worker.clientToServerSink.finish();
                worker.finished = true;
            }
        }
    }

    /**
     * Handle failures from any failure source (e.g., server setup, exchange, transport).
     *
     * Only the first failure will trigger the batchExchangeStatusListener.onFailure() callback.
     * Subsequent failures will be logged but ignored to prevent duplicate notifications.
     *
     * @param context the context of the failure (for logging), e.g., "server setup", "client-to-server exchange"
     * @param failure the failure exception
     */
    public void handleFailure(String context, Exception failure) {
        // Use compareAndSet to ensure only the first failure triggers the listener
        if (failureRef.compareAndSet(null, failure)) {
            logger.error("[LookupJoinClient] Failure from {} will be reported: {}", context, failure.getMessage());
            // Notify the operator's failure listener FIRST, before unblocking the driver.
            // This ensures that when finishEarly() wakes up the driver thread, the operator's
            // failure field is already set, so getOutput() will throw immediately.
            if (batchExchangeStatusListener != null) {
                logger.debug(
                    "[LookupJoinClient] Calling batch exchange status listener onFailure (from {}), failure={}",
                    context,
                    failure.getMessage()
                );
                batchExchangeStatusListener.onFailure(failure);
                logger.debug("[LookupJoinClient] Batch exchange status listener onFailure completed");
            }
            // NOW unblock the driver - the failure is already set in the operator
            // Finish the server-to-client source handler to unblock the client driver
            // The driver is waiting for pages from this exchange, so we need to signal completion
            if (serverToClientSourceHandler != null) {
                logger.debug("[LookupJoinClient] Finishing server-to-client source handler due to failure");
                serverToClientSourceHandler.finishEarly(true, ActionListener.noop());
            }
            // Close the sorted source to unblock any consumers waiting for pages
            // This ensures waitForPage() returns NOT_BLOCKED so the operator can check for failure
            if (sortedSource != null) {
                logger.debug("[LookupJoinClient] Closing sorted source due to failure");
                sortedSource.close();
            }
        } else {
            // Failure already stored - just log, don't notify again
            Exception existingFailure = failureRef.get();
            logger.warn(
                "[LookupJoinClient] Additional failure received from {} is ignored: {}. Already reporting failure: {}",
                context,
                failure.getMessage(),
                existingFailure != null ? existingFailure.getMessage() : "unknown"
            );
        }
    }

    private void checkFailure() {
        Exception failure = failureRef.get();
        if (failure != null) {
            // Rethrow RuntimeExceptions directly (e.g., CircuitBreakingException) for proper test handling
            if (failure instanceof RuntimeException rte) {
                throw rte;
            }
            throw new RuntimeException("BidirectionalBatchExchangeClient failed", failure);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        logger.debug("[LookupJoinClient] Closing BidirectionalBatchExchangeClient");

        // Finish all client-to-server exchanges FIRST to signal servers that no more batches will be sent
        // This allows the server drivers to finish and send responses
        try {
            finish();
        } catch (Exception e) {
            logger.error("[LookupJoinClient] Error calling finish()", e);
        }

        // Drain all sink handler buffers to release any pages
        for (Worker worker : workers) {
            try {
                if (worker.clientToServerSinkHandler != null) {
                    logger.debug("[LookupJoinClient] Draining sink handler buffer for worker={}", worker.workerId);
                    worker.clientToServerSinkHandler.onFailure(new TaskCancelledException("client closed"));
                }
            } catch (Exception e) {
                logger.error("[LookupJoinClient] Error draining sink handler for worker=" + worker.workerId, e);
            }
        }

        // Wait for all worker responses - this ensures all workers have finished processing
        for (Worker worker : workers) {
            // First: wait for setup to complete (success or failure) so requestSent has its final value
            try {
                if (worker.setupReadyListener.isDone() == false) {
                    logger.debug("[LookupJoinClient] Waiting for setup completion for worker={}", worker.workerId);
                    PlainActionFuture<Void> setupFuture = new PlainActionFuture<>();
                    worker.setupReadyListener.addListener(setupFuture);
                    setupFuture.actionGet(TimeValue.timeValueSeconds(30));
                }
            } catch (Exception e) {
                logger.warn("[LookupJoinClient] Timeout or exception waiting for setup completion for worker=" + worker.workerId, e);
            }

            // Then: if request was sent, wait for server response
            if (worker.requestSent) {
                try {
                    logger.debug(
                        "[LookupJoinClient] Waiting for server response from worker={}, isDone={}",
                        worker.workerId,
                        worker.serverResponseListener.isDone()
                    );
                    if (worker.serverResponseListener.isDone() == false) {
                        PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
                        worker.serverResponseListener.addListener(waitFuture);
                        waitFuture.actionGet(TimeValue.timeValueSeconds(30));
                    }
                    logger.debug("[LookupJoinClient] Server response received from worker={}", worker.workerId);
                } catch (Exception e) {
                    logger.warn(
                        "[LookupJoinClient] Timeout or exception waiting for server response from worker="
                            + worker.workerId
                            + " - server may not have finished",
                        e
                    );
                }
            }
        }

        // Log incomplete batches for debugging (but don't wait - we're shutting down)
        int started = startedBatchCount;
        int completed = completedBatchCount;
        if (started > 0 && completed < started) {
            logger.warn("[LookupJoinClient] Closing with incomplete batches: started={}, completed={}", started, completed);
        }

        // Finish the shared server-to-client source handler to signal completion
        if (serverToClientSourceHandler != null) {
            logger.debug("[LookupJoinClient] Finishing server-to-client source handler");
            serverToClientSourceHandler.finishEarly(true, ActionListener.noop());
        }

        // Close the sorted source to release any buffered pages
        if (sortedSource != null) {
            logger.debug("[LookupJoinClient] Closing sorted source");
            sortedSource.close();
        }

        // Remove the source handler from the exchange service
        if (serverToClientSourceHandler != null) {
            logger.debug("[LookupJoinClient] Removing server-to-client source handler");
            exchangeService.removeExchangeSourceHandler(serverToClientId);
        }

        logger.debug("[LookupJoinClient] BidirectionalBatchExchangeClient closed");
    }

    /**
     * Represents a worker that can process batches in parallel.
     * Each worker has its own exchange channel to a server and server-side BatchDriver.
     * Multiple workers can target the same server node for parallel execution.
     */
    private static class Worker {
        final int workerId;
        final DiscoveryNode serverNode;
        final String clientToServerId;
        final String serverToClientId;
        ExchangeSinkHandler clientToServerSinkHandler;
        ExchangeSink clientToServerSink;
        final SubscribableListener<Void> serverResponseListener = new SubscribableListener<>();
        final SubscribableListener<Void> setupReadyListener = new SubscribableListener<>();
        int pendingBatches = 0;
        volatile boolean requestSent = false;
        volatile boolean finished = false;

        Worker(int workerId, DiscoveryNode serverNode, String sessionId) {
            this.workerId = workerId;
            this.serverNode = serverNode;
            // Each worker gets unique exchange IDs based on session and worker ID
            this.clientToServerId = BidirectionalBatchExchangeBase.buildClientToServerId(sessionId) + "/worker" + workerId;
            this.serverToClientId = BidirectionalBatchExchangeBase.buildServerToClientId(sessionId) + "/worker" + workerId;
        }
    }
}
