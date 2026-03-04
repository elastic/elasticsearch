/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
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
    private int lastSelectedWorkerIndex = 0;  // For round-robin tie-breaking

    // Supplier for getting server nodes for new workers
    private final Supplier<DiscoveryNode> serverNodeSupplier;

    // Shared exchange ID for the server-to-client direction (all workers share this exchange)
    private final String sharedExchangeId;

    // Shared state (single instance for all workers)
    private ExchangeSourceHandler serverToClientSourceHandler;
    private BatchSortedExchangeSource sortedSource; // Wraps ExchangeSource with sorting

    private final AtomicReference<Exception> primaryFailure = new AtomicReference<>();
    private final AtomicBoolean failureTriggered = new AtomicBoolean(false);
    private final Object sendFinishLock = new Object(); // Synchronizes finish() with transport callbacks
    private final ActionListener<Void> batchExchangeStatusListener; // Listener for batch exchange status completion
    private volatile boolean closed = false; // Track if close() has been called (for idempotency)
    // Track batch counts to ensure all batches complete before closing
    private int startedBatchCount = 0;
    private int completedBatchCount = 0;

    // Track pending worker connections to prevent finishing client-to-server exchanges prematurely
    // This ensures workers can't finish before all connections are established
    private final AtomicInteger pendingWorkerConnections = new AtomicInteger(0);
    private volatile boolean finishRequested = false;

    // Tracks completion of all worker channels (sink + status refs, 2 per worker).
    // The initial ref is released by finish() when no more workers will be created.
    // Error prioritization is handled by setPrimaryFailure/notifyFailure, not here.
    private RefCountingRunnable responseRefs;
    // Resolves when notifyFailure() has set the operator's failure and shut down the exchange.
    // Used by pollPage()/waitForPage()/etc. to block the driver after catching TaskCancelledException
    // from the aborted exchange source, preventing spin-waiting until the real error is available.
    private final SubscribableListener<Void> failureNotified = new SubscribableListener<>();
    // Set to true when all worker channel refs have been released
    private volatile boolean coordinatorDone = false;
    // Resolves when all worker channels have completed (all sink + status refs released).
    // Used by stop() to trigger final cleanup and by waitForServerResponse() to block the driver.
    private final SubscribableListener<Void> allWorkersCompleted = new SubscribableListener<>();

    // Server setup callback - called lazily when first page is sent
    private final ServerSetupCallback serverSetupCallback;

    // Callback for when lookup plan is received from server (for profiling)
    // BiConsumer takes (workerKey, planString) where workerKey is "nodeId:workerN"
    @Nullable
    private final BiConsumer<String, String> lookupPlanConsumer;

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
        @Nullable BiConsumer<String, String> lookupPlanConsumer,
        int maxWorkers,
        Supplier<DiscoveryNode> serverNodeSupplier
    ) {
        super(sessionId, exchangeService, executor, maxBufferSize, transportService, task, settings);
        this.sharedExchangeId = buildServerToClientId(sessionId);
        this.batchExchangeStatusListener = batchExchangeStatusListener;
        this.serverSetupCallback = serverSetupCallback;
        this.lookupPlanConsumer = lookupPlanConsumer;
        this.maxWorkers = maxWorkers;
        this.serverNodeSupplier = serverNodeSupplier;
        logger.debug(
            "[LookupJoinClient] Created BidirectionalBatchExchangeClient: sharedExchangeId={}, maxBufferSize={}, maxWorkers={}",
            sharedExchangeId,
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
        exchangeService.addExchangeSourceHandler(sharedExchangeId, serverToClientSourceHandler);

        // Create BatchSortedExchangeSource that wraps ExchangeSource with sorting
        // All servers send results to the same source handler, sorted by batchId
        ExchangeSource exchangeSource = serverToClientSourceHandler.createExchangeSource();
        sortedSource = new BatchSortedExchangeSource(exchangeSource);
        logger.debug("[LookupJoinClient] Created shared server-to-client sorted source: exchangeId={}", sharedExchangeId);

        // Tracks completion of all worker channels. The initial ref is released by finish().
        // On completion: if no failure was recorded, notify success; otherwise it was already handled.
        responseRefs = new RefCountingRunnable(() -> {
            coordinatorDone = true;
            if (primaryFailure.get() == null) {
                batchExchangeStatusListener.onResponse(null);
            }
            allWorkersCompleted.onResponse(null);
        });
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
        int workerId = workers.size();
        Worker worker = new Worker(workerId, serverNode, sessionId);
        workers.add(worker);
        // Increment pending connections to prevent finishing client-to-server exchanges
        // until this connection is fully established (addRemoteSink called)
        pendingWorkerConnections.incrementAndGet();
        // Acquire completion-tracking refs for this worker's channels (sink + status).
        // Must be acquired here (on the Driver thread, before the async setup) so the refs
        // are visible before any transport callback can release them.
        ActionListener<Void> sinkCompletionRef = responseRefs.acquireListener();
        worker.sinkRef = ActionListener.wrap(sinkCompletionRef::onResponse, e -> {
            notifyFailure(e);
            sinkCompletionRef.onFailure(e);
        });
        worker.statusRef = responseRefs.acquireListener();
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

        // When handler completes (buffer finished), clean up the sink handler.
        // Client-to-server exchange failures call notifyFailure directly. The error will also
        // propagate through the server's status response channel.
        worker.clientToServerSinkHandler.addCompletionListener(
            ActionListener.wrap(v -> exchangeService.finishSinkHandler(worker.clientToServerId, null), e -> {
                notifyFailure(e);
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
                    onWorkerConnectionComplete(worker, "Setup callback failed");
                    // Release both refs. connectToServerSink was never called for this worker.
                    worker.sinkRef.onFailure(e);
                    worker.statusRef.onFailure(e);
                    worker.setupReadyListener.onFailure(e);
                }
            }, e -> {
                logger.error("[LookupJoinClient] Server setup failed for worker={}: {}", worker.workerId, e.getMessage());
                onWorkerConnectionComplete(worker, "Setup failed");
                worker.sinkRef.onFailure(e);
                worker.statusRef.onFailure(e);
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
        // Each worker connects to its own server's sink, all feeding into the shared serverToClientSourceHandler.
        // Use failFast=true so the exchange source aborts immediately on sink failure. The sinkRef
        // wrapper calls notifyFailure directly with the real error (e.g. CircuitBreakingException).
        // TaskCancelledException from the aborted source is caught at the client boundary methods
        // (pollPage, isExchangeDone, etc.) and converted to a block on failureNotified.
        connectRemoteSink(worker.serverNode, worker.serverToClientId, serverToClientSourceHandler, worker.sinkRef, "server sink handler");

        // Connection is now established (addRemoteSink was called synchronously in connectRemoteSink)
        onWorkerConnectionComplete(worker, "Connection established");

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
                        worker.statusRef.onResponse(null);
                    } else {
                        Exception failure = response.getFailure();
                        logger.warn(
                            "[LookupJoinClient] Batch exchange status response indicates failure for worker={}: {}",
                            worker.workerId,
                            failure != null ? failure.getMessage() : "unknown"
                        );
                        notifyFailure(failure);
                        worker.statusRef.onFailure(failure);
                    }
                }, failure -> {
                    logger.error(
                        "[LookupJoinClient] Failed to receive batch exchange status response for worker={}: {}",
                        worker.workerId,
                        failure.getMessage()
                    );
                    notifyFailure(failure);
                    worker.statusRef.onFailure(failure);
                })
            );
            logger.debug("[LookupJoinClient] Batch exchange status request sent for worker={}", worker.workerId);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to send batch exchange status request for worker [" + worker.workerId + "]", e);
        }
    }

    /**
     * Notify the operator of a failure and shut down the exchange. May be called from multiple
     * sources: the response coordinator, the client-to-server sink handler, the status response
     * handler, or directly from a server-to-client sink failure. The highest-priority error is
     * kept via {@link #setPrimaryFailure}. Only the first call triggers the one-time shutdown
     * (drain exchange, unblock driver); subsequent calls only upgrade the error if better.
     */
    private void notifyFailure(Exception failure) {
        setPrimaryFailure(failure);
        if (failureTriggered.compareAndSet(false, true)) {
            logger.error("[LookupJoinClient] Notifying failure: {}", failure.getMessage());
            // Notify the operator's failure listener FIRST, before unblocking the driver.
            // This ensures that when the driver thread unblocks, the operator's failure field
            // is already set, so getOutput() will throw the real error immediately.
            batchExchangeStatusListener.onFailure(failure);
            // Shut down the exchange
            serverToClientSourceHandler.finishEarly(true, ActionListener.noop());
            sortedSource.close();
            // LAST: resolve failureNotified so the driver (blocked after catching TCE) can
            // proceed to checkFailureAndMaybeThrow() which will see the real error.
            failureNotified.onResponse(null);
        } else {
            logger.warn("[LookupJoinClient] Additional failure (primary={}): {}", primaryFailure.get() == failure, failure.getMessage());
        }
    }

    /**
     * CAS loop that keeps the higher-priority error in {@link #primaryFailure}.
     * Uses {@link FailureCollector} to determine priority (CLIENT > SERVER > CANCELLATION).
     */
    private void setPrimaryFailure(Exception failure) {
        while (true) {
            Exception current = primaryFailure.get();
            if (current == null) {
                if (primaryFailure.compareAndSet(null, failure)) {
                    return;
                }
                continue;
            }
            Exception best = betterException(current, failure);
            if (best == current) {
                return;
            }
            if (primaryFailure.compareAndSet(current, best)) {
                return;
            }
            // otherwise the primaryFailure is updated during our processing by another thread
            // rerun the loop
        }
    }

    private static Exception betterException(Exception a, Exception b) {
        FailureCollector collector = new FailureCollector(1);
        collector.unwrapAndCollect(a);
        collector.unwrapAndCollect(b);
        return collector.getFailure();
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
        return primaryFailure.get() != null;
    }

    /**
     * Returns the highest-priority failure, or null if no failure has occurred.
     */
    public Exception getPrimaryFailure() {
        return primaryFailure.get();
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
     * The consumer should call this to retrieve pages and check the page's BatchMetadata
     * to detect batch completion via {@code page.batchMetadata().isLastPageInBatch()}.
     * <p>
     * With failFast=true, the exchange source may throw TaskCancelledException when aborted.
     * We catch it here and return null — the real error will arrive via the response coordinator
     * and notifyFailure(), which sets the operator's failure field.
     *
     * @return the next page, or null if no pages are available or exchange is aborted
     */
    public Page pollPage() {
        try {
            return sortedSource.pollPage();
        } catch (TaskCancelledException e) {
            return null;
        }
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when a page is available or when finished.
     * <p>
     * If the exchange source is aborted (failFast=true), catches TaskCancelledException and
     * blocks on {@link #failureNotified} until the real error is available.
     *
     * @return NOT_BLOCKED if a page is available or finished, otherwise a blocked result
     */
    public IsBlockedResult waitForPage() {
        try {
            return sortedSource.waitForReading();
        } catch (TaskCancelledException e) {
            return new IsBlockedResult(failureNotified, "waiting for real error after exchange abort");
        }
    }

    /**
     * Blocks until a page is ready to be polled OR the exchange is finished.
     * This guarantees that when NOT_BLOCKED is returned, either:
     * <ul>
     *   <li>{@link #hasReadyPages()} == true (pollPage will return a page)</li>
     *   <li>{@link #isExchangeDone()} == true (no more pages expected)</li>
     * </ul>
     * This prevents busy-spinning when pages arrive out of order in multi-node scenarios.
     * <p>
     * If the exchange source is aborted (failFast=true), catches TaskCancelledException and
     * blocks on {@link #failureNotified} until the real error is available.
     *
     * @return NOT_BLOCKED if a page is ready or finished, otherwise a blocked result
     */
    public IsBlockedResult waitUntilPageReady() {
        try {
            return sortedSource.waitUntilReady();
        } catch (TaskCancelledException e) {
            return new IsBlockedResult(failureNotified, "waiting for real error after exchange abort");
        }
    }

    /**
     * Returns true if the exchange is done (upstream finished and no buffered pages).
     * <p>
     * With failFast=true, the exchange source throws TaskCancelledException when aborted.
     * We catch it and return false — the exchange is not "done", it's aborting.
     */
    public boolean isExchangeDone() {
        try {
            return sortedSource.isFinished();
        } catch (TaskCancelledException e) {
            return false;
        }
    }

    /**
     * Returns true if the client has fully finished - all batches complete AND all worker responses were received.
     * The worker responses confirm whether each worker's server succeeded or failed.
     * If any worker failed, hasFailed() will return true and the failure can be retrieved.
     */
    public boolean isFinished() {
        // Check if all worker channels have completed (all sink + status refs released)
        if (coordinatorDone == false) {
            return false;
        }
        // If all sent batches have been completed and all workers responded, we're done.
        int started = startedBatchCount;
        int completed = completedBatchCount;
        if (started > 0 && completed >= started) {
            return true;
        }
        // Also check exchange for edge cases (e.g., no batches sent)
        return isExchangeDone();
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when all worker channels have completed
     * (all sink + status refs released). Use this to block while waiting for all workers'
     * success/failure confirmation.
     */
    public IsBlockedResult waitForServerResponse() {
        if (allWorkersCompleted.isDone() == false) {
            if (failureNotified.isDone()) {
                return NOT_BLOCKED;
            }
            SubscribableListener<Void> either = new SubscribableListener<>();
            allWorkersCompleted.addListener(either);
            failureNotified.addListener(either);
            return new IsBlockedResult(either, "waiting for all workers to complete or failure");
        }
        return NOT_BLOCKED;
    }

    /**
     * Returns the current number of buffered pages.
     */
    public int bufferedPageCount() {
        return sortedSource.bufferSize();
    }

    /**
     * Returns true if there are pages ready to be output (in correct order).
     * Unlike {@link #bufferedPageCount()}, this only returns true when pages are
     * actually ready for consumption, not when they're buffered waiting for
     * out-of-order pages to arrive.
     */
    public boolean hasReadyPages() {
        return sortedSource.hasReadyPages();
    }

    /**
     * Returns the sorted source for diagnostic purposes (e.g. toString in timeout messages).
     */
    public BatchSortedExchangeSource getSortedSource() {
        return sortedSource;
    }

    /**
     * Send a Page with BatchMetadata for processing.
     * The worker is selected using least-loaded assignment strategy.
     * Workers are lazily initialized as needed up to maxWorkers.
     * The batchId should be monotonically increasing for each call.
     * Currently, only single-page batches are supported, so isLastPageInBatch must always be true.
     * Called only from the single-threaded Driver, so no synchronization needed.
     *
     * @param page the page with BatchMetadata to send
     */
    public void sendPage(Page page) {
        checkFailure();
        BatchMetadata metadata = page.batchMetadata();
        if (metadata == null) {
            throw new IllegalArgumentException("Page must have BatchMetadata");
        }
        // Currently only single-page batches are supported
        if (metadata.isLastPageInBatch() == false) {
            throw new IllegalArgumentException(
                "Multi-page batches are not yet supported. Received page for batch "
                    + metadata.batchId()
                    + " with isLastPageInBatch=false (pageIndex="
                    + metadata.pageIndexInBatch()
                    + ")"
            );
        }
        // Get least loaded worker (creates new if pool not full)
        Worker worker = getLeastLoadedWorker();
        // Track pending batches for this worker
        worker.pendingBatches++;
        batchToWorker.put(metadata.batchId(), worker);
        // Track the number of batches that have been sent
        startedBatchCount++;
        page.allowPassingToDifferentDriver();
        worker.clientToServerSink.addPage(page);
        logger.trace(
            "[LookupJoinClient] Sent batch {} to worker {}, pending={}",
            metadata.batchId(),
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
        Page marker = Page.createBatchMarkerPage(batchId, 0);
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
            int pending = --worker.pendingBatches;
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
            if (finishRequested == false) {
                finishRequested = true;
                // Release the initial ref. All worker refs have been acquired (workers are
                // created on the Driver thread before finish() is called). The callback fires
                // when all remaining refs (sink + status per worker) are released.
                responseRefs.close();
            }
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
     * Called when a worker connection attempt completes (success or failure).
     * Decrements pending connection count and triggers deferred finish if needed.
     *
     * @param worker the worker whose connection completed
     * @param reason description of what completed (for logging)
     */
    private void onWorkerConnectionComplete(Worker worker, String reason) {
        int remaining = pendingWorkerConnections.decrementAndGet();
        logger.debug("[LookupJoinClient] {} for worker {}, remaining pending={}", reason, worker.workerId, remaining);
        if (remaining == 0 && finishRequested) {
            synchronized (sendFinishLock) {
                doFinish();
            }
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

    private void checkFailure() {
        Exception failure = primaryFailure.get();
        if (failure != null) {
            // Rethrow RuntimeExceptions directly (e.g., CircuitBreakingException) for proper test handling
            if (failure instanceof RuntimeException rte) {
                throw rte;
            }
            throw new RuntimeException("BidirectionalBatchExchangeClient failed", failure);
        }
    }

    /**
     * Triggers a non-blocking shutdown: finishes all client-to-server exchanges and drains
     * sink handler buffers to trigger cascading server shutdown. Does not wait for worker
     * completion -- the driver blocks on {@link #waitForServerResponse()} (which uses
     * {@link #allWorkersCompleted}) before calling {@link #close()}, so all workers are
     * guaranteed to be done by the time close runs.
     */
    private void stop() {
        // Finish all client-to-server exchanges to signal servers that no more batches will be sent.
        // This allows the server drivers to finish and send responses.
        try {
            finish();
        } catch (Exception e) {
            logger.error("[LookupJoinClient] Error calling finish()", e);
        }

        // Drain all sink handler buffers to release any pages, then explicitly remove the sink handler
        // from the exchange service. The explicit finishSinkHandler call is a safety net: normally the
        // completion listener registered in initializeWorker() removes it, but under rare race conditions
        // (e.g. the completion future was already resolved via the buffer chain on a transport thread)
        // the onFailure call may be a no-op and the handler could remain registered.
        for (Worker worker : workers) {
            try {
                if (worker.clientToServerSinkHandler != null) {
                    logger.debug(
                        "[LookupJoinClient] Draining sink handler buffer for worker={}, isFinished={}",
                        worker.workerId,
                        worker.clientToServerSinkHandler.isFinished()
                    );
                    worker.clientToServerSinkHandler.onFailure(new TaskCancelledException("client stopped"));
                }
            } catch (Exception e) {
                logger.error("[LookupJoinClient] Error draining sink handler for worker=" + worker.workerId, e);
            }
            try {
                exchangeService.finishSinkHandler(worker.clientToServerId, new TaskCancelledException("client stopped"));
            } catch (Exception e) {
                logger.debug("[LookupJoinClient] finishSinkHandler already done for worker={}", worker.workerId);
            }
        }
    }

    /**
     * Closes the client. The driver guarantees all workers have completed (via
     * {@link #waitForServerResponse()} blocking on {@link #allWorkersCompleted}) before
     * calling this method, so cleanup is purely synchronous.
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        logger.debug("[LookupJoinClient] Closing BidirectionalBatchExchangeClient");

        stop();

        // Log incomplete batches for debugging
        int started = startedBatchCount;
        int completed = completedBatchCount;
        if (started > 0 && completed < started) {
            logger.warn("[LookupJoinClient] Closing with incomplete batches: started={}, completed={}", started, completed);
        }

        // Finish the shared server-to-client source handler to signal completion
        try {
            if (serverToClientSourceHandler != null) {
                logger.debug("[LookupJoinClient] Finishing server-to-client source handler");
                serverToClientSourceHandler.finishEarly(true, ActionListener.noop());
            }
        } catch (Exception e) {
            logger.error("[LookupJoinClient] Error finishing server-to-client source handler", e);
        }

        // Close the sorted source to release any buffered pages
        try {
            if (sortedSource != null) {
                logger.debug("[LookupJoinClient] Closing sorted source");
                sortedSource.close();
            }
        } catch (Exception e) {
            logger.error("[LookupJoinClient] Error closing sorted source", e);
        }

        // Remove the source handler from the exchange service
        try {
            if (serverToClientSourceHandler != null) {
                logger.debug("[LookupJoinClient] Removing server-to-client source handler");
                exchangeService.removeExchangeSourceHandler(sharedExchangeId);
            }
        } catch (Exception e) {
            logger.error("[LookupJoinClient] Error removing server-to-client source handler", e);
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
        final SubscribableListener<Void> setupReadyListener = new SubscribableListener<>();
        int pendingBatches = 0;
        volatile boolean finished = false;
        // Completion-tracking refs from responseRefs. Both are released when the
        // corresponding channel completes (success or failure). On setup failure before
        // connectToServerSink is reached, both are released with the setup error.
        ActionListener<Void> sinkRef;
        ActionListener<Void> statusRef;

        Worker(int workerId, DiscoveryNode serverNode, String sessionId) {
            this.workerId = workerId;
            this.serverNode = serverNode;
            // Each worker gets unique exchange IDs based on session and worker ID
            this.clientToServerId = BidirectionalBatchExchangeBase.buildClientToServerId(sessionId) + "/worker" + workerId;
            this.serverToClientId = BidirectionalBatchExchangeBase.buildServerToClientId(sessionId) + "/worker" + workerId;
        }
    }
}
