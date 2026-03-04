/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FailureCollector;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.lookup.RightChunkedLeftJoin;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Streaming version of LookupFromIndexOperator.
 * Uses BidirectionalBatchExchange to stream pages to server and receive results.
 */
public class StreamingLookupFromIndexOperator implements Operator {
    private static final Logger logger = LogManager.getLogger(StreamingLookupFromIndexOperator.class);
    private static final AtomicLong exchangeIdGenerator = new AtomicLong(0);

    // Configuration
    private final DriverContext driverContext;
    private final int maxOutstandingRequests;
    private final LookupFromIndexService lookupService;
    private final String sessionId; // Original session ID from the query
    private final String exchangeSessionId; // Unique ID for this operator's exchange
    private final CancellableTask parentTask;
    private final String lookupIndex;
    private final String lookupIndexPattern;
    private final List<NamedExpression> loadFields;
    private final Source source;
    private final PhysicalPlan rightPreJoinPlan;
    private final Expression joinOnConditions;
    private final int exchangeBufferSize;
    private final LookupFromIndexOperator.MatchFieldsMapping matchFieldsMapping;
    private final boolean profile;

    // State
    private final AtomicLong batchIdGenerator = new AtomicLong(0);
    private final Map<Long, PendingJoin> activeBatches = new HashMap<>();
    private BidirectionalBatchExchangeClient client;
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    private volatile boolean finished = false;
    private volatile boolean closed = false;
    private boolean clientFinishCalled = false;
    private volatile boolean hadMissingMarkersWithoutFailure = false;
    // Cached flag to avoid iterating activeBatches on every canProduceMoreDataWithoutExtraInput() call
    private volatile boolean hasBatchReadyToOutput = false;

    // Stats
    private long pagesReceived = 0;
    private long pagesCompleted = 0;
    private long totalInputRows = 0;
    private long totalOutputRows = 0;

    // Timing stats - volatile because written from transport callback threads, read from driver thread
    private volatile long planningStartNanos = 0;
    private volatile long planningEndNanos = 0;
    private volatile long processEndNanos = 0;

    // Lookup plans from servers (for profile output)
    // Maps plan string -> set of worker keys (e.g., "nodeId:worker0") that produced this plan
    private final Map<String, Set<String>> planToWorkers = new ConcurrentHashMap<>();

    /**
     * State for a pending join operation (one input page being processed).
     */
    private static class PendingJoin {
        final long batchId;
        final RightChunkedLeftJoin join;
        boolean receivedLastPage = false;

        PendingJoin(long batchId, Page inputPage, int loadFieldCount) {
            this.batchId = batchId;
            this.join = new RightChunkedLeftJoin(inputPage, loadFieldCount);
        }
    }

    public StreamingLookupFromIndexOperator(
        DriverContext driverContext,
        List<MatchConfig> matchFields,
        String sessionId,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        LookupFromIndexService lookupService,
        String lookupIndexPattern,
        String lookupIndex,
        List<NamedExpression> loadFields,
        Source source,
        PhysicalPlan rightPreJoinPlan,
        Expression joinOnConditions,
        int exchangeBufferSize,
        boolean profile
    ) {
        this.driverContext = driverContext;
        this.maxOutstandingRequests = maxOutstandingRequests;
        this.lookupService = lookupService;
        this.sessionId = sessionId;
        this.exchangeSessionId = sessionId + "/streaming-lookup/" + exchangeIdGenerator.incrementAndGet();
        this.parentTask = parentTask;
        this.lookupIndex = lookupIndex;
        this.lookupIndexPattern = lookupIndexPattern;
        this.loadFields = loadFields;
        this.source = source;
        this.rightPreJoinPlan = rightPreJoinPlan;
        this.joinOnConditions = joinOnConditions;
        this.exchangeBufferSize = exchangeBufferSize;
        this.matchFieldsMapping = LookupFromIndexOperator.buildMatchFieldsMapping(matchFields, joinOnConditions);
        this.profile = profile;

        // Initialize exchange client in constructor
        initializeClient();
    }

    private void initializeClient() {
        ExchangeService exchangeService = lookupService.getExchangeService();

        try {
            // Create the server setup callback - will be called lazily when first page is sent to a server
            BidirectionalBatchExchangeClient.ServerSetupCallback setupCallback = (
                serverNode,
                clientToServerId,
                serverToClientId,
                listener) -> {
                planningStartNanos = System.nanoTime();
                // Create setup request for this server
                // clientToServerId is per-server unique, serverToClientId is shared across all servers
                LookupFromIndexService.Request setupRequest = new LookupFromIndexService.Request(
                    sessionId,
                    lookupIndex,
                    lookupIndexPattern,
                    matchFieldsMapping.reindexedMatchFields(),
                    new Page(0), // Empty page for setup
                    loadFields,
                    source,
                    rightPreJoinPlan,
                    joinOnConditions,
                    clientToServerId,
                    serverToClientId,
                    profile
                );

                lookupService.lookupAsync(setupRequest, serverNode, parentTask, ActionListener.wrap(response -> {
                    planningEndNanos = System.nanoTime();
                    logger.debug("Server setup complete for node={}", serverNode.getId());
                    listener.onResponse(response.planString());
                }, e -> {
                    planningEndNanos = System.nanoTime();
                    logger.error("Server setup failed for node=" + serverNode.getId(), e);
                    failure.set(e);
                    listener.onFailure(e);
                }));
            };

            driverContext.addAsyncAction();
            client = new BidirectionalBatchExchangeClient(
                exchangeSessionId,
                exchangeService,
                lookupService.getExecutor(),
                exchangeBufferSize,
                lookupService.getTransportService(),
                parentTask,
                ActionListener.wrap(v -> handleBatchExchangeSuccess(), this::handleBatchExchangeFailure),
                lookupService.getSettings(),
                setupCallback,
                profile
                    ? (workerKey, planString) -> planToWorkers.computeIfAbsent(planString, k -> ConcurrentHashMap.newKeySet())
                        .add(workerKey)
                    : null,  // Only track plans when profiling
                maxOutstandingRequests,
                this::determineServerNode  // Supplier for getting server nodes for workers
            );

        } catch (Exception e) {
            logger.error("Failed to create client", e);
            failure.set(e);
            driverContext.removeAsyncAction();
        }
    }

    private DiscoveryNode determineServerNode() {
        var clusterState = lookupService.getClusterService().state();
        var projectState = lookupService.getProjectResolver().getProjectState(clusterState);
        var shardIterators = lookupService.getClusterService()
            .operationRouting()
            .searchShards(projectState, new String[] { lookupIndex }, Map.of(), "_local");
        if (shardIterators.size() != 1) {
            throw new IllegalStateException(
                "Expected exactly 1 shard for lookup index [" + lookupIndex + "], got " + shardIterators.size()
            );
        }
        var shardIt = shardIterators.get(0);
        ShardRouting shardRouting = shardIt.nextOrNull();
        if (shardRouting == null) {
            throw new IllegalStateException("No available shard routing for lookup index [" + lookupIndex + "]");
        }
        DiscoveryNode node = clusterState.nodes().get(shardRouting.currentNodeId());
        if (node == null) {
            throw new IllegalStateException(
                "Node [" + shardRouting.currentNodeId() + "] for lookup index [" + lookupIndex + "] not found in cluster state"
            );
        }
        return node;
    }

    private void handleBatchExchangeSuccess() {
        logger.debug("Batch exchange completed successfully");
        driverContext.removeAsyncAction();
    }

    private void handleBatchExchangeFailure(Exception e) {
        logger.error("Batch exchange failed", e);
        failure.set(e);
        driverContext.removeAsyncAction();
    }

    @Override
    public boolean needsInput() {
        // Don't accept new input if we already have maxOutstandingRequests in flight
        if (activeBatches.size() >= maxOutstandingRequests) {
            return false;
        }
        return finished == false && failure.get() == null && closed == false && client != null;
    }

    @Override
    public void addInput(Page page) {
        if (closed || failure.get() != null) {
            page.releaseBlocks();
            return;
        }

        // Client must be available - it's created synchronously in constructor
        if (client == null) {
            page.releaseBlocks();
            failure.compareAndSet(null, new IllegalStateException("Client not initialized"));
            return;
        }

        totalInputRows += page.getPositionCount();
        long batchId = batchIdGenerator.incrementAndGet();
        logger.trace("addInput: batchId={}, positions={}", batchId, page.getPositionCount());

        // Send page synchronously - the exchange buffer holds pages until server fetches them
        // The client handles worker/server selection internally using least-loaded strategy
        Page pageWithMetadata = null;
        try {
            Block[] inputBlocks = matchFieldsMapping.applyTo(page);
            // incRef the blocks since they're shared with the original page
            for (Block block : inputBlocks) {
                block.incRef();
            }
            pageWithMetadata = new Page(new BatchMetadata(batchId, 0, true), inputBlocks);
            client.sendPage(pageWithMetadata);
            logger.trace("addInput: sent batchId={} to worker", batchId);
        } catch (RuntimeException e) {
            logger.error("addInput: failed to send batchId={}: {}", batchId, e.getMessage());
            if (pageWithMetadata != null) {
                pageWithMetadata.releaseBlocks();
            }
            page.releaseBlocks();
            throw e;
        } catch (Exception e) {
            logger.error("addInput: failed to send batchId={}: {}", batchId, e.getMessage());
            if (pageWithMetadata != null) {
                pageWithMetadata.releaseBlocks();
            }
            page.releaseBlocks();
            failure.set(e);
            return;
        }

        // Track batch for receiving results
        PendingJoin batch = new PendingJoin(batchId, page, loadFields.size());
        activeBatches.put(batchId, batch);
        pagesReceived++;
    }

    @Override
    public Page getOutput() {
        checkFailureAndMaybeThrow();

        // If missing markers were detected, check for real errors that may have arrived via
        // the server response (the Driver loop through isBlocked() -> waitForServerResponse()
        // handles the non-blocking wait for server responses to arrive).
        if (hadMissingMarkersWithoutFailure && client != null) {
            checkFailureAndMaybeThrow();
            cleanupBatchResources();
            throw new IllegalStateException(
                "Exchange completed but some batches never received last-page markers. "
                    + "This indicates a bug in the exchange protocol or server."
            );
        }

        if (client == null) {
            return null;
        }

        if (activeBatches.isEmpty()) {
            return null;
        }

        Page output = getOutputFromActiveBatches();
        if (output != null) {
            totalOutputRows += output.getPositionCount();
        }
        return output;
    }

    private Page getOutputFromActiveBatches() {
        while (true) {
            // First, check for batches that received all results and need trailing nulls computed
            // We compute trailing nulls lazily - only when we're ready to output them
            Iterator<Map.Entry<Long, PendingJoin>> it = activeBatches.entrySet().iterator();
            while (it.hasNext()) {
                PendingJoin batch = it.next().getValue();
                if (batch.receivedLastPage) {
                    Optional<Page> trailingNulls = batch.join.noMoreRightHandPages();
                    if (trailingNulls.isPresent()) {
                        Page output = trailingNulls.get();
                        completeBatch(batch, it);
                        return output;
                    } else {
                        // No trailing nulls, just complete the batch
                        completeBatch(batch, it);
                        // Continue checking other batches - don't return null yet
                    }
                }
            }

            // If all batches were just completed, return null
            if (activeBatches.isEmpty()) {
                return null;
            }

            // Try to poll a result page from the client
            Page resultPage = client.pollPage();
            if (resultPage == null) {
                // If the client's exchange is done, but we haven't received markers for all batches,
                // we must wait for the server response to know if it succeeded or failed.
                // We can't compute trailing nulls until we know the server completed successfully,
                // otherwise we might produce wrong results (NULLs for rows that actually had matches).
                if (client.isExchangeDone()) {
                    for (PendingJoin batch : activeBatches.values()) {
                        if (batch.receivedLastPage == false) {
                            // Check if server failed - if so, let the failure propagate
                            if (client.hasFailed()) {
                                return null;
                            }
                            // Page cache is done but marker is missing and no failure reported yet.
                            // Set flag and proceed to close(), which will wait for the server response.
                            // If no more specific server error arrives after waiting, close() will throw error.
                            logger.warn("getOutput: pageCacheDone but no marker for batchId={}, proceeding to close", batch.batchId);
                            hadMissingMarkersWithoutFailure = true;
                            return null;
                        }
                    }
                }
                logger.trace(
                    "getOutput: pollPage returned null, activeBatches={}, bufferedPageCount={}, exchangeDone={}",
                    activeBatches.size(),
                    client.bufferedPageCount(),
                    client.isExchangeDone()
                );
                return null;
            }

            BatchMetadata resultMetadata = resultPage.batchMetadata();
            logger.trace(
                "getOutput: received result for batch {}, isLast={}",
                resultMetadata.batchId(),
                resultMetadata.isLastPageInBatch()
            );

            // Result pages were created on the server-side driver, so we need to allow
            // releasing them on this (client-side) driver thread
            resultPage.allowPassingToDifferentDriver();

            // Look up the batch by ID
            PendingJoin batch = activeBatches.get(resultMetadata.batchId());
            if (batch == null) {
                resultPage.releaseBlocks();
                throw new IllegalStateException("Received result for unknown batch: batchId=" + resultMetadata.batchId());
            }

            if (resultMetadata.isLastPageInBatch()) {
                batch.receivedLastPage = true;
                hasBatchReadyToOutput = true;
            }

            // Check for empty result page (marker only)
            if (resultPage.getPositionCount() == 0) {
                resultPage.releaseBlocks();
                // If this was the last page, loop back to process trailing nulls immediately
                if (batch.receivedLastPage) {
                    continue;
                }
                return null;
            }

            // Join the right page with the left (input) page
            // Use try-finally to ensure resultPage is always released, even if join() throws
            Page output;
            try {
                output = batch.join.join(resultPage);
            } finally {
                resultPage.releaseBlocks();
            }

            return output;
        }
    }

    private void completeBatch(PendingJoin batch, Iterator<?> it) {
        if (batch != null) {
            if (client != null) {
                logger.debug("completeBatch: batchId={}", batch.batchId);
                client.markBatchCompleted(batch.batchId);
            }
            Releasables.closeExpectNoException(batch.join);
            it.remove();
            pagesCompleted++;
            // Recompute cached flag - check if any remaining batch has receivedLastPage
            hasBatchReadyToOutput = false;
            for (PendingJoin remaining : activeBatches.values()) {
                if (remaining.receivedLastPage) {
                    hasBatchReadyToOutput = true;
                    break;
                }
            }
        }
    }

    /**
     * Check for failure and throw if one has occurred.
     * Cleans up batch resources before throwing.
     */
    private void checkFailureAndMaybeThrow() {
        Exception ex = failure.get();
        Exception clientEx = (client != null) ? client.getPrimaryFailure() : null;
        Exception best = bestError(ex, clientEx);
        if (best != null) {
            cleanupBatchResources();
            if (best instanceof RuntimeException rte) {
                throw rte;
            }
            throw new IllegalStateException("Batch exchange failed", best);
        }
    }

    private static Exception bestError(@Nullable Exception a, @Nullable Exception b) {
        if (a == null) return b;
        if (b == null) return a;
        if (a == b) return a;
        FailureCollector collector = new FailureCollector(1);
        collector.unwrapAndCollect(a);
        collector.unwrapAndCollect(b);
        return collector.getFailure();
    }

    /**
     * Clean up all batch resources (activeBatches).
     * Called when an error is detected to release resources before throwing.
     */
    private void cleanupBatchResources() {
        for (PendingJoin batch : activeBatches.values()) {
            logger.debug("cleanupBatchResources: cleaning batch batchId={}", batch.batchId);
            Releasables.closeExpectNoException(batch.join);
        }
        activeBatches.clear();
    }

    @Override
    public void finish() {
        logger.debug("finish() called, activeBatches={}", activeBatches.size());
        finished = true;
        if (client != null && clientFinishCalled == false) {
            clientFinishCalled = true;
            logger.debug("Calling client.finish()");
            client.finish();
        }
    }

    @Override
    public boolean isFinished() {
        // If there's a failure, return false so getOutput() is called to throw the exception
        if (failure.get() != null) {
            return false;
        }
        if (finished == false) {
            return false;
        }
        if (activeBatches.isEmpty() == false) {
            logger.trace("isFinished: false (activeBatches not empty), size={}", activeBatches.size());
            return false;
        }
        // If no batches were ever sent, we're done immediately
        if (batchIdGenerator.get() == 0) {
            logger.debug("isFinished: true (no batches sent)");
            return true;
        }
        // Must wait for server's batch exchange status response to confirm success/failure
        if (client != null && client.isFinished() == false) {
            logger.debug("isFinished: false (client not finished, waiting for server response)");
            return false;
        }
        logger.debug("isFinished: true");
        return true;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        // Return true only if we have actual data ready to emit:
        // 1. Any batch received last page (needs trailing nulls computed) - use cached flag
        // 2. We have pages ready in the output queue (not just buffered out-of-order pages)
        if (hasBatchReadyToOutput) {
            return true;
        }
        // Check if there are pages ready to output (in correct order).
        // Using hasReadyPages() instead of bufferedPageCount() > 0 prevents busy-spinning
        // when pages arrive out-of-order and are buffered waiting for earlier pages.
        if (client != null && client.hasReadyPages()) {
            return true;
        }
        return false;
    }

    @Override
    public IsBlockedResult isBlocked() {
        // Fast path: if we have data ready to output, don't block
        if (hasBatchReadyToOutput) {
            return NOT_BLOCKED;
        }

        Exception ex = failure.get();
        if (ex == null && client != null) {
            ex = client.getPrimaryFailure();
        }
        if (ex != null) {
            // it will throw in getOutput() with the exception
            return NOT_BLOCKED;
        }

        // Fast path: if client has ready pages, don't block
        if (client != null && client.hasReadyPages()) {
            return NOT_BLOCKED;
        }

        // If we have active batches waiting for results, check if we need to wait for pages
        if (activeBatches.isEmpty() == false) {
            // If page cache is done but we have batches without markers, wait for server response
            // to know if the server succeeded or failed before deciding what to do
            if (client.isExchangeDone()) {
                logger.debug("isBlocked: exchangeDone but missing markers, waiting for server response");
                return client.waitForServerResponse();
            }

            // No batch has data ready, wait for pages from client
            // waitUntilPageReady() guarantees that when it returns done, either:
            // - hasReadyPages() == true (pollPage will return a page)
            // - isExchangeDone() == true (no more pages expected)
            IsBlockedResult waitResult = client.waitUntilPageReady();
            if (waitResult.listener().isDone() == false) {
                logger.trace(
                    "isBlocked: waiting for page, activeBatches={}, bufferedPageCount={}, exchangeDone={}",
                    activeBatches.size(),
                    client.bufferedPageCount(),
                    client.isExchangeDone()
                );
                return waitResult;
            }
            // waitUntilPageReady() returned done - pages are ready or cache is done
            // If cache is done but no pages ready, we need to wait for server response
            if (client.hasReadyPages() == false && client.isExchangeDone()) {
                logger.debug("isBlocked: no ready pages and exchangeDone, waiting for server response");
                return client.waitForServerResponse();
            }
        }

        // If we're done processing batches but waiting for server response, block on that
        // This ensures we don't complete until we know if the server succeeded or failed
        if (finished && activeBatches.isEmpty() && client != null) {
            if (client.isFinished() == false) {
                logger.debug("isBlocked: waiting for server response");
                return client.waitForServerResponse();
            }
        }

        return NOT_BLOCKED;
    }

    @Override
    public void close() {
        logger.debug("close() called");
        processEndNanos = System.nanoTime();
        closed = true;

        cleanupBatchResources();

        if (client != null) {
            // Per-worker setup and server response waits are handled inside client.close().
            try {
                client.finish();
            } catch (Exception e) {
                logger.error("Error finishing client", e);
            }
            try {
                client.close();
            } catch (Exception e) {
                logger.error("Error closing client", e);
            }
        }
    }

    @Override
    public Status status() {
        long planningNanos = (planningEndNanos > 0 && planningStartNanos > 0) ? (planningEndNanos - planningStartNanos) : 0;
        // Calculate process_nanos as time since planning completed until now (or until close() was called)
        long processEnd = (processEndNanos > 0) ? processEndNanos : System.nanoTime();
        long processNanos = (planningEndNanos > 0) ? (processEnd - planningEndNanos) : 0;
        // Create a defensive copy of planToWorkers
        Map<String, Set<String>> plansCopy = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : planToWorkers.entrySet()) {
            plansCopy.put(entry.getKey(), new HashSet<>(entry.getValue()));
        }
        return new StreamingLookupStatus(
            pagesReceived,
            pagesCompleted,
            totalInputRows,
            totalOutputRows,
            planningNanos,
            processNanos,
            plansCopy
        );
    }

    @Override
    public String toString() {
        return "StreamingLookupOperator[index=" + lookupIndex + "]";
    }

    /**
     * Status for StreamingLookupFromIndexOperator.
     */
    public static class StreamingLookupStatus implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "streaming_lookup",
            StreamingLookupStatus::new
        );

        // Reuse the streaming session ID version since streaming is not in production yet
        private static final TransportVersion ESQL_LOOKUP_PLAN_STRING = TransportVersion.fromName("esql_streaming_lookup_join");

        private final long pagesReceived;
        private final long pagesEmitted;
        private final long rowsReceived;
        private final long rowsEmitted;
        private final long planningNanos;
        private final long processNanos;
        // Maps plan string -> set of worker keys (e.g., "nodeId:worker0") that produced this plan
        private final Map<String, Set<String>> planToWorkers;

        public StreamingLookupStatus(
            long pagesReceived,
            long pagesEmitted,
            long rowsReceived,
            long rowsEmitted,
            long planningNanos,
            long processNanos,
            Map<String, Set<String>> planToWorkers
        ) {
            this.pagesReceived = pagesReceived;
            this.pagesEmitted = pagesEmitted;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
            this.planningNanos = planningNanos;
            this.processNanos = processNanos;
            this.planToWorkers = planToWorkers == null ? Map.of() : planToWorkers;
        }

        public StreamingLookupStatus(StreamInput in) throws IOException {
            this.pagesReceived = in.readVLong();
            this.pagesEmitted = in.readVLong();
            this.rowsReceived = in.readVLong();
            this.rowsEmitted = in.readVLong();
            this.planningNanos = in.readVLong();
            this.processNanos = in.readVLong();
            if (in.getTransportVersion().supports(ESQL_LOOKUP_PLAN_STRING)) {
                this.planToWorkers = in.readMap(i -> new HashSet<>(i.readStringCollectionAsList()));
            } else {
                this.planToWorkers = Map.of();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(pagesReceived);
            out.writeVLong(pagesEmitted);
            out.writeVLong(rowsReceived);
            out.writeVLong(rowsEmitted);
            out.writeVLong(planningNanos);
            out.writeVLong(processNanos);
            if (out.getTransportVersion().supports(ESQL_LOOKUP_PLAN_STRING)) {
                out.writeMap(planToWorkers, StreamOutput::writeStringCollection);
            }
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("pages_received", pagesReceived);
            builder.field("pages_emitted", pagesEmitted);
            builder.field("rows_received", rowsReceived);
            builder.field("rows_emitted", rowsEmitted);
            builder.field("planning_nanos", planningNanos);
            builder.field("process_nanos", processNanos);
            if (planToWorkers.isEmpty() == false) {
                builder.startArray("lookup_plans");
                for (Map.Entry<String, Set<String>> entry : planToWorkers.entrySet()) {
                    builder.startObject();
                    builder.array("workers", entry.getValue().toArray(String[]::new));
                    builder.field("plan", entry.getKey());
                    builder.endObject();
                }
                builder.endArray();
            }
            return builder.endObject();
        }

        public long pagesReceived() {
            return pagesReceived;
        }

        public long pagesEmitted() {
            return pagesEmitted;
        }

        public long rowsReceived() {
            return rowsReceived;
        }

        public long rowsEmitted() {
            return rowsEmitted;
        }

        public long planningNanos() {
            return planningNanos;
        }

        public long processNanos() {
            return processNanos;
        }

        public Map<String, Set<String>> planToWorkers() {
            return planToWorkers;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StreamingLookupStatus that = (StreamingLookupStatus) o;
            return pagesReceived == that.pagesReceived
                && pagesEmitted == that.pagesEmitted
                && rowsReceived == that.rowsReceived
                && rowsEmitted == that.rowsEmitted
                && planningNanos == that.planningNanos
                && processNanos == that.processNanos
                && Objects.equals(planToWorkers, that.planToWorkers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pagesReceived, pagesEmitted, rowsReceived, rowsEmitted, planningNanos, processNanos, planToWorkers);
        }

        @Override
        public String toString() {
            return "StreamingLookupStatus{"
                + "pagesReceived="
                + pagesReceived
                + ", pagesEmitted="
                + pagesEmitted
                + ", rowsReceived="
                + rowsReceived
                + ", rowsEmitted="
                + rowsEmitted
                + ", planningNanos="
                + planningNanos
                + ", processNanos="
                + processNanos
                + ", planToWorkers="
                + planToWorkers
                + '}';
        }
    }
}
