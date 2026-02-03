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
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.exchange.BatchPage;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.lookup.RightChunkedLeftJoin;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final Map<Long, BatchState> activeBatches = new HashMap<>();
    private BidirectionalBatchExchangeClient client;
    private final SubscribableListener<Void> clientReadyListener = new SubscribableListener<>();
    private final IsBlockedResult waitingForClientResult = new IsBlockedResult(clientReadyListener, "waiting for client");
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    private volatile boolean finished = false;
    private volatile boolean closed = false;
    private boolean clientFinishCalled = false;
    private volatile boolean hadMissingMarkersWithoutFailure = false;

    // Stats
    private long pagesReceived = 0;
    private long pagesCompleted = 0;
    private long totalInputRows = 0;
    private long totalOutputRows = 0;

    // Timing stats
    private long planningStartNanos = 0;
    private long planningEndNanos = 0;
    private long processEndNanos = 0;

    // Lookup plan from server (for profile output)
    @Nullable
    private volatile String lookupPlan = null;

    /**
     * State for a single batch (one input page).
     */
    private static class BatchState {
        final long batchId;
        final RightChunkedLeftJoin join;
        boolean receivedLastPage = false;

        BatchState(long batchId, Page inputPage, int loadFieldCount) {
            this.batchId = batchId;
            this.join = new RightChunkedLeftJoin(inputPage, loadFieldCount);
        }
    }

    public StreamingLookupFromIndexOperator(
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
        DiscoveryNode serverNode = determineServerNode();
        if (serverNode == null) {
            clientReadyListener.onFailure(new IllegalStateException("Could not determine server node for lookup"));
            return;
        }

        ExchangeService exchangeService = lookupService.getExchangeService();

        try {
            client = new BidirectionalBatchExchangeClient(
                exchangeSessionId,
                lookupService.getClusterService().getClusterName().value(),
                exchangeService,
                lookupService.getExecutor(),
                exchangeBufferSize,
                lookupService.getTransportService(),
                parentTask,
                serverNode,
                ActionListener.wrap(v -> handleBatchExchangeSuccess(), this::handleBatchExchangeFailure),
                lookupService.getSettings()
            );

            // Send setup request to server
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
                client.getSessionId(),
                profile
            );

            planningStartNanos = System.nanoTime();
            lookupService.lookupAsync(setupRequest, serverNode, parentTask, ActionListener.wrap(response -> {
                planningEndNanos = System.nanoTime();
                // Store the lookup plan from the response (if profiling is enabled)
                lookupPlan = response.planString();
                logger.debug("Client setup complete, connecting to server sink");
                // Connect to server's sink to receive results and send BatchExchangeStatusRequest
                // This starts the server's driver which processes the batches
                client.connectToServerSink();
                clientReadyListener.onResponse(null);
            }, e -> {
                planningEndNanos = System.nanoTime();
                logger.error("Client setup failed", e);
                failure.set(e);
                // Notify the client of the failure to unblock its internal driver
                // The client driver is waiting for pages from the exchange, so we need to signal failure
                client.handleFailure("server setup", e);
                clientReadyListener.onFailure(e);
            }));
        } catch (Exception e) {
            logger.error("Failed to create client", e);
            failure.set(e);
            clientReadyListener.onFailure(e);
        }
    }

    private DiscoveryNode determineServerNode() {
        try {
            var clusterState = lookupService.getClusterService().state();
            var projectState = lookupService.getProjectResolver().getProjectState(clusterState);
            var shardIterators = lookupService.getClusterService()
                .operationRouting()
                .searchShards(projectState, new String[] { lookupIndex }, java.util.Map.of(), "_local");
            if (shardIterators.size() != 1) {
                return null;
            }
            var shardIt = shardIterators.get(0);
            ShardRouting shardRouting = shardIt.nextOrNull();
            if (shardRouting == null) {
                return null;
            }
            return clusterState.nodes().get(shardRouting.currentNodeId());
        } catch (Exception e) {
            logger.error("Failed to determine server node", e);
            return null;
        }
    }

    private void handleBatchExchangeSuccess() {
        logger.debug("Batch exchange completed successfully");
    }

    private void handleBatchExchangeFailure(Exception e) {
        logger.error("Batch exchange failed", e);
        failure.set(e);
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
        // Note: BatchPage constructor calls incRef() on blocks, so we need to release the batchPage on failure
        BatchPage batchPage = null;
        try {
            Block[] inputBlocks = applyMatchFieldsMapping(page);
            batchPage = new BatchPage(new Page(inputBlocks), batchId, 0, true);
            client.sendPage(batchPage);
            logger.trace("addInput: sent batchId={} to exchange buffer", batchId);
        } catch (RuntimeException e) {
            logger.error("addInput: failed to send batchId={}: {}", batchId, e.getMessage());
            // Release the batchPage if it was created (it has incRef'd the blocks)
            if (batchPage != null) {
                batchPage.releaseBlocks();
            }
            // Also release the original page since we won't be using it
            page.releaseBlocks();
            // Rethrow runtime exceptions (like CircuitBreakingException) directly for proper test handling
            throw e;
        } catch (Exception e) {
            logger.error("addInput: failed to send batchId={}: {}", batchId, e.getMessage());
            // Release the batchPage if it was created (it has incRef'd the blocks)
            if (batchPage != null) {
                batchPage.releaseBlocks();
            }
            // Also release the original page since we won't be using it
            page.releaseBlocks();
            failure.set(e);
            return;
        }

        // Track batch for receiving results
        BatchState batch = new BatchState(batchId, page, loadFields.size());
        activeBatches.put(batchId, batch);
        pagesReceived++;
    }

    private Block[] applyMatchFieldsMapping(Page inputPage) {
        Map<Integer, Integer> channelMapping = matchFieldsMapping.channelMapping();
        Block[] result = new Block[channelMapping.size()];
        for (Map.Entry<Integer, Integer> entry : channelMapping.entrySet()) {
            int newIndex = entry.getKey();
            int originalChannel = entry.getValue();
            result[newIndex] = inputPage.getBlock(originalChannel);
        }
        return result;
    }

    @Override
    public Page getOutput() {
        Exception ex = failure.get();
        if (ex != null) {
            // Clean up resources before throwing - release activeBatches
            // This is necessary because the exception may have occurred on the server side,
            // and we're holding resources (like inputPage in RightChunkedLeftJoin) that need releasing
            cleanupBatchResources();

            // Rethrow RuntimeExceptions directly (e.g., CircuitBreakingException)
            // to allow proper handling by the driver and test framework
            if (ex instanceof RuntimeException rte) {
                throw rte;
            }
            throw new IllegalStateException("Batch exchange failed", ex);
        }

        // If missing markers were detected, wait for client to close (which waits for server response).
        // This ensures we give real errors time to arrive before throwing a synthetic error.
        if (hadMissingMarkersWithoutFailure && client != null) {
            // close() is idempotent and waits up to 30s for server response
            try {
                client.close();
            } catch (Exception e) {
                logger.warn("Error closing client while waiting for server response", e);
            }
            // Check again for real error after waiting for server response
            ex = failure.get();
            if (ex != null) {
                cleanupBatchResources();
                if (ex instanceof RuntimeException rte) {
                    throw rte;
                }
                throw new IllegalStateException("Batch exchange failed", ex);
            }
            // No real error arrived after waiting - throw synthetic error
            cleanupBatchResources();
            throw new IllegalStateException(
                "Exchange completed but some batches never received last-page markers. "
                    + "This indicates a bug in the exchange protocol or server."
            );
        }

        if (client == null || clientReadyListener.isDone() == false) {
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
            Iterator<Map.Entry<Long, BatchState>> it = activeBatches.entrySet().iterator();
            while (it.hasNext()) {
                BatchState batch = it.next().getValue();
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
            BatchPage resultPage = client.pollPage();
            if (resultPage == null) {
                // If the page cache is done but we haven't received markers for all batches,
                // we must wait for the server response to know if it succeeded or failed.
                // We can't compute trailing nulls until we know the server completed successfully,
                // otherwise we might produce wrong results (NULLs for rows that actually had matches).
                if (client.isPageCacheDone()) {
                    for (BatchState batch : activeBatches.values()) {
                        if (batch.receivedLastPage == false) {
                            // Check if server failed - if so, let the failure propagate
                            if (client.hasFailed()) {
                                return null;
                            }
                            // Page cache is done but marker is missing and no failure reported yet.
                            // Set flag and proceed to close(), which will wait for the server response.
                            // If no error arrives after waiting, close() will throw.
                            logger.warn("getOutput: pageCacheDone but no marker for batchId={}, proceeding to close", batch.batchId);
                            hadMissingMarkersWithoutFailure = true;
                            return null;
                        }
                    }
                }
                logger.trace(
                    "getOutput: pollPage returned null, activeBatches={}, pageCacheSize={}, pageCacheDone={}",
                    activeBatches.size(),
                    client.pageCacheSize(),
                    client.isPageCacheDone()
                );
                return null;
            }

            logger.trace("getOutput: received result for batch {}, isLast={}", resultPage.batchId(), resultPage.isLastPageInBatch());

            // Result pages were created on the server-side driver, so we need to allow
            // releasing them on this (client-side) driver thread
            resultPage.allowPassingToDifferentDriver();

            // Look up the batch by ID
            BatchState batch = activeBatches.get(resultPage.batchId());
            if (batch == null) {
                resultPage.releaseBlocks();
                throw new IllegalStateException("Received result for unknown batch: batchId=" + resultPage.batchId());
            }

            if (resultPage.isLastPageInBatch()) {
                batch.receivedLastPage = true;
            }

            // BatchPage extends Page, so we can use it directly
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

    private void completeBatch(BatchState batch, Iterator<?> it) {
        if (batch != null) {
            if (client != null) {
                logger.debug("completeBatch: batchId={}", batch.batchId);
                client.markBatchCompleted(batch.batchId);
            }
            Releasables.closeExpectNoException(batch.join);
            it.remove();
            pagesCompleted++;
        }
    }

    /**
     * Clean up all batch resources (activeBatches).
     * Called when an error is detected to release resources before throwing.
     */
    private void cleanupBatchResources() {
        for (BatchState batch : activeBatches.values()) {
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
        // 1. Any batch received last page (needs trailing nulls computed)
        // 2. We have pages ready in the output queue (not just buffered out-of-order pages)
        for (BatchState batch : activeBatches.values()) {
            if (batch.receivedLastPage) {
                return true;
            }
        }
        // Check if there are pages ready to output (in correct order).
        // Using hasReadyPages() instead of pageCacheSize() > 0 prevents busy-spinning
        // when pages arrive out-of-order and are buffered waiting for earlier pages.
        if (client != null && client.hasReadyPages()) {
            return true;
        }
        return false;
    }

    @Override
    public IsBlockedResult isBlocked() {
        Exception ex = failure.get();
        if (ex != null) {
            // it will throw in getOutput() with the exception
            return NOT_BLOCKED;
        }

        if (clientReadyListener.isDone() == false) {
            return waitingForClientResult;
        }

        // If we have active batches waiting for results, check if we need to wait for pages
        if (activeBatches.isEmpty() == false) {
            // Check if any batch has received all results (ready for trailing nulls)
            for (BatchState batch : activeBatches.values()) {
                if (batch.receivedLastPage) {
                    return NOT_BLOCKED; // Data ready to process (trailing nulls)
                }
            }

            // If page cache is done but we have batches without markers, wait for server response
            // to know if the server succeeded or failed before deciding what to do
            if (client.isPageCacheDone()) {
                logger.debug("isBlocked: pageCacheDone but missing markers, waiting for server response");
                return client.waitForServerResponse();
            }

            // No batch has data ready, wait for pages from client
            IsBlockedResult waitResult = client.waitForPage();
            if (waitResult.listener().isDone() == false) {
                logger.trace(
                    "isBlocked: waiting for page, activeBatches={}, pageCacheSize={}, pageCacheDone={}",
                    activeBatches.size(),
                    client.pageCacheSize(),
                    client.isPageCacheDone()
                );
                return waitResult;
            }
            // waitForPage() returned done - check if data is actually ready.
            // If not, we need to keep waiting to avoid spinning.
            if (client.hasReadyPages() == false) {
                // No data ready - if page cache is done, wait for server response
                if (client.isPageCacheDone()) {
                    logger.debug("isBlocked: no ready pages and pageCacheDone, waiting for server response");
                    return client.waitForServerResponse();
                }
                // Otherwise keep waiting on the client (return the waitResult to block on it)
                logger.trace("isBlocked: waitForPage done but no ready pages, continuing to wait");
                return waitResult;
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

        for (BatchState batch : activeBatches.values()) {
            Releasables.closeExpectNoException(batch.join);
        }
        activeBatches.clear();

        if (client != null) {
            // Wait for server setup to complete before closing client.
            // This ensures that clientToServer exchange is open
            // and closing it signals the server that we are closing too
            if (clientReadyListener.isDone() == false) {
                try {
                    PlainActionFuture<Void> waitFuture = new PlainActionFuture<>();
                    clientReadyListener.addListener(waitFuture);
                    waitFuture.actionGet(TimeValue.timeValueSeconds(30));
                } catch (Exception e) {
                    logger.debug("Timeout waiting for server setup during close", e);
                }
            }
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
        return new StreamingLookupStatus(
            pagesReceived,
            pagesCompleted,
            totalInputRows,
            totalOutputRows,
            planningNanos,
            processNanos,
            lookupPlan
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
        @Nullable
        private final String lookupPlan;

        public StreamingLookupStatus(
            long pagesReceived,
            long pagesEmitted,
            long rowsReceived,
            long rowsEmitted,
            long planningNanos,
            long processNanos,
            @Nullable String lookupPlan
        ) {
            this.pagesReceived = pagesReceived;
            this.pagesEmitted = pagesEmitted;
            this.rowsReceived = rowsReceived;
            this.rowsEmitted = rowsEmitted;
            this.planningNanos = planningNanos;
            this.processNanos = processNanos;
            this.lookupPlan = lookupPlan;
        }

        public StreamingLookupStatus(StreamInput in) throws IOException {
            this.pagesReceived = in.readVLong();
            this.pagesEmitted = in.readVLong();
            this.rowsReceived = in.readVLong();
            this.rowsEmitted = in.readVLong();
            this.planningNanos = in.readVLong();
            this.processNanos = in.readVLong();
            if (in.getTransportVersion().supports(ESQL_LOOKUP_PLAN_STRING)) {
                this.lookupPlan = in.readOptionalString();
            } else {
                this.lookupPlan = null;
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
                out.writeOptionalString(lookupPlan);
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
            if (lookupPlan != null) {
                builder.field("lookup_plan", lookupPlan);
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

        @Nullable
        public String lookupPlan() {
            return lookupPlan;
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
                && java.util.Objects.equals(lookupPlan, that.lookupPlan);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(pagesReceived, pagesEmitted, rowsReceived, rowsEmitted, planningNanos, processNanos, lookupPlan);
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
                + ", lookupPlan="
                + lookupPlan
                + '}';
        }
    }
}
