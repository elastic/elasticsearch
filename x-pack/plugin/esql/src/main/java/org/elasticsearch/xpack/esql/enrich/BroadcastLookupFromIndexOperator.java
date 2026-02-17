/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.compute.data.BatchMetadata;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.operator.exchange.BidirectionalBatchExchangeClient;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.compute.operator.lookup.HashJoin;
import org.elasticsearch.core.ReleasableIterator;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Broadcast join operator: fetches the entire (filtered) right-side dataset from the lookup index
 * via a streaming exchange, builds an in-memory hash table, then joins each left input page locally.
 * <p>
 * Phase 1 (FETCHING_RIGHT): Sends a single dummy page to the server with empty matchFields,
 * causing a match_all (or pre-join-filtered) query. Collects all result pages from the exchange.
 * <p>
 * Phase 2 (JOINING): Builds a {@link HashJoin} from the collected right pages. For each left
 * input page, performs an in-memory hash join and outputs the result.
 */
public class BroadcastLookupFromIndexOperator implements Operator {

    public record Factory(
        List<MatchConfig> matchFields,
        String sessionId,
        CancellableTask parentTask,
        Function<DriverContext, LookupFromIndexService> lookupService,
        String lookupIndexPattern,
        String lookupIndex,
        List<NamedExpression> loadFields,
        Source source,
        PhysicalPlan rightPreJoinPlan,
        int exchangeBufferSize,
        boolean profile,
        int[] leftJoinKeyChannels,
        int[] joinKeyColumnsInRight,
        int addedFieldCount
    ) implements OperatorFactory {

        @Override
        public String describe() {
            return "BroadcastLookupOperator[index=" + lookupIndex + " load_fields=" + loadFields + "]";
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new BroadcastLookupFromIndexOperator(
                matchFields,
                sessionId,
                parentTask,
                lookupService.apply(driverContext),
                lookupIndexPattern,
                lookupIndex,
                loadFields,
                source,
                rightPreJoinPlan,
                exchangeBufferSize,
                profile,
                driverContext.blockFactory(),
                leftJoinKeyChannels,
                joinKeyColumnsInRight,
                addedFieldCount,
                Warnings.createWarnings(driverContext.warningsMode(), source)
            );
        }
    }

    private static final Logger logger = LogManager.getLogger(BroadcastLookupFromIndexOperator.class);
    private static final AtomicLong exchangeIdGenerator = new AtomicLong(0);
    private static final long BROADCAST_BATCH_ID = 0;

    enum Phase {
        FETCHING_RIGHT,
        JOINING
    }

    // Configuration
    private final LookupFromIndexService lookupService;
    private final String sessionId;
    private final String exchangeSessionId;
    private final CancellableTask parentTask;
    private final String lookupIndex;
    private final String lookupIndexPattern;
    private final List<NamedExpression> loadFields;
    private final Source source;
    private final PhysicalPlan rightPreJoinPlan;
    private final int exchangeBufferSize;
    private final List<MatchConfig> matchFields;
    private final boolean profile;
    private final int[] leftJoinKeyChannels;
    private final int[] joinKeyColumnsInRight;
    private final int addedFieldCount;
    private final BlockFactory blockFactory;
    private final Warnings warnings;

    // State
    private Phase phase = Phase.FETCHING_RIGHT;
    private BidirectionalBatchExchangeClient client;
    private final SubscribableListener<Void> clientReadyListener = new SubscribableListener<>();
    private final IsBlockedResult waitingForClientResult = new IsBlockedResult(clientReadyListener, "waiting for client");
    private final AtomicReference<Exception> failure = new AtomicReference<>();
    private final List<Page> collectedRightPages = new ArrayList<>();
    private HashJoin hashJoin;
    private ReleasableIterator<Page> joinIterator;
    private boolean inputFinished = false;
    private volatile boolean closed = false;
    private boolean rightBatchComplete = false;

    // Stats
    private long rightPagesReceived = 0;
    private long leftPagesProcessed = 0;
    private long pagesEmitted = 0;
    private long totalInputRows = 0;
    private long totalOutputRows = 0;
    private long planningStartNanos = 0;
    private long planningEndNanos = 0;
    private long processEndNanos = 0;

    // Lookup plans from servers (for profile output)
    private final ConcurrentHashMap<String, Set<String>> planToWorkers = new ConcurrentHashMap<>();

    public BroadcastLookupFromIndexOperator(
        List<MatchConfig> matchFields,
        String sessionId,
        CancellableTask parentTask,
        LookupFromIndexService lookupService,
        String lookupIndexPattern,
        String lookupIndex,
        List<NamedExpression> loadFields,
        Source source,
        PhysicalPlan rightPreJoinPlan,
        int exchangeBufferSize,
        boolean profile,
        BlockFactory blockFactory,
        int[] leftJoinKeyChannels,
        int[] joinKeyColumnsInRight,
        int addedFieldCount,
        Warnings warnings
    ) {
        this.matchFields = matchFields;
        this.lookupService = lookupService;
        this.sessionId = sessionId;
        this.exchangeSessionId = sessionId + "/broadcast-lookup/" + exchangeIdGenerator.incrementAndGet();
        this.parentTask = parentTask;
        this.lookupIndex = lookupIndex;
        this.lookupIndexPattern = lookupIndexPattern;
        this.loadFields = loadFields;
        this.source = source;
        this.rightPreJoinPlan = rightPreJoinPlan;
        this.exchangeBufferSize = exchangeBufferSize;
        this.profile = profile;
        this.leftJoinKeyChannels = leftJoinKeyChannels;
        this.joinKeyColumnsInRight = joinKeyColumnsInRight;
        this.addedFieldCount = addedFieldCount;
        this.blockFactory = blockFactory;
        this.warnings = warnings;

        logger.info("Using BROADCAST JOIN strategy for lookup index [{}]", lookupIndex);
        initializeClient(blockFactory);
    }

    private void initializeClient(BlockFactory blockFactory) {
        ExchangeService exchangeService = lookupService.getExchangeService();

        try {
            BidirectionalBatchExchangeClient.ServerSetupCallback setupCallback = (
                serverNode,
                clientToServerId,
                serverToClientId,
                listener) -> {
                planningStartNanos = System.nanoTime();
                LookupFromIndexService.Request setupRequest = new LookupFromIndexService.Request(
                    sessionId,
                    lookupIndex,
                    lookupIndexPattern,
                    matchFields,
                    new Page(0),
                    loadFields,
                    source,
                    rightPreJoinPlan,
                    null, // joinOnConditions - broadcast join is field-based with empty matchFields
                    clientToServerId,
                    serverToClientId,
                    profile
                );

                lookupService.lookupAsync(setupRequest, serverNode, parentTask, ActionListener.wrap(response -> {
                    planningEndNanos = System.nanoTime();
                    logger.debug("Server setup complete for broadcast lookup, node={}", serverNode.getId());
                    listener.onResponse(response.planString());
                }, e -> {
                    planningEndNanos = System.nanoTime();
                    logger.error("Server setup failed for broadcast lookup, node=" + serverNode.getId(), e);
                    failure.set(e);
                    listener.onFailure(e);
                }));
            };

            BiConsumer<String, String> planConsumer = profile
                ? (workerKey, planString) -> planToWorkers.computeIfAbsent(planString, k -> ConcurrentHashMap.newKeySet()).add(workerKey)
                : null;

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
                planConsumer,
                1, // maxWorkers: broadcast only needs 1 worker
                this::determineServerNode
            );

            clientReadyListener.onResponse(null);

            // Send a single dummy page with 1 position to trigger the match_all query on the server.
            // The server uses inputPage.getPositionCount() (= 1) to generate one query and returns
            // all matching documents.
            IntBlock dummyBlock = blockFactory.newConstantIntBlockWith(0, 1);
            Page batchPage = new Page(new BatchMetadata(BROADCAST_BATCH_ID, 0, true), dummyBlock);
            // Switch blocks to the parent (global) factory so the server thread can safely release
            // them when client and server are on the same node (exchange passes pages by reference).
            batchPage.allowPassingToDifferentDriver();
            try {
                client.sendPage(batchPage);
            } catch (Exception e) {
                batchPage.releaseBlocks();
                throw e;
            }

            // Signal no more pages to send - we only needed one dummy page
            client.finish();
        } catch (Exception e) {
            logger.error("Failed to initialize broadcast lookup client", e);
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
            logger.error("Failed to determine server node for broadcast lookup", e);
            return null;
        }
    }

    private void handleBatchExchangeSuccess() {
        logger.debug("Broadcast exchange completed successfully");
    }

    private void handleBatchExchangeFailure(Exception e) {
        logger.error("Broadcast exchange failed", e);
        failure.set(e);
    }

    @Override
    public boolean needsInput() {
        if (phase == Phase.FETCHING_RIGHT) {
            return false;
        }
        return inputFinished == false && joinIterator == null && failure.get() == null && closed == false;
    }

    @Override
    public void addInput(Page page) {
        if (closed || failure.get() != null || phase != Phase.JOINING) {
            page.releaseBlocks();
            return;
        }

        totalInputRows += page.getPositionCount();
        leftPagesProcessed++;

        try {
            joinIterator = hashJoin.join(page, leftJoinKeyChannels);
        } catch (RuntimeException e) {
            page.releaseBlocks();
            throw e;
        }
    }

    @Override
    public Page getOutput() {
        checkFailureAndMaybeThrow();

        if (phase == Phase.FETCHING_RIGHT) {
            fetchRightPages();
            return null;
        }

        // Phase JOINING: pull next page from join iterator
        if (joinIterator != null) {
            if (joinIterator.hasNext()) {
                Page output = joinIterator.next();
                totalOutputRows += output.getPositionCount();
                pagesEmitted++;
                if (joinIterator.hasNext() == false) {
                    joinIterator.close();
                    joinIterator = null;
                }
                return output;
            }
            joinIterator.close();
            joinIterator = null;
        }
        return null;
    }

    /**
     * Poll right-side pages from the exchange and collect them. When the batch is complete
     * (last-page marker received), build the hash table and transition to JOINING phase.
     */
    private void fetchRightPages() {
        if (client == null || clientReadyListener.isDone() == false) {
            return;
        }

        while (true) {
            Page resultPage = client.pollPage();
            if (resultPage == null) {
                if (rightBatchComplete) {
                    // All right pages received — build hash table and switch to join phase
                    hashJoin = new HashJoin(collectedRightPages, joinKeyColumnsInRight, addedFieldCount, blockFactory, warnings);
                    // HashJoin now owns the pages; clear our reference without releasing
                    collectedRightPages.clear();
                    phase = Phase.JOINING;
                    logger.debug(
                        "Broadcast right-side fetch complete: {} pages, {} keys, {} rows",
                        rightPagesReceived,
                        hashJoin.keyCount(),
                        hashJoin.indexedRowCount()
                    );
                } else if (client.isExchangeDone()) {
                    // Exchange finished but we never got the last-page marker
                    if (client.hasFailed()) {
                        return; // let failure propagate
                    }
                    failure.compareAndSet(
                        null,
                        new IllegalStateException(
                            "Broadcast exchange completed but never received last-page marker. "
                                + "This indicates a bug in the exchange protocol or server."
                        )
                    );
                }
                return;
            }

            resultPage.allowPassingToDifferentDriver();
            rightPagesReceived++;

            if (resultPage.batchMetadata() != null && resultPage.batchMetadata().isLastPageInBatch()) {
                rightBatchComplete = true;
                if (client != null) {
                    client.markBatchCompleted(BROADCAST_BATCH_ID);
                }
            }

            if (resultPage.getPositionCount() > 0) {
                collectedRightPages.add(resultPage);
            } else {
                resultPage.releaseBlocks();
            }
        }
    }

    private void checkFailureAndMaybeThrow() {
        Exception ex = failure.get();
        if (ex != null) {
            if (ex instanceof RuntimeException rte) {
                throw rte;
            }
            throw new IllegalStateException("Broadcast lookup failed", ex);
        }
    }

    @Override
    public void finish() {
        logger.debug("finish() called for broadcast lookup");
        inputFinished = true;
    }

    @Override
    public boolean isFinished() {
        if (failure.get() != null) {
            return false;
        }
        if (phase == Phase.FETCHING_RIGHT) {
            return false;
        }
        return inputFinished && joinIterator == null;
    }

    @Override
    public boolean canProduceMoreDataWithoutExtraInput() {
        return joinIterator != null;
    }

    @Override
    public IsBlockedResult isBlocked() {
        if (failure.get() != null) {
            return NOT_BLOCKED;
        }

        if (phase == Phase.FETCHING_RIGHT) {
            if (clientReadyListener.isDone() == false) {
                return waitingForClientResult;
            }
            if (client != null) {
                // If right batch is complete, don't block — getOutput will build hash table
                if (rightBatchComplete) {
                    return NOT_BLOCKED;
                }
                if (client.hasReadyPages()) {
                    return NOT_BLOCKED;
                }
                if (client.isExchangeDone()) {
                    // Exchange done but no last marker — wait for server response
                    return client.waitForServerResponse();
                }
                return client.waitUntilPageReady();
            }
        }

        return NOT_BLOCKED;
    }

    @Override
    public void close() {
        logger.debug("close() called for broadcast lookup");
        processEndNanos = System.nanoTime();
        closed = true;

        if (joinIterator != null) {
            joinIterator.close();
            joinIterator = null;
        }

        Releasables.closeExpectNoException(hashJoin);

        // Release any uncollected right pages (only non-empty if hash table was never built)
        for (Page page : collectedRightPages) {
            page.releaseBlocks();
        }
        collectedRightPages.clear();

        if (client != null) {
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
                logger.error("Error finishing client during close", e);
            }
            try {
                client.close();
            } catch (Exception e) {
                logger.error("Error closing client during close", e);
            }
        }
    }

    @Override
    public Status status() {
        long planningNanos = (planningEndNanos > 0 && planningStartNanos > 0) ? (planningEndNanos - planningStartNanos) : 0;
        long processEnd = (processEndNanos > 0) ? processEndNanos : System.nanoTime();
        long processNanos = (planningEndNanos > 0) ? (processEnd - planningEndNanos) : 0;
        return new StreamingLookupFromIndexOperator.StreamingLookupStatus(
            rightPagesReceived + leftPagesProcessed,
            pagesEmitted,
            totalInputRows,
            totalOutputRows,
            planningNanos,
            processNanos,
            new java.util.HashMap<>(planToWorkers)
        );
    }

    @Override
    public String toString() {
        return "BroadcastLookupOperator[index=" + lookupIndex + "]";
    }

    /**
     * XContent for profiling/status output.
     */
    public XContentBuilder toXContent(XContentBuilder builder, org.elasticsearch.xcontent.ToXContent.Params params) throws IOException {
        return status().toXContent(builder, params);
    }
}
