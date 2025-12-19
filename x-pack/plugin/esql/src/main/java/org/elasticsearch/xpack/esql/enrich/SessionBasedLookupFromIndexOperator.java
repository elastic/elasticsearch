/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.lookup.RightChunkedLeftJoin;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Session-based lookup operator that extends LookupFromIndexOperator.
 * This operator reuses the base implementation and only customizes behavior
 * specific to session-based lookups.
 */
public final class SessionBasedLookupFromIndexOperator extends LookupFromIndexOperator {

    public static class Factory implements OperatorFactory {
        private final LookupFromIndexOperator.Factory baseFactory;
        private final String lookupSessionId;

        public Factory(
            List<MatchConfig> matchFields,
            String sessionId,
            String lookupSessionId,
            CancellableTask parentTask,
            int maxOutstandingRequests,
            Function<DriverContext, LookupFromIndexService> lookupService,
            String lookupIndexPattern,
            String lookupIndex,
            List<NamedExpression> loadFields,
            Source source,
            PhysicalPlan rightPreJoinPlan,
            Expression joinOnConditions
        ) {
            this.lookupSessionId = lookupSessionId;
            this.baseFactory = new LookupFromIndexOperator.Factory(
                matchFields,
                sessionId,
                parentTask,
                maxOutstandingRequests,
                lookupService,
                lookupIndexPattern,
                lookupIndex,
                loadFields,
                source,
                rightPreJoinPlan,
                joinOnConditions
            );
        }

        @Override
        public String describe() {
            return baseFactory.describe().replace("LookupOperator", "SessionBasedLookupOperator");
        }

        @Override
        public Operator get(DriverContext driverContext) {
            return new SessionBasedLookupFromIndexOperator(
                baseFactory.matchFields,
                baseFactory.sessionId,
                lookupSessionId,
                driverContext,
                baseFactory.parentTask,
                baseFactory.maxOutstandingRequests,
                baseFactory.lookupService.apply(driverContext),
                baseFactory.lookupIndexPattern,
                baseFactory.lookupIndex,
                baseFactory.loadFields,
                baseFactory.source,
                baseFactory.rightPreJoinPlan,
                baseFactory.joinOnConditions
            );
        }
    }

    private final String lookupSessionId;
    /**
     * Tracks INIT completion per shard/node combination.
     * - If a CompletableFuture exists and is not completed: INIT is pending
     * - If a CompletableFuture exists and is completed: INIT is done, PROCESS can proceed
     * - If no entry exists: first contact, will create future and send INIT
     */
    private final ConcurrentHashMap<ShardNodePair, CompletableFuture<Void>> initCompletions = new ConcurrentHashMap<>();

    /**
     * Represents a shard/node pair for tracking contacted combinations.
     */
    private record ShardNodePair(ShardId shardId, String nodeId) {}

    public SessionBasedLookupFromIndexOperator(
        List<MatchConfig> matchFields,
        String sessionId,
        String lookupSessionId,
        DriverContext driverContext,
        CancellableTask parentTask,
        int maxOutstandingRequests,
        LookupFromIndexService lookupService,
        String lookupIndexPattern,
        String lookupIndex,
        List<NamedExpression> loadFields,
        Source source,
        PhysicalPlan rightPreJoinPlan,
        Expression joinOnConditions
    ) {
        super(
            matchFields,
            sessionId,
            driverContext,
            parentTask,
            maxOutstandingRequests,
            lookupService,
            lookupIndexPattern,
            lookupIndex,
            loadFields,
            source,
            rightPreJoinPlan,
            joinOnConditions
        );
        this.lookupSessionId = lookupSessionId;
    }

    @Override
    protected void performAsync(Page inputPage, ActionListener<OngoingJoin> listener) {
        MatchFieldsMapping mapping = buildMatchFieldsMapping();
        Block[] inputBlockArray = applyMatchFieldsMapping(inputPage, mapping.channelMapping());

        // Resolve shard routing to determine if this is the first contact with this shard/node
        AbstractLookupService.ShardRoutingResult routingResult = lookupService.resolveShardRouting(lookupIndex);
        ShardNodePair shardNodePair = new ShardNodePair(routingResult.shardId(), routingResult.targetNode().getId());

        // Get or create the INIT completion future for this shard/node pair
        CompletableFuture<Void> initFuture = initCompletions.computeIfAbsent(shardNodePair, k -> {
            // First contact: create future and send INIT request (builds and caches operation only)
            CompletableFuture<Void> future = new CompletableFuture<>();
            sendInitRequest(shardNodePair, mapping, future);
            return future;
        });

        // Wait for INIT to complete (if pending) or proceed immediately (if already completed)
        initFuture.whenComplete((v, error) -> {
            if (error != null) {
                Exception exception = error instanceof Exception ? (Exception) error : new RuntimeException(error);
                listener.onFailure(exception);
            } else {
                // INIT completed, now send PROCESS request for this page
                sendProcessRequest(mapping, inputBlockArray, inputPage, listener);
            }
        });
    }

    private void sendInitRequest(ShardNodePair shardNodePair, MatchFieldsMapping mapping, CompletableFuture<Void> initFuture) {
        // Create an empty page for INIT - we only need the structure, not the data
        // Since mergePages is false, determineOptimization won't access the page content
        Page emptyPage = new Page(0);

        LookupFromIndexService.Request initRequest = new LookupFromIndexService.Request(
            sessionId,
            lookupIndex,
            lookupIndexPattern,
            mapping.reindexedMatchFields(),
            emptyPage,
            loadFields,
            source,
            rightPreJoinPlan,
            joinOnConditions,
            lookupSessionId,
            LookupFromIndexService.LookupRequestType.INIT
        );
        // INIT request only builds and caches the operation, does not process the page
        lookupService.lookupAsync(initRequest, parentTask, ActionListener.wrap(response -> {
            // INIT completed successfully (operation cached)
            initFuture.complete(null);
        }, error -> {
            // INIT failed, complete future exceptionally and remove from map
            initCompletions.remove(shardNodePair);
            initFuture.completeExceptionally(error);
        }));
    }

    private void sendProcessRequest(
        MatchFieldsMapping mapping,
        Block[] inputBlockArray,
        Page inputPage,
        ActionListener<OngoingJoin> listener
    ) {
        LookupFromIndexService.Request processRequest = new LookupFromIndexService.Request(
            sessionId,
            lookupIndex,
            lookupIndexPattern,
            mapping.reindexedMatchFields(),
            new Page(inputBlockArray),
            loadFields,
            source,
            rightPreJoinPlan,
            joinOnConditions,
            lookupSessionId,
            LookupFromIndexService.LookupRequestType.PROCESS_PAGE
        );
        lookupService.lookupAsync(
            processRequest,
            parentTask,
            listener.map(pages -> new OngoingJoin(new RightChunkedLeftJoin(inputPage, loadFields.size()), pages.iterator()))
        );
    }

    @Override
    public String toString() {
        return super.toString().replace("LookupOperator", "SessionBasedLookupOperator");
    }

    @Override
    protected Operator.Status status(long receivedPages, long completedPages, long processNanos) {
        return new Status(receivedPages, completedPages, processNanos, totalRows, emittedPages, emittedRows);
    }

    public static class Status extends LookupFromIndexOperator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "session_based_lookup",
            Status::new
        );

        public Status(long receivedPages, long completedPages, long processNanos, long totalRows, long emittedPages, long emittedRows) {
            super(receivedPages, completedPages, processNanos, totalRows, emittedPages, emittedRows);
        }

        public Status(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }
    }
}
