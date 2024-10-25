/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSize;
import static org.elasticsearch.action.search.SearchPhaseController.mergeTopDocs;
import static org.elasticsearch.action.search.SearchPhaseController.setShardIndex;

/**
 * A {@link ArraySearchPhaseResults} implementation that incrementally reduces aggregation results
 * as shard results are consumed.
 * This implementation adds the memory that it used to save and reduce the results of shard aggregations
 * in the {@link CircuitBreaker#REQUEST} circuit breaker. Before any partial or final reduce, the memory
 * needed to reduce the aggregations is estimated and a {@link CircuitBreakingException} is thrown if it
 * exceeds the maximum memory allowed in this breaker.
 */
public class QueryPhaseResultConsumer extends ArraySearchPhaseResults<SearchPhaseResult> {
    private static final Logger logger = LogManager.getLogger(QueryPhaseResultConsumer.class);

    private final Executor executor;
    private final CircuitBreaker circuitBreaker;
    private final SearchProgressListener progressListener;
    private final AggregationReduceContext.Builder aggReduceContextBuilder;
    private final QueryPhaseRankCoordinatorContext queryPhaseRankCoordinatorContext;

    private final int topNSize;
    private final boolean hasTopDocs;
    private final boolean hasAggs;
    private final boolean performFinalReduce;

    private final PendingMerges pendingMerges;
    private final Consumer<Exception> onPartialMergeFailure;

    /**
     * Creates a {@link QueryPhaseResultConsumer} that incrementally reduces aggregation results
     * as shard results are consumed.
     */
    public QueryPhaseResultConsumer(
        SearchRequest request,
        Executor executor,
        CircuitBreaker circuitBreaker,
        SearchPhaseController controller,
        Supplier<Boolean> isCanceled,
        SearchProgressListener progressListener,
        int expectedResultSize,
        Consumer<Exception> onPartialMergeFailure
    ) {
        super(expectedResultSize);
        this.executor = executor;
        this.circuitBreaker = circuitBreaker;
        this.progressListener = progressListener;
        this.topNSize = getTopDocsSize(request);
        this.performFinalReduce = request.isFinalReduce();
        this.onPartialMergeFailure = onPartialMergeFailure;

        SearchSourceBuilder source = request.source();
        int size = source == null || source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size();
        int from = source == null || source.from() == -1 ? SearchService.DEFAULT_FROM : source.from();
        this.queryPhaseRankCoordinatorContext = source == null || source.rankBuilder() == null
            ? null
            : source.rankBuilder().buildQueryPhaseCoordinatorContext(size, from);
        this.hasTopDocs = (source == null || size != 0) && queryPhaseRankCoordinatorContext == null;
        this.hasAggs = source != null && source.aggregations() != null;
        this.aggReduceContextBuilder = hasAggs ? controller.getReduceContext(isCanceled, source.aggregations()) : null;
        int batchReduceSize = (hasAggs || hasTopDocs) ? Math.min(request.getBatchedReduceSize(), expectedResultSize) : expectedResultSize;
        this.pendingMerges = new PendingMerges(batchReduceSize, request.resolveTrackTotalHitsUpTo());
    }

    @Override
    protected void doClose() {
        pendingMerges.close();
    }

    @Override
    public void consumeResult(SearchPhaseResult result, Runnable next) {
        super.consumeResult(result, () -> {});
        QuerySearchResult querySearchResult = result.queryResult();
        progressListener.notifyQueryResult(querySearchResult.getShardIndex(), querySearchResult);
        pendingMerges.consume(querySearchResult, next);
    }

    @Override
    public SearchPhaseController.ReducedQueryPhase reduce() throws Exception {
        if (pendingMerges.hasPendingMerges()) {
            throw new AssertionError("partial reduce in-flight");
        }
        Exception failure = pendingMerges.failure.get();
        if (failure != null) {
            throw failure;
        }

        // ensure consistent ordering
        pendingMerges.sortBuffer();
        final TopDocsStats topDocsStats = pendingMerges.topDocsStats;
        final int resultSize = pendingMerges.buffer.size() + (pendingMerges.mergeResult == null ? 0 : 1);
        final List<TopDocs> topDocsList = hasTopDocs ? new ArrayList<>(resultSize) : null;
        final List<DelayableWriteable<InternalAggregations>> aggsList = hasAggs ? new ArrayList<>(resultSize) : null;
        synchronized (pendingMerges) {
            if (pendingMerges.mergeResult != null) {
                if (topDocsList != null) {
                    topDocsList.add(pendingMerges.mergeResult.reducedTopDocs);
                }
                if (aggsList != null) {
                    aggsList.add(DelayableWriteable.referencing(pendingMerges.mergeResult.reducedAggs));
                }
            }
            for (QuerySearchResult result : pendingMerges.buffer) {
                topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
                if (topDocsList != null) {
                    TopDocsAndMaxScore topDocs = result.consumeTopDocs();
                    setShardIndex(topDocs.topDocs, result.getShardIndex());
                    topDocsList.add(topDocs.topDocs);
                }
                if (aggsList != null) {
                    aggsList.add(result.getAggs());
                }
            }
        }
        SearchPhaseController.ReducedQueryPhase reducePhase;
        long breakerSize = pendingMerges.circuitBreakerBytes;
        try {
            if (aggsList != null) {
                // Add an estimate of the final reduce size
                breakerSize = pendingMerges.addEstimateAndMaybeBreak(PendingMerges.estimateRamBytesUsedForReduce(breakerSize));
            }
            reducePhase = SearchPhaseController.reducedQueryPhase(
                results.asList(),
                aggsList,
                topDocsList == null ? Collections.emptyList() : topDocsList,
                topDocsStats,
                pendingMerges.numReducePhases,
                false,
                aggReduceContextBuilder,
                queryPhaseRankCoordinatorContext,
                performFinalReduce
            );
        } finally {
            pendingMerges.releaseAggs();
        }
        if (hasAggs
            // reduced aggregations can be null if all shards failed
            && reducePhase.aggregations() != null) {

            // Update the circuit breaker to replace the estimation with the serialized size of the newly reduced result
            long finalSize = DelayableWriteable.getSerializedSize(reducePhase.aggregations()) - breakerSize;
            pendingMerges.addWithoutBreaking(finalSize);
            logger.trace("aggs final reduction [{}] max [{}]", pendingMerges.aggsCurrentBufferSize, pendingMerges.maxAggsCurrentBufferSize);
        }
        if (progressListener != SearchProgressListener.NOOP) {
            progressListener.notifyFinalReduce(
                SearchProgressListener.buildSearchShards(results.asList()),
                reducePhase.totalHits(),
                reducePhase.aggregations(),
                reducePhase.numReducePhases()
            );
        }
        return reducePhase;
    }

    private static final Comparator<QuerySearchResult> RESULT_COMPARATOR = Comparator.comparingInt(QuerySearchResult::getShardIndex);

    private MergeResult partialReduce(
        QuerySearchResult[] toConsume,
        List<SearchShard> emptyResults,
        TopDocsStats topDocsStats,
        MergeResult lastMerge,
        int numReducePhases
    ) {
        // ensure consistent ordering
        Arrays.sort(toConsume, RESULT_COMPARATOR);

        final List<SearchShard> processedShards = new ArrayList<>(emptyResults);
        final TopDocs newTopDocs;
        final InternalAggregations newAggs;
        final List<DelayableWriteable<InternalAggregations>> aggsList;
        final int resultSetSize = toConsume.length + (lastMerge != null ? 1 : 0);
        if (hasAggs) {
            aggsList = new ArrayList<>(resultSetSize);
            if (lastMerge != null) {
                aggsList.add(DelayableWriteable.referencing(lastMerge.reducedAggs));
            }
        } else {
            aggsList = null;
        }
        List<TopDocs> topDocsList;
        if (hasTopDocs) {
            topDocsList = new ArrayList<>(resultSetSize);
            if (lastMerge != null) {
                topDocsList.add(lastMerge.reducedTopDocs);
            }
        } else {
            topDocsList = null;
        }
        try {
            for (QuerySearchResult result : toConsume) {
                topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
                SearchShardTarget target = result.getSearchShardTarget();
                processedShards.add(new SearchShard(target.getClusterAlias(), target.getShardId()));
                if (aggsList != null) {
                    aggsList.add(result.getAggs());
                }
                if (topDocsList != null) {
                    TopDocsAndMaxScore topDocs = result.consumeTopDocs();
                    setShardIndex(topDocs.topDocs, result.getShardIndex());
                    topDocsList.add(topDocs.topDocs);
                }
            }
            // we have to merge here in the same way we collect on a shard
            newTopDocs = topDocsList == null ? null : mergeTopDocs(topDocsList, topNSize, 0);
            newAggs = aggsList == null
                ? null
                : InternalAggregations.topLevelReduceDelayable(aggsList, aggReduceContextBuilder.forPartialReduction());
        } finally {
            releaseAggs(toConsume);
        }
        if (lastMerge != null) {
            processedShards.addAll(lastMerge.processedShards);
        }
        if (progressListener != SearchProgressListener.NOOP) {
            progressListener.notifyPartialReduce(processedShards, topDocsStats.getTotalHits(), newAggs, numReducePhases);
        }
        // we leave the results un-serialized because serializing is slow but we compute the serialized
        // size as an estimate of the memory used by the newly reduced aggregations.
        return new MergeResult(processedShards, newTopDocs, newAggs, newAggs != null ? DelayableWriteable.getSerializedSize(newAggs) : 0);
    }

    public int getNumReducePhases() {
        return pendingMerges.numReducePhases;
    }

    private class PendingMerges implements Releasable {
        private final int batchReduceSize;
        private final List<QuerySearchResult> buffer = new ArrayList<>();
        private final List<SearchShard> emptyResults = new ArrayList<>();
        // the memory that is accounted in the circuit breaker for this consumer
        private volatile long circuitBreakerBytes;
        // the memory that is currently used in the buffer
        private volatile long aggsCurrentBufferSize;
        private volatile long maxAggsCurrentBufferSize = 0;

        private final ArrayDeque<MergeTask> queue = new ArrayDeque<>();
        private final AtomicReference<MergeTask> runningTask = new AtomicReference<>();
        private final AtomicReference<Exception> failure = new AtomicReference<>();

        private final TopDocsStats topDocsStats;
        private volatile MergeResult mergeResult;
        private volatile boolean hasPartialReduce;
        private volatile int numReducePhases;

        PendingMerges(int batchReduceSize, int trackTotalHitsUpTo) {
            this.batchReduceSize = batchReduceSize;
            this.topDocsStats = new TopDocsStats(trackTotalHitsUpTo);
        }

        @Override
        public synchronized void close() {
            assert assertFailureAndBreakerConsistent();

            releaseBuffer();
            circuitBreaker.addWithoutBreaking(-circuitBreakerBytes);
            circuitBreakerBytes = 0;

            if (hasPendingMerges()) {
                // This is a theoretically unreachable exception.
                throw new IllegalStateException("Attempted to close with partial reduce in-flight");
            }
        }

        private boolean assertFailureAndBreakerConsistent() {
            boolean hasFailure = failure.get() != null;
            if (hasFailure) {
                assert circuitBreakerBytes == 0;
            } else {
                assert circuitBreakerBytes >= 0;
            }
            return true;
        }

        boolean hasFailure() {
            return failure.get() != null;
        }

        boolean hasPendingMerges() {
            return queue.isEmpty() == false || runningTask.get() != null;
        }

        void sortBuffer() {
            if (buffer.size() > 0) {
                buffer.sort(RESULT_COMPARATOR);
            }
        }

        synchronized void addWithoutBreaking(long size) {
            circuitBreaker.addWithoutBreaking(size);
            circuitBreakerBytes += size;
            maxAggsCurrentBufferSize = Math.max(maxAggsCurrentBufferSize, circuitBreakerBytes);
        }

        synchronized long addEstimateAndMaybeBreak(long estimatedSize) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(estimatedSize, "<reduce_aggs>");
            circuitBreakerBytes += estimatedSize;
            maxAggsCurrentBufferSize = Math.max(maxAggsCurrentBufferSize, circuitBreakerBytes);
            return circuitBreakerBytes;
        }

        /**
         * Returns the size of the serialized aggregation that is contained in the
         * provided {@link QuerySearchResult}.
         */
        long ramBytesUsedQueryResult(QuerySearchResult result) {
            return hasAggs ? result.aggregations().getSerializedSize() : 0;
        }

        /**
         * Returns an estimation of the size that a reduce of the provided size
         * would take on memory.
         * This size is estimated as roughly 1.5 times the size of the serialized
         * aggregations that need to be reduced. This estimation can be completely
         * off for some aggregations but it is corrected with the real size after
         * the reduce completes.
         */
        static long estimateRamBytesUsedForReduce(long size) {
            return Math.round(1.5d * size - size);
        }

        public void consume(QuerySearchResult result, Runnable next) {
            if (hasFailure()) {
                result.consumeAll();
                next.run();
            } else if (result.isNull()) {
                result.consumeAll();
                SearchShardTarget target = result.getSearchShardTarget();
                SearchShard searchShard = new SearchShard(target.getClusterAlias(), target.getShardId());
                synchronized (this) {
                    emptyResults.add(searchShard);
                }
                next.run();
            } else {
                final long aggsSize = ramBytesUsedQueryResult(result);
                boolean executeNextImmediately = true;
                boolean hasFailure = false;
                synchronized (this) {
                    if (hasFailure()) {
                        hasFailure = true;
                    } else {
                        if (hasAggs) {
                            try {
                                addEstimateAndMaybeBreak(aggsSize);
                            } catch (Exception exc) {
                                releaseBuffer();
                                onMergeFailure(exc);
                                hasFailure = true;
                            }
                        }
                        if (hasFailure == false) {
                            aggsCurrentBufferSize += aggsSize;
                            // add one if a partial merge is pending
                            int size = buffer.size() + (hasPartialReduce ? 1 : 0);
                            if (size >= batchReduceSize) {
                                hasPartialReduce = true;
                                executeNextImmediately = false;
                                QuerySearchResult[] clone = buffer.toArray(QuerySearchResult[]::new);
                                MergeTask task = new MergeTask(clone, aggsCurrentBufferSize, new ArrayList<>(emptyResults), next);
                                aggsCurrentBufferSize = 0;
                                buffer.clear();
                                emptyResults.clear();
                                queue.add(task);
                                tryExecuteNext();
                            }
                            buffer.add(result);
                        }
                    }
                }
                if (hasFailure) {
                    result.consumeAll();
                }
                if (executeNextImmediately) {
                    next.run();
                }
            }
        }

        private void releaseBuffer() {
            for (QuerySearchResult querySearchResult : buffer) {
                querySearchResult.releaseAggs();
            }
            buffer.clear();
        }

        private synchronized void onMergeFailure(Exception exc) {
            if (failure.compareAndSet(null, exc) == false) {
                assert circuitBreakerBytes == 0;
                return;
            }
            assert circuitBreakerBytes >= 0;
            if (circuitBreakerBytes > 0) {
                // make sure that we reset the circuit breaker
                circuitBreaker.addWithoutBreaking(-circuitBreakerBytes);
                circuitBreakerBytes = 0;
            }
            onPartialMergeFailure.accept(exc);
            final MergeTask task = runningTask.getAndSet(null);
            if (task != null) {
                task.cancel();
            }
            MergeTask mergeTask;
            while ((mergeTask = queue.pollFirst()) != null) {
                mergeTask.cancel();
            }
            mergeResult = null;
        }

        private void tryExecuteNext() {
            final MergeTask task;
            synchronized (this) {
                if (hasFailure() || runningTask.get() != null) {
                    return;
                }
                task = queue.poll();
                runningTask.set(task);
            }
            if (task == null) {
                return;
            }

            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    MergeTask mergeTask = task;
                    QuerySearchResult[] toConsume = mergeTask.consumeBuffer();
                    while (mergeTask != null) {
                        final MergeResult thisMergeResult = mergeResult;
                        long estimatedTotalSize = (thisMergeResult != null ? thisMergeResult.estimatedSize : 0) + mergeTask.aggsBufferSize;
                        final MergeResult newMerge;
                        try {
                            long estimatedMergeSize = estimateRamBytesUsedForReduce(estimatedTotalSize);
                            addEstimateAndMaybeBreak(estimatedMergeSize);
                            estimatedTotalSize += estimatedMergeSize;
                            ++numReducePhases;
                            newMerge = partialReduce(toConsume, mergeTask.emptyResults, topDocsStats, thisMergeResult, numReducePhases);
                        } catch (Exception t) {
                            QueryPhaseResultConsumer.releaseAggs(toConsume);
                            onMergeFailure(t);
                            return;
                        }
                        synchronized (QueryPhaseResultConsumer.this) {
                            if (hasFailure()) {
                                return;
                            }
                            mergeResult = newMerge;
                            if (hasAggs) {
                                // Update the circuit breaker to remove the size of the source aggregations
                                // and replace the estimation with the serialized size of the newly reduced result.
                                long newSize = mergeResult.estimatedSize - estimatedTotalSize;
                                addWithoutBreaking(newSize);
                                if (logger.isTraceEnabled()) {
                                    logger.trace(
                                        "aggs partial reduction [{}->{}] max [{}]",
                                        estimatedTotalSize,
                                        mergeResult.estimatedSize,
                                        maxAggsCurrentBufferSize
                                    );
                                }
                            }
                        }
                        Runnable r = mergeTask.consumeListener();
                        synchronized (QueryPhaseResultConsumer.this) {
                            while (true) {
                                mergeTask = queue.poll();
                                runningTask.set(mergeTask);
                                if (mergeTask == null) {
                                    break;
                                }
                                toConsume = mergeTask.consumeBuffer();
                                if (toConsume != null) {
                                    break;
                                }
                            }
                        }
                        if (r != null) {
                            r.run();
                        }
                    }
                }

                @Override
                public void onFailure(Exception exc) {
                    onMergeFailure(exc);
                }
            });
        }

        public synchronized void releaseAggs() {
            if (hasAggs) {
                for (QuerySearchResult result : buffer) {
                    result.releaseAggs();
                }
            }
        }
    }

    private static void releaseAggs(QuerySearchResult... toConsume) {
        for (QuerySearchResult result : toConsume) {
            result.releaseAggs();
        }
    }

    private record MergeResult(
        List<SearchShard> processedShards,
        TopDocs reducedTopDocs,
        InternalAggregations reducedAggs,
        long estimatedSize
    ) {}

    private static class MergeTask {
        private final List<SearchShard> emptyResults;
        private QuerySearchResult[] buffer;
        private final long aggsBufferSize;
        private Runnable next;

        private MergeTask(QuerySearchResult[] buffer, long aggsBufferSize, List<SearchShard> emptyResults, Runnable next) {
            this.buffer = buffer;
            this.aggsBufferSize = aggsBufferSize;
            this.emptyResults = emptyResults;
            this.next = next;
        }

        public synchronized QuerySearchResult[] consumeBuffer() {
            QuerySearchResult[] toRet = buffer;
            buffer = null;
            return toRet;
        }

        public synchronized Runnable consumeListener() {
            Runnable n = next;
            next = null;
            return n;
        }

        public void cancel() {
            QuerySearchResult[] buffer = consumeBuffer();
            if (buffer != null) {
                releaseAggs(buffer);
            }
            Runnable next = consumeListener();
            if (next != null) {
                next.run();
            }
        }
    }
}
