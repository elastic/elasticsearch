/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankCoordinatorContext;

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

import static org.elasticsearch.action.search.SearchPhaseController.getTopDocsSizesPerQuery;
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

    public static final int AGG_QUERY_INDEX = 0;

    private static final Logger logger = LogManager.getLogger(QueryPhaseResultConsumer.class);

    private final Executor executor;
    private final CircuitBreaker circuitBreaker;
    private final SearchProgressListener progressListener;
    private final AggregationReduceContext.Builder aggReduceContextBuilder;
    private final RankCoordinatorContext rankCoordinatorContext;

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
        this.performFinalReduce = request.isFinalReduce();
        this.onPartialMergeFailure = onPartialMergeFailure;

        SearchSourceBuilder source = request.source();
        int size = source == null || source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size();
        int from = source == null || source.from() == -1 ? SearchService.DEFAULT_FROM : source.from();
        this.rankCoordinatorContext = source == null || source.rankBuilder() == null
            ? null
            : source.rankBuilder().buildRankCoordinatorContext(size, from);
        this.hasAggs = source != null && source.aggregations() != null;
        this.aggReduceContextBuilder = hasAggs ? controller.getReduceContext(isCanceled, source.aggregations()) : null;
        List<Integer> topDocsSizesPerQuery = getTopDocsSizesPerQuery(request);
        int batchReduceSize = (hasAggs || topDocsSizesPerQuery.stream().anyMatch(s -> s != 0))
            ? Math.min(request.getBatchedReduceSize(), expectedResultSize)
            : expectedResultSize;
        this.pendingMerges = new PendingMerges(batchReduceSize, request.resolveTrackTotalHitsUpTo(), topDocsSizesPerQuery);
    }

    @Override
    protected void doClose() {
        try {
            super.doClose();
        } finally {
            pendingMerges.close();
        }
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
        } else if (pendingMerges.hasFailure()) {
            throw pendingMerges.getFailure();
        }

        // ensure consistent ordering
        pendingMerges.sortBuffers();
        // TODO: figure out how to handle multiple stats for top docs
        final TopDocsStats topDocsStats = pendingMerges.consumeTopDocsStats().get(0);
        final List<TopDocs> topDocsList = pendingMerges.consumeTopDocs().get(0);
        final List<InternalAggregations> aggsList = pendingMerges.consumeAggs();
        long breakerSize = pendingMerges.circuitBreakerBytes;
        if (hasAggs) {
            // Add an estimate of the final reduce size
            breakerSize = pendingMerges.addEstimateAndMaybeBreak(PendingMerges.estimateRamBytesUsedForReduce(breakerSize));
        }
        SearchPhaseController.ReducedQueryPhase reducePhase = SearchPhaseController.reducedQueryPhase(
            results.asList(),
            aggsList,
            topDocsList,
            topDocsStats,
            pendingMerges.numReducePhases,
            false,
            aggReduceContextBuilder,
            rankCoordinatorContext,
            performFinalReduce
        );
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

    private MergeResult partialReduce(QuerySearchResult[] toConsume, MergeTask mergeTask, MergeResult lastMerge, int numReducePhases) {
        // ensure consistent ordering
        Arrays.sort(toConsume, RESULT_COMPARATOR);

        for (QuerySearchResult result : toConsume) {
            mergeTask.topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
        }

        final TopDocs newTopDocs;
        if (mergeTask.topDocsSize > 0) {
            List<TopDocs> topDocsList = new ArrayList<>();
            if (lastMerge != null) {
                topDocsList.add(lastMerge.reducedTopDocs);
            }
            for (QuerySearchResult result : toConsume) {
                TopDocsAndMaxScore topDocs = result.consumeTopDocs();
                setShardIndex(topDocs.topDocs, result.getShardIndex());
                topDocsList.add(topDocs.topDocs);
            }
            newTopDocs = mergeTopDocs(
                topDocsList,
                // we have to merge here in the same way we collect on a shard
                mergeTask.topDocsSize,
                0
            );
        } else {
            newTopDocs = null;
        }

        final InternalAggregations newAggs;
        if (hasAggs) {
            List<InternalAggregations> aggsList = new ArrayList<>();
            if (lastMerge != null) {
                aggsList.add(lastMerge.reducedAggs);
            }
            for (QuerySearchResult result : toConsume) {
                aggsList.add(result.consumeAggs());
            }
            newAggs = InternalAggregations.topLevelReduce(aggsList, aggReduceContextBuilder.forPartialReduction());
        } else {
            newAggs = null;
        }
        List<SearchShard> processedShards = new ArrayList<>(mergeTask.emptyResults);
        if (lastMerge != null) {
            processedShards.addAll(lastMerge.processedShards);
        }
        for (QuerySearchResult result : toConsume) {
            SearchShardTarget target = result.getSearchShardTarget();
            processedShards.add(new SearchShard(target.getClusterAlias(), target.getShardId()));
        }
        if (progressListener != SearchProgressListener.NOOP) {
            progressListener.notifyPartialReduce(processedShards, mergeTask.topDocsStats.getTotalHits(), newAggs, numReducePhases);
        }
        // we leave the results un-serialized because serializing is slow but we compute the serialized
        // size as an estimate of the memory used by the newly reduced aggregations.
        long serializedSize = hasAggs ? DelayableWriteable.getSerializedSize(newAggs) : 0;
        return new MergeResult(processedShards, newTopDocs, newAggs, hasAggs ? serializedSize : 0);
    }

    public int getNumReducePhases() {
        return pendingMerges.numReducePhases;
    }

    private class PendingMerges implements Releasable {

        private final int batchReduceSize;

        private final List<PendingMerge> pendingMerges = new ArrayList<>();
        private final List<MergeResult> mergeResults = new ArrayList<>();

        // the memory that is accounted in the circuit breaker for this consumer
        private volatile long circuitBreakerBytes;
        // the memory that is currently used in the buffer
        private volatile long aggsCurrentBufferSize;
        private volatile long maxAggsCurrentBufferSize = 0;

        private final ArrayDeque<MergeTask> queue = new ArrayDeque<>();
        private final AtomicReference<MergeTask> runningTask = new AtomicReference<>();
        private final AtomicReference<Exception> failure = new AtomicReference<>();

        private volatile boolean hasPartialReduce;
        private volatile int numReducePhases;

        PendingMerges(int batchReduceSize, int trackTotalHitsUpTo, List<Integer> topDocsSizesPerQuery) {
            this.batchReduceSize = batchReduceSize;
            pendingMerges.add(new QueryPhaseResultConsumer.PendingMerge(0, trackTotalHitsUpTo, topDocsSizesPerQuery.get(0)));
            mergeResults.add(null);
            for (int qci = 1; qci < topDocsSizesPerQuery.size(); ++qci) {
                pendingMerges.add(new QueryPhaseResultConsumer.PendingMerge(qci, 0, topDocsSizesPerQuery.get(qci)));
                mergeResults.add(null);
            }
        }

        @Override
        public synchronized void close() {
            if (hasFailure()) {
                assert circuitBreakerBytes == 0;
            } else {
                assert circuitBreakerBytes >= 0;
            }

            for (QueryPhaseResultConsumer.PendingMerge pendingMerge : pendingMerges) {
                pendingMerge.releaseBuffer();
            }
            circuitBreaker.addWithoutBreaking(-circuitBreakerBytes);
            circuitBreakerBytes = 0;

            if (hasPendingMerges()) {
                // This is a theoretically unreachable exception.
                throw new IllegalStateException("Attempted to close with partial reduce in-flight");
            }
        }

        synchronized Exception getFailure() {
            return failure.get();
        }

        boolean hasFailure() {
            return failure.get() != null;
        }

        boolean hasPendingMerges() {
            return queue.isEmpty() == false || runningTask.get() != null;
        }

        void sortBuffers() {
            for (QueryPhaseResultConsumer.PendingMerge pendingMerge : pendingMerges) {
                if (pendingMerge.buffer.isEmpty() == false) {
                    pendingMerge.buffer.sort(RESULT_COMPARATOR);
                }
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
            boolean executeNextImmediately = true;
            QueryPhaseResultConsumer.PendingMerge pendingMerge = pendingMerges.get(result.queryId());
            synchronized (this) {
                if (hasFailure() || result.isNull()) {
                    result.consumeAll();
                    if (result.isNull()) {
                        SearchShardTarget target = result.getSearchShardTarget();
                        pendingMerge.emptyResults.add(new SearchShard(target.getClusterAlias(), target.getShardId()));
                    }
                } else {
                    if (hasAggs) {
                        long aggsSize = ramBytesUsedQueryResult(result);
                        try {
                            addEstimateAndMaybeBreak(aggsSize);
                        } catch (Exception exc) {
                            result.releaseAggs();
                            pendingMerge.releaseBuffer();
                            onMergeFailure(exc);
                            next.run();
                            return;
                        }
                        aggsCurrentBufferSize += aggsSize;
                    }
                    // add one if a partial merge is pending
                    int size = pendingMerge.buffer.size() + (hasPartialReduce ? 1 : 0);
                    if (size >= batchReduceSize) {
                        hasPartialReduce = true;
                        executeNextImmediately = false;
                        MergeTask task;
                        if (result.queryId() == AGG_QUERY_INDEX) {
                            task = new MergeTask(pendingMerge, aggsCurrentBufferSize, next);
                            aggsCurrentBufferSize = 0;
                        } else {
                            task = new MergeTask(pendingMerge, 0, next);
                        }
                        queue.add(task);
                        tryExecuteNext();
                    }
                    pendingMerge.buffer.add(result);
                }
            }
            if (executeNextImmediately) {
                next.run();
            }
        }

        private synchronized void onMergeFailure(Exception exc) {
            if (hasFailure()) {
                assert circuitBreakerBytes == 0;
                return;
            }
            assert circuitBreakerBytes >= 0;
            if (circuitBreakerBytes > 0) {
                // make sure that we reset the circuit breaker
                circuitBreaker.addWithoutBreaking(-circuitBreakerBytes);
                circuitBreakerBytes = 0;
            }
            failure.compareAndSet(null, exc);
            final List<Releasable> toCancels = new ArrayList<>();
            toCancels.add(() -> onPartialMergeFailure.accept(exc));
            final MergeTask task = runningTask.getAndSet(null);
            if (task != null) {
                toCancels.add(task::cancel);
            }
            MergeTask mergeTask;
            while ((mergeTask = queue.pollFirst()) != null) {
                toCancels.add(mergeTask::cancel);
            }
            for (PendingMerge pendingMerge : pendingMerges) {
                pendingMerge.releaseBuffer();
            }
            pendingMerges.clear();
            mergeResults.clear();
            Releasables.close(toCancels);
        }

        private void onAfterMerge(MergeTask task, MergeResult newResult, long estimatedSize) {
            synchronized (this) {
                if (hasFailure()) {
                    return;
                }
                runningTask.compareAndSet(task, null);
                mergeResults.set(task.queryId, newResult);
                if (hasAggs) {
                    // Update the circuit breaker to remove the size of the source aggregations
                    // and replace the estimation with the serialized size of the newly reduced result.
                    long newSize = newResult.estimatedSize - estimatedSize;
                    addWithoutBreaking(newSize);
                    logger.trace(
                        "aggs partial reduction [{}->{}] max [{}]",
                        estimatedSize,
                        newResult.estimatedSize,
                        maxAggsCurrentBufferSize
                    );
                }
                task.consumeListener();
            }
        }

        private void tryExecuteNext() {
            final MergeTask task;
            final MergeResult result;
            synchronized (this) {
                if (queue.isEmpty() || hasFailure() || runningTask.get() != null) {
                    return;
                }
                task = queue.poll();
                result = mergeResults.get(task.queryId);
                runningTask.compareAndSet(null, task);
            }

            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    long estimatedTotalSize = (result != null ? result.estimatedSize : 0) + task.aggsBufferSize;
                    final MergeResult newMerge;
                    final QuerySearchResult[] toConsume = task.consumeBuffer();
                    if (toConsume == null) {
                        return;
                    }
                    try {
                        long estimatedMergeSize = estimateRamBytesUsedForReduce(estimatedTotalSize);
                        addEstimateAndMaybeBreak(estimatedMergeSize);
                        estimatedTotalSize += estimatedMergeSize;
                        ++numReducePhases;
                        newMerge = partialReduce(toConsume, task, result, numReducePhases);
                    } catch (Exception t) {
                        for (QuerySearchResult result : toConsume) {
                            result.releaseAggs();
                        }
                        onMergeFailure(t);
                        return;
                    }
                    onAfterMerge(task, newMerge, estimatedTotalSize);
                    tryExecuteNext();
                }

                @Override
                public void onFailure(Exception exc) {
                    onMergeFailure(exc);
                }
            });
        }

        public synchronized List<TopDocsStats> consumeTopDocsStats() {
            List<TopDocsStats> topDocsStats = new ArrayList<>();
            for (PendingMerge pendingMerge : pendingMerges) {
                for (QuerySearchResult result : pendingMerge.buffer) {
                    pendingMerge.topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
                }
                topDocsStats.add(pendingMerge.topDocsStats);
            }
            return topDocsStats;
        }

        public synchronized List<List<TopDocs>> consumeTopDocs() {
            int qci = 0;
            List<List<TopDocs>> topDocsPerQuery = new ArrayList<>(qci);
            for (PendingMerge pendingMerge : pendingMerges) {
                if (pendingMerge.topDocsSize == 0) {
                    topDocsPerQuery.add(Collections.emptyList());
                } else {
                    List<TopDocs> topDocs = new ArrayList<>();
                    if (mergeResults.get(qci) != null) {
                        topDocs.add(mergeResults.get(qci).reducedTopDocs);
                    }
                    for (QuerySearchResult result : pendingMerge.buffer) {
                        TopDocsAndMaxScore resultTopDocs = result.consumeTopDocs();
                        setShardIndex(resultTopDocs.topDocs, result.getShardIndex());
                        topDocs.add(resultTopDocs.topDocs);
                    }
                    topDocsPerQuery.add(topDocs);
                }
                ++qci;
            }
            return topDocsPerQuery;
        }

        public synchronized List<InternalAggregations> consumeAggs() {
            if (hasAggs == false) {
                return Collections.emptyList();
            }
            List<InternalAggregations> aggsList = new ArrayList<>();
            MergeResult mergeResult = mergeResults.get(AGG_QUERY_INDEX);
            if (mergeResult != null) {
                aggsList.add(mergeResult.reducedAggs);
            }
            PendingMerge pendingMerge = pendingMerges.get(AGG_QUERY_INDEX);
            for (QuerySearchResult result : pendingMerge.buffer) {
                aggsList.add(result.consumeAggs());
            }
            return aggsList;
        }
    }

    private static class PendingMerge {
        private final int queryId;
        private final int topDocsSize;
        private final List<QuerySearchResult> buffer = new ArrayList<>();
        private final List<SearchShard> emptyResults = new ArrayList<>();
        private final TopDocsStats topDocsStats;

        private PendingMerge(int queryId, int topDocsSize, int trackTotalHitsUpTo) {
            this.queryId = queryId;
            this.topDocsSize = topDocsSize;
            this.topDocsStats = new TopDocsStats(trackTotalHitsUpTo);
        }

        private void releaseBuffer() {
            buffer.forEach(QuerySearchResult::releaseAggs);
            buffer.clear();
        }
    }

    private record MergeResult(
        List<SearchShard> processedShards,
        TopDocs reducedTopDocs,
        InternalAggregations reducedAggs,
        long estimatedSize
    ) {}

    private static class MergeTask {
        private final int queryId;
        private final int topDocsSize;
        private final TopDocsStats topDocsStats;
        private final List<SearchShard> emptyResults;
        private QuerySearchResult[] buffer;
        private final long aggsBufferSize;
        private Runnable next;

        private MergeTask(PendingMerge pendingMerge, long aggsBufferSize, Runnable next) {
            this.queryId = pendingMerge.queryId;
            this.topDocsSize = pendingMerge.topDocsSize;
            this.topDocsStats = pendingMerge.topDocsStats;
            this.buffer = pendingMerge.buffer.toArray(QuerySearchResult[]::new);
            this.aggsBufferSize = aggsBufferSize;
            this.emptyResults = new ArrayList<>(pendingMerge.emptyResults);
            this.next = next;

            pendingMerge.buffer.clear();
            pendingMerge.emptyResults.clear();
        }

        public synchronized QuerySearchResult[] consumeBuffer() {
            QuerySearchResult[] toRet = buffer;
            buffer = null;
            return toRet;
        }

        public void consumeListener() {
            if (next != null) {
                next.run();
                next = null;
            }
        }

        public synchronized void cancel() {
            QuerySearchResult[] buffer = consumeBuffer();
            if (buffer != null) {
                for (QuerySearchResult result : buffer) {
                    result.releaseAggs();
                }
            }
            consumeListener();
        }
    }
}
