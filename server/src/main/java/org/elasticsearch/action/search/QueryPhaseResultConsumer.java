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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContextBuilder;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchResult;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

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
public class QueryPhaseResultConsumer extends ArraySearchPhaseResults<SearchPhaseResult> implements Releasable {
    private static final Logger logger = LogManager.getLogger(QueryPhaseResultConsumer.class);

    private final Executor executor;
    private final CircuitBreaker circuitBreaker;
    private final SearchPhaseController controller;
    private final SearchProgressListener progressListener;
    private final ReduceContextBuilder aggReduceContextBuilder;
    private final NamedWriteableRegistry namedWriteableRegistry;

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
    public QueryPhaseResultConsumer(SearchRequest request,
                                    Executor executor,
                                    CircuitBreaker circuitBreaker,
                                    SearchPhaseController controller,
                                    SearchProgressListener progressListener,
                                    NamedWriteableRegistry namedWriteableRegistry,
                                    int expectedResultSize,
                                    Consumer<Exception> onPartialMergeFailure) {
        super(expectedResultSize);
        this.executor = executor;
        this.circuitBreaker = circuitBreaker;
        this.controller = controller;
        this.progressListener = progressListener;
        this.aggReduceContextBuilder = controller.getReduceContext(request);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.topNSize = getTopDocsSize(request);
        this.performFinalReduce = request.isFinalReduce();
        this.onPartialMergeFailure = onPartialMergeFailure;

        SearchSourceBuilder source = request.source();
        this.hasTopDocs = source == null || source.size() != 0;
        this.hasAggs = source != null && source.aggregations() != null;
        int batchReduceSize = (hasAggs || hasTopDocs) ? Math.min(request.getBatchedReduceSize(), expectedResultSize)  : expectedResultSize;
        this.pendingMerges = new PendingMerges(batchReduceSize, request.resolveTrackTotalHitsUpTo());
    }

    @Override
    public void close() {
        Releasables.close(pendingMerges);
    }

    @Override
    public void consumeResult(SearchPhaseResult result, Runnable next) {
        super.consumeResult(result, () -> {});
        QuerySearchResult querySearchResult = result.queryResult();
        progressListener.notifyQueryResult(querySearchResult.getShardIndex());
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
        pendingMerges.sortBuffer();
        final TopDocsStats topDocsStats = pendingMerges.consumeTopDocsStats();
        final List<TopDocs> topDocsList = pendingMerges.consumeTopDocs();
        final List<InternalAggregations> aggsList = pendingMerges.consumeAggs();
        long breakerSize = pendingMerges.circuitBreakerBytes;
        if (hasAggs) {
            // Add an estimate of the final reduce size
            breakerSize = pendingMerges.addEstimateAndMaybeBreak(pendingMerges.estimateRamBytesUsedForReduce(breakerSize));
        }
        SearchPhaseController.ReducedQueryPhase reducePhase = controller.reducedQueryPhase(results.asList(), aggsList,
            topDocsList, topDocsStats, pendingMerges.numReducePhases, false, aggReduceContextBuilder, performFinalReduce);
        if (hasAggs
                // reduced aggregations can be null if all shards failed
                && reducePhase.aggregations != null) {

            // Update the circuit breaker to replace the estimation with the serialized size of the newly reduced result
            long finalSize = DelayableWriteable.getSerializedSize(reducePhase.aggregations) - breakerSize;
            pendingMerges.addWithoutBreaking(finalSize);
            logger.trace("aggs final reduction [{}] max [{}]",
                pendingMerges.aggsCurrentBufferSize, pendingMerges.maxAggsCurrentBufferSize);
        }
        progressListener.notifyFinalReduce(SearchProgressListener.buildSearchShards(results.asList()),
            reducePhase.totalHits, reducePhase.aggregations, reducePhase.numReducePhases);
        return reducePhase;
    }

    private MergeResult partialReduce(QuerySearchResult[] toConsume,
                                      List<SearchShard> emptyResults,
                                      TopDocsStats topDocsStats,
                                      MergeResult lastMerge,
                                      int numReducePhases) {
        // ensure consistent ordering
        Arrays.sort(toConsume, Comparator.comparingInt(QuerySearchResult::getShardIndex));

        for (QuerySearchResult result : toConsume) {
            topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
        }

        final TopDocs newTopDocs;
        if (hasTopDocs) {
            List<TopDocs> topDocsList = new ArrayList<>();
            if (lastMerge != null) {
                topDocsList.add(lastMerge.reducedTopDocs);
            }
            for (QuerySearchResult result : toConsume) {
                TopDocsAndMaxScore topDocs = result.consumeTopDocs();
                setShardIndex(topDocs.topDocs, result.getShardIndex());
                topDocsList.add(topDocs.topDocs);
            }
            newTopDocs = mergeTopDocs(topDocsList,
                // we have to merge here in the same way we collect on a shard
                topNSize, 0);
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
        List<SearchShard> processedShards = new ArrayList<>(emptyResults);
        if (lastMerge != null) {
            processedShards.addAll(lastMerge.processedShards);
        }
        for (QuerySearchResult result : toConsume) {
            SearchShardTarget target = result.getSearchShardTarget();
            processedShards.add(new SearchShard(target.getClusterAlias(), target.getShardId()));
        }
        progressListener.notifyPartialReduce(processedShards, topDocsStats.getTotalHits(), newAggs, numReducePhases);
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
            assert hasPendingMerges() == false : "cannot close with partial reduce in-flight";
            if (hasFailure()) {
                assert circuitBreakerBytes == 0;
                return;
            }
            assert circuitBreakerBytes >= 0;
            circuitBreaker.addWithoutBreaking(-circuitBreakerBytes);
            circuitBreakerBytes = 0;
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

        void sortBuffer() {
            if (buffer.size() > 0) {
                Collections.sort(buffer, Comparator.comparingInt(QuerySearchResult::getShardIndex));
            }
        }

        synchronized long addWithoutBreaking(long size) {
            circuitBreaker.addWithoutBreaking(size);
            circuitBreakerBytes += size;
            maxAggsCurrentBufferSize = Math.max(maxAggsCurrentBufferSize, circuitBreakerBytes);
            return circuitBreakerBytes;
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
        long estimateRamBytesUsedForReduce(long size) {
            return Math.round(1.5d * size - size);
        }

        public void consume(QuerySearchResult result, Runnable next) {
            boolean executeNextImmediately = true;
            synchronized (this) {
                if (hasFailure() || result.isNull()) {
                    result.consumeAll();
                    if (result.isNull()) {
                        SearchShardTarget target = result.getSearchShardTarget();
                        emptyResults.add(new SearchShard(target.getClusterAlias(), target.getShardId()));
                    }
                } else {
                    if (hasAggs) {
                        long aggsSize = ramBytesUsedQueryResult(result);
                        try {
                            addEstimateAndMaybeBreak(aggsSize);
                        } catch (Exception exc) {
                            result.releaseAggs();
                            buffer.forEach(QuerySearchResult::releaseAggs);
                            buffer.clear();
                            onMergeFailure(exc);
                            next.run();
                            return;
                        }
                        aggsCurrentBufferSize += aggsSize;
                    }
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
            final MergeTask task = runningTask.getAndSet(null);
            final List<Releasable> toCancels = new ArrayList<>();
            toCancels.add(() -> onPartialMergeFailure.accept(exc));
            if (task != null) {
                toCancels.add(task::cancel);
            }
            for (MergeTask mergeTask : queue) {
                toCancels.add(mergeTask::cancel);
            }
            queue.clear();
            mergeResult = null;
            Releasables.close(toCancels);
        }

        private void onAfterMerge(MergeTask task, MergeResult newResult, long estimatedSize) {
            synchronized (this) {
                if (hasFailure()) {
                    return;
                }
                runningTask.compareAndSet(task, null);
                mergeResult = newResult;
                if (hasAggs) {
                    // Update the circuit breaker to remove the size of the source aggregations
                    // and replace the estimation with the serialized size of the newly reduced result.
                    long newSize = mergeResult.estimatedSize - estimatedSize;
                    addWithoutBreaking(newSize);
                    logger.trace("aggs partial reduction [{}->{}] max [{}]",
                        estimatedSize, mergeResult.estimatedSize, maxAggsCurrentBufferSize);
                }
                task.consumeListener();
            }
        }

        private void tryExecuteNext() {
            final MergeTask task;
            synchronized (this) {
                if (queue.isEmpty()
                        || hasFailure()
                        || runningTask.get() != null) {
                    return;
                }
                task = queue.poll();
                runningTask.compareAndSet(null, task);
            }

            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    final MergeResult thisMergeResult = mergeResult;
                    long estimatedTotalSize = (thisMergeResult != null ? thisMergeResult.estimatedSize : 0) + task.aggsBufferSize;
                    final MergeResult newMerge;
                    final QuerySearchResult[] toConsume = task.consumeBuffer();
                    if (toConsume == null) {
                        return;
                    }
                    try {
                        long estimatedMergeSize = estimateRamBytesUsedForReduce(estimatedTotalSize);
                        addEstimateAndMaybeBreak(estimatedMergeSize);
                        estimatedTotalSize += estimatedMergeSize;
                        ++ numReducePhases;
                        newMerge = partialReduce(toConsume, task.emptyResults, topDocsStats, thisMergeResult, numReducePhases);
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

        public synchronized TopDocsStats consumeTopDocsStats() {
            for (QuerySearchResult result : buffer) {
                topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
            }
            return topDocsStats;
        }

        public synchronized List<TopDocs> consumeTopDocs() {
            if (hasTopDocs == false) {
                return Collections.emptyList();
            }
            List<TopDocs> topDocsList = new ArrayList<>();
            if (mergeResult != null) {
                topDocsList.add(mergeResult.reducedTopDocs);
            }
            for (QuerySearchResult result : buffer) {
                TopDocsAndMaxScore topDocs = result.consumeTopDocs();
                setShardIndex(topDocs.topDocs, result.getShardIndex());
                topDocsList.add(topDocs.topDocs);
            }
            return topDocsList;
        }

        public synchronized List<InternalAggregations> consumeAggs() {
            if (hasAggs == false) {
                return Collections.emptyList();
            }
            List<InternalAggregations> aggsList = new ArrayList<>();
            if (mergeResult != null) {
                aggsList.add(mergeResult.reducedAggs);
            }
            for (QuerySearchResult result : buffer) {
                aggsList.add(result.consumeAggs());
            }
            return aggsList;
        }
    }

    private static class MergeResult {
        private final List<SearchShard> processedShards;
        private final TopDocs reducedTopDocs;
        private final InternalAggregations reducedAggs;
        private final long estimatedSize;

        private MergeResult(List<SearchShard> processedShards, TopDocs reducedTopDocs,
                            InternalAggregations reducedAggs, long estimatedSize) {
            this.processedShards = processedShards;
            this.reducedTopDocs = reducedTopDocs;
            this.reducedAggs = reducedAggs;
            this.estimatedSize = estimatedSize;
        }
    }

    private static class MergeTask {
        private final List<SearchShard> emptyResults;
        private QuerySearchResult[] buffer;
        private long aggsBufferSize;
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
