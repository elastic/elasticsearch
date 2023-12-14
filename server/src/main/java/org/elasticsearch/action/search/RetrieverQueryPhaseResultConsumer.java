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
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.retriever.RetrieverBuilder;

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
public class RetrieverQueryPhaseResultConsumer extends ArraySearchPhaseResults<SearchPhaseResult> {
    private static final Logger logger = LogManager.getLogger(QueryPhaseResultConsumer.class);

    private final Executor executor;
    private final CircuitBreaker circuitBreaker;
    private final SearchProgressListener progressListener;
    private final AggregationReduceContext.Builder aggReduceContextBuilder;
    private final RetrieverBuilder<?> retrieverBuilder;
    private final int queryCount;
    private final int numShards;
    private final boolean hasAggs;
    private final boolean performFinalReduce;
    private final Consumer<Exception> onPartialMergeFailure;

    private final Object lock = new Object();
    private final List<PendingMerge> pendingMerges;
    private final List<MergeResult> mergeResults;
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

    /**
     * Creates a {@link QueryPhaseResultConsumer} that incrementally reduces aggregation results
     * as shard results are consumed.
     */
    public RetrieverQueryPhaseResultConsumer(
        SearchRequest request,
        Executor executor,
        CircuitBreaker circuitBreaker,
        SearchPhaseController controller,
        Supplier<Boolean> isCanceled,
        SearchProgressListener progressListener,
        int queryCount,
        int numShards,
        Consumer<Exception> onPartialMergeFailure
    ) {
        super(queryCount * numShards);
        this.executor = executor;
        this.circuitBreaker = circuitBreaker;
        this.progressListener = progressListener;
        this.performFinalReduce = request.isFinalReduce();
        this.onPartialMergeFailure = onPartialMergeFailure;

        this.queryCount = queryCount;
        this.numShards = numShards;

        int expectedResultSize = queryCount * numShards;
        SearchSourceBuilder source = request.source();
        assert source != null;
        this.retrieverBuilder = source.getRetrieverBuilder();
        this.hasAggs = source.aggregations() != null;
        this.aggReduceContextBuilder = hasAggs ? controller.getReduceContext(isCanceled, source.aggregations()) : null;
        pendingMerges = retrieverBuilder.buildPendingMerges(source, request.getBatchedReduceSize(), expectedResultSize);
        mergeResults = new ArrayList<>();
        for (int qci = 0; qci < queryCount; ++qci) {
            mergeResults.add(null);
        }
        LogManager.getLogger(RetrieverQueryPhaseResultConsumer.class).info("EXPECTED: " + expectedResultSize);
    }

    @Override
    protected void doClose() {
        try {
            super.doClose();
        } finally {
            synchronized (lock) {
                if (hasFailure()) {
                    assert circuitBreakerBytes == 0;
                } else {
                    assert circuitBreakerBytes >= 0;
                }

                for (PendingMerge pendingMerge : pendingMerges) {
                    pendingMerge.releaseBuffer();
                }
                circuitBreaker.addWithoutBreaking(-circuitBreakerBytes);
                circuitBreakerBytes = 0;

                if (hasPendingMerges()) {
                    // This is a theoretically unreachable exception.
                    throw new IllegalStateException("Attempted to close with partial reduce in-flight");
                }
            }
        }
    }

    @Override
    public void consumeResult(SearchPhaseResult result, Runnable next) {
        QuerySearchResult querySearchResult = result.queryResult();
        int index = queryCount * querySearchResult.getQueryIndex() + result.getShardIndex();
        results.set(index, querySearchResult);
        querySearchResult.incRef();
        progressListener.notifyQueryResult(querySearchResult.getShardIndex(), querySearchResult);
        LogManager.getLogger(RetrieverQueryPhaseResultConsumer.class).info("CONSUMED");
        consume(querySearchResult, next);
    }

    @Override
    public SearchPhaseController.ReducedQueryPhase reduce() throws Exception {
        if (hasPendingMerges()) {
            throw new AssertionError("partial reduce in-flight");
        } else if (hasFailure()) {
            throw getFailure();
        }

        // ensure consistent ordering
        sortBuffers();
        // TODO: figure out how to handle multiple stats for top docs
        final TopDocsStats topDocsStats = consumeTopDocsStats().get(0);
        final List<TopDocs> topDocsList = consumeTopDocs().get(0);
        final List<InternalAggregations> aggsList = consumeAggs();
        long breakerSize = circuitBreakerBytes;
        if (hasAggs) {
            // Add an estimate of the final reduce size
            breakerSize = addEstimateAndMaybeBreak(estimateRamBytesUsedForReduce(breakerSize));
        }
        SearchPhaseController.ReducedQueryPhase reducePhase = SearchPhaseController.reducedQueryPhase(
            results.asList(),
            aggsList,
            topDocsList,
            topDocsStats,
            numReducePhases,
            false,
            aggReduceContextBuilder,
            null,
            performFinalReduce
        );
        if (hasAggs
            // reduced aggregations can be null if all shards failed
            && reducePhase.aggregations() != null) {

            // Update the circuit breaker to replace the estimation with the serialized size of the newly reduced result
            long finalSize = DelayableWriteable.getSerializedSize(reducePhase.aggregations()) - breakerSize;
            addWithoutBreaking(finalSize);
            logger.trace("aggs final reduction [{}] max [{}]", aggsCurrentBufferSize, maxAggsCurrentBufferSize);
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
        return numReducePhases;
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
        for (PendingMerge pendingMerge : pendingMerges) {
            if (pendingMerge.buffer.isEmpty() == false) {
                pendingMerge.buffer.sort(RESULT_COMPARATOR);
            }
        }
    }

    void addWithoutBreaking(long size) {
        synchronized (lock) {
            circuitBreaker.addWithoutBreaking(size);
            circuitBreakerBytes += size;
            maxAggsCurrentBufferSize = Math.max(maxAggsCurrentBufferSize, circuitBreakerBytes);
        }
    }

    long addEstimateAndMaybeBreak(long estimatedSize) {
        synchronized (lock) {
            circuitBreaker.addEstimateBytesAndMaybeBreak(estimatedSize, "<reduce_aggs>");
            circuitBreakerBytes += estimatedSize;
            maxAggsCurrentBufferSize = Math.max(maxAggsCurrentBufferSize, circuitBreakerBytes);
            return circuitBreakerBytes;
        }
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
        PendingMerge pendingMerge = pendingMerges.get(result.getQueryIndex());
        synchronized (lock) {
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
                if (size >= pendingMerge.batchedReduceSize) {
                    hasPartialReduce = true;
                    executeNextImmediately = false;
                    MergeTask task;
                    if (result.getQueryIndex() == RetrieverBuilder.COMPOUND_QUERY_INDEX) {
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

    private void onMergeFailure(Exception exc) {
        synchronized (lock) {
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
    }

    private void onAfterMerge(MergeTask task, MergeResult newResult, long estimatedSize) {
        synchronized (lock) {
            if (hasFailure()) {
                return;
            }
            runningTask.compareAndSet(task, null);
            mergeResults.set(task.queryIndex, newResult);
            if (hasAggs) {
                // Update the circuit breaker to remove the size of the source aggregations
                // and replace the estimation with the serialized size of the newly reduced result.
                long newSize = newResult.estimatedSize - estimatedSize;
                addWithoutBreaking(newSize);
                logger.trace("aggs partial reduction [{}->{}] max [{}]", estimatedSize, newResult.estimatedSize, maxAggsCurrentBufferSize);
            }
            task.consumeListener();
        }
    }

    private void tryExecuteNext() {
        final MergeTask task;
        final MergeResult result;
        synchronized (lock) {
            if (queue.isEmpty() || hasFailure() || runningTask.get() != null) {
                return;
            }
            task = queue.poll();
            result = mergeResults.get(task.queryIndex);
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

    public List<TopDocsStats> consumeTopDocsStats() {
        synchronized (lock) {
            List<TopDocsStats> topDocsStats = new ArrayList<>();
            for (PendingMerge pendingMerge : pendingMerges) {
                for (QuerySearchResult result : pendingMerge.buffer) {
                    pendingMerge.topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
                }
                topDocsStats.add(pendingMerge.topDocsStats);
            }
            return topDocsStats;
        }
    }

    public List<List<TopDocs>> consumeTopDocs() {
        synchronized (lock) {
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
    }

    public List<InternalAggregations> consumeAggs() {
        synchronized (lock) {
            if (hasAggs == false) {
                return Collections.emptyList();
            }
            List<InternalAggregations> aggsList = new ArrayList<>();
            MergeResult mergeResult = mergeResults.get(RetrieverBuilder.COMPOUND_QUERY_INDEX);
            if (mergeResult != null) {
                aggsList.add(mergeResult.reducedAggs);
            }
            PendingMerge pendingMerge = pendingMerges.get(RetrieverBuilder.COMPOUND_QUERY_INDEX);
            for (QuerySearchResult result : pendingMerge.buffer) {
                aggsList.add(result.consumeAggs());
            }
            return aggsList;
        }
    }

    public static class PendingMerge {
        private final int queryIndex;
        private final int topDocsSize;
        private final TopDocsStats topDocsStats;
        private final int batchedReduceSize;
        private final List<QuerySearchResult> buffer;
        private final List<SearchShard> emptyResults;

        public PendingMerge(int queryIndex, int topDocsSize, int trackTotalHitsUpTo, int batchedReduceSize) {
            this.queryIndex = queryIndex;
            this.topDocsSize = topDocsSize;
            this.topDocsStats = new TopDocsStats(trackTotalHitsUpTo);
            this.batchedReduceSize = batchedReduceSize;
            this.buffer = new ArrayList<>();
            this.emptyResults = new ArrayList<>();
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

    public static class MergeTask {
        private final int queryIndex;
        private final int topDocsSize;
        private final TopDocsStats topDocsStats;
        private final List<SearchShard> emptyResults;
        private QuerySearchResult[] buffer;
        private final long aggsBufferSize;
        private Runnable next;

        private MergeTask(PendingMerge pendingMerge, long aggsBufferSize, Runnable next) {
            this.queryIndex = pendingMerge.queryIndex;
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
