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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
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

    private final Consumer<Exception> onPartialMergeFailure;

    private final int batchReduceSize;
    List<QuerySearchResult> buffer = new ArrayList<>();
    private List<SearchShard> emptyResults = new ArrayList<>();
    // the memory that is accounted in the circuit breaker for this consumer
    private volatile long circuitBreakerBytes;
    // the memory that is currently used in the buffer
    private volatile long aggsCurrentBufferSize;
    private volatile long maxAggsCurrentBufferSize = 0;

    private final ArrayDeque<MergeTask> queue = new ArrayDeque<>();
    private final AtomicReference<MergeTask> runningTask = new AtomicReference<>();
    public final AtomicReference<Exception> failure = new AtomicReference<>();

    public final TopDocsStats topDocsStats;
    private volatile MergeResult mergeResult;
    private volatile boolean hasPartialReduce;
    private volatile int numReducePhases;

    /**
     * Creates a {@link QueryPhaseResultConsumer} that incrementally reduces aggregation results
     * as shard results are consumed.
     * @param batchReduceSize number of results to accumulate before merging results, or -1 if it should be automatically determined
     *                        from the {@code request}
     */
    public QueryPhaseResultConsumer(
        SearchRequest request,
        Executor executor,
        CircuitBreaker circuitBreaker,
        SearchPhaseController controller,
        Supplier<Boolean> isCanceled,
        SearchProgressListener progressListener,
        int expectedResultSize,
        int batchReduceSize,
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
        if (batchReduceSize >= 0) {
            this.batchReduceSize = batchReduceSize;
        } else {
            this.batchReduceSize = (hasAggs || hasTopDocs)
                ? Math.min(request.getBatchedReduceSize(), expectedResultSize)
                : expectedResultSize;
        }
        topDocsStats = new TopDocsStats(request.resolveTrackTotalHitsUpTo());
    }

    @Override
    protected synchronized void doClose() {
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

    @Override
    public void consumeResult(SearchPhaseResult result, Runnable next) {
        super.consumeResult(result, () -> {});
        QuerySearchResult querySearchResult = result.queryResult();
        progressListener.notifyQueryResult(querySearchResult.getShardIndex(), querySearchResult);
        assert result.getShardIndex() == querySearchResult.getShardIndex();
        consume(querySearchResult, next);
    }

    private final List<Tuple<TopDocsStats, MergeResult>> batchedResults = new ArrayList<>();

    public MergeResult consumePartialResult() {
        var mergeResult = this.mergeResult;
        this.mergeResult = null;
        assert runningTask.get() == null;
        final List<QuerySearchResult> buffer;
        synchronized (this) {
            buffer = this.buffer;
        }
        if (buffer != null && buffer.isEmpty() == false) {
            this.buffer = null;
            buffer.sort(RESULT_COMPARATOR);
            mergeResult = partialReduce(buffer, emptyResults, topDocsStats, mergeResult, numReducePhases++);
            emptyResults = null;
        }
        return mergeResult;
    }

    public void addPartialResult(TopDocsStats topDocsStats, MergeResult mergeResult) {
        if (mergeResult.processedShards.isEmpty() == false) {
            synchronized (batchedResults) {
                batchedResults.add(new Tuple<>(topDocsStats, mergeResult));
            }
        }
    }

    @Override
    public SearchPhaseController.ReducedQueryPhase reduce() throws Exception {
        if (hasPendingMerges()) {
            throw new AssertionError("partial reduce in-flight");
        }
        Exception f = failure.get();
        if (f != null) {
            throw f;
        }
        List<QuerySearchResult> buffer;
        synchronized (this) {
            // final reduce, we're done with the buffer so we just null it out and continue with a local variable to
            // save field references. The synchronized block is never contended but needed to have a memory barrier and sync buffer's
            // contents with all the previous writers to it
            buffer = this.buffer;
            buffer = buffer == null ? Collections.emptyList() : buffer;
            this.buffer = null;

        }
        // ensure consistent ordering
        buffer.sort(RESULT_COMPARATOR);
        final TopDocsStats topDocsStats = this.topDocsStats;
        var mergeResult = this.mergeResult;
        final List<Tuple<TopDocsStats, MergeResult>> batchedResults;
        synchronized (this.batchedResults) {
            batchedResults = this.batchedResults;
        }
        final int resultSize = buffer.size() + (mergeResult == null ? 0 : 1) + batchedResults.size();
        final List<TopDocs> topDocsList = hasTopDocs ? new ArrayList<>(resultSize) : null;
        final List<DelayableWriteable<InternalAggregations>> aggsList = hasAggs ? new ArrayList<>(resultSize) : null;
        if (mergeResult != null) {
            this.mergeResult = null;
            consumePartialMergeResult(mergeResult, topDocsList, aggsList);
        }
        for (int i = 0; i < batchedResults.size(); i++) {
            Tuple<TopDocsStats, MergeResult> batchedResult = batchedResults.set(i, null);
            consumePartialMergeResult(batchedResult.v2(), topDocsList, aggsList);
            topDocsStats.add(batchedResult.v1());
        }
        for (QuerySearchResult result : buffer) {
            if (result.isReduced()) {
                continue;
            }
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
        SearchPhaseController.ReducedQueryPhase reducePhase;
        long breakerSize = circuitBreakerBytes;
        try {
            if (aggsList != null) {
                // Add an estimate of the final reduce size
                breakerSize = addEstimateAndMaybeBreak(estimateRamBytesUsedForReduce(breakerSize));
            }
            reducePhase = SearchPhaseController.reducedQueryPhase(
                results.asList(),
                aggsList,
                topDocsList == null ? Collections.emptyList() : topDocsList,
                topDocsStats,
                2,
                false,
                aggReduceContextBuilder,
                queryPhaseRankCoordinatorContext,
                performFinalReduce
            );
        } finally {
            releaseAggs(buffer);
        }
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

    private static void consumePartialMergeResult(
        MergeResult partialResult,
        List<TopDocs> topDocsList,
        List<DelayableWriteable<InternalAggregations>> aggsList
    ) {
        if (topDocsList != null) {
            topDocsList.add(partialResult.reducedTopDocs);
        }
        if (aggsList != null) {
            aggsList.add(DelayableWriteable.referencing(partialResult.reducedAggs));
        }
    }

    private static final Comparator<QuerySearchResult> RESULT_COMPARATOR = Comparator.comparingInt(QuerySearchResult::getShardIndex);

    private MergeResult partialReduce(
        List<QuerySearchResult> toConsume,
        List<SearchShard> processedShards,
        TopDocsStats topDocsStats,
        MergeResult lastMerge,
        int numReducePhases
    ) {
        // ensure consistent ordering
        toConsume.sort(RESULT_COMPARATOR);

        final TopDocs newTopDocs;
        final InternalAggregations newAggs;
        final List<DelayableWriteable<InternalAggregations>> aggsList;
        final int resultSetSize = toConsume.size() + (lastMerge != null ? 1 : 0);
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
            newTopDocs = topDocsList == null ? Lucene.EMPTY_TOP_DOCS : mergeTopDocs(topDocsList, topNSize, 0);
            newAggs = aggsList == null
                ? null
                : InternalAggregations.topLevelReduceDelayable(aggsList, aggReduceContextBuilder.forPartialReduction());
        } finally {
            releaseAggs(toConsume);
            for (QuerySearchResult querySearchResult : toConsume) {
                querySearchResult.setReduced();
            }
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
        return numReducePhases;
    }

    public boolean hasFailure() {
        return failure.get() != null;
    }

    private boolean hasPendingMerges() {
        return queue.isEmpty() == false || runningTask.get() != null;
    }

    private synchronized void addWithoutBreaking(long size) {
        circuitBreaker.addWithoutBreaking(size);
        circuitBreakerBytes += size;
        maxAggsCurrentBufferSize = Math.max(maxAggsCurrentBufferSize, circuitBreakerBytes);
    }

    private synchronized long addEstimateAndMaybeBreak(long estimatedSize) {
        circuitBreaker.addEstimateBytesAndMaybeBreak(estimatedSize, "<reduce_aggs>");
        circuitBreakerBytes += estimatedSize;
        maxAggsCurrentBufferSize = Math.max(maxAggsCurrentBufferSize, circuitBreakerBytes);
        return circuitBreakerBytes;
    }

    /**
     * Returns the size of the serialized aggregation that is contained in the
     * provided {@link QuerySearchResult}.
     */
    private long ramBytesUsedQueryResult(QuerySearchResult result) {
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
    private static long estimateRamBytesUsedForReduce(long size) {
        return Math.round(1.5d * size - size);
    }

    private void consume(QuerySearchResult result, Runnable next) {
        if (hasFailure()) {
            result.consumeAll();
            next.run();
        } else if (result.isNull() || result.isReduced()) {
            if (result.isReduced()) {
                if (result.hasConsumedTopDocs() == false) {
                    result.consumeTopDocs();
                }
                result.releaseAggs();
            } else {
                result.consumeAll();
            }
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
                        var b = buffer;
                        aggsCurrentBufferSize += aggsSize;
                        // add one if a partial merge is pending
                        int size = b.size() + (hasPartialReduce ? 1 : 0);
                        if (size >= batchReduceSize) {
                            hasPartialReduce = true;
                            executeNextImmediately = false;
                            MergeTask task = new MergeTask(b, aggsCurrentBufferSize, emptyResults, next);
                            b = buffer = new ArrayList<>();
                            emptyResults = new ArrayList<>();
                            aggsCurrentBufferSize = 0;
                            queue.add(task);
                            tryExecuteNext();
                        }
                        b.add(result);
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
        var b = buffer;
        if (b != null) {
            this.buffer = null;
            for (QuerySearchResult querySearchResult : b) {
                querySearchResult.releaseAggs();
            }
        }
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
        assert Thread.holdsLock(this);
        final MergeTask task;
        if (hasFailure() || runningTask.get() != null) {
            return;
        }
        task = queue.poll();
        runningTask.set(task);
        if (task == null) {
            return;
        }

        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() {
                MergeTask mergeTask = task;
                List<QuerySearchResult> toConsume = mergeTask.consumeBuffer();
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

    private static void releaseAggs(List<QuerySearchResult> toConsume) {
        for (QuerySearchResult result : toConsume) {
            result.releaseAggs();
        }
    }

    public record MergeResult(
        List<SearchShard> processedShards,
        TopDocs reducedTopDocs,
        @Nullable InternalAggregations reducedAggs,
        long estimatedSize
    ) implements Writeable {

        static MergeResult readFrom(StreamInput in) throws IOException {
            return new MergeResult(
                in.readCollectionAsImmutableList(i -> new SearchShard(i.readOptionalString(), new ShardId(i))),
                Lucene.readTopDocsOnly(in),
                in.readOptionalWriteable(InternalAggregations::readFrom),
                in.readVLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(processedShards, (o, s) -> {
                o.writeOptionalString(s.clusterAlias());
                s.shardId().writeTo(o);
            });
            Lucene.writeTopDocsIncludingShardIndex(out, reducedTopDocs);
            out.writeOptionalWriteable(reducedAggs);
            out.writeVLong(estimatedSize);
        }
    }

    private static class MergeTask {
        private final List<SearchShard> emptyResults;
        private List<QuerySearchResult> buffer;
        private final long aggsBufferSize;
        private Runnable next;

        private MergeTask(List<QuerySearchResult> buffer, long aggsBufferSize, List<SearchShard> emptyResults, Runnable next) {
            this.buffer = buffer;
            this.aggsBufferSize = aggsBufferSize;
            this.emptyResults = emptyResults;
            this.next = next;
        }

        public synchronized List<QuerySearchResult> consumeBuffer() {
            List<QuerySearchResult> toRet = buffer;
            buffer = null;
            return toRet;
        }

        public synchronized Runnable consumeListener() {
            Runnable n = next;
            next = null;
            return n;
        }

        public void cancel() {
            List<QuerySearchResult> buffer = consumeBuffer();
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
