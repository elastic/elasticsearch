/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContextBuilder;
import org.elasticsearch.search.aggregations.InternalAggregations;
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

import static org.elasticsearch.action.search.SearchPhaseController.mergeTopDocs;
import static org.elasticsearch.action.search.SearchPhaseController.setShardIndex;

/**
 * A {@link ArraySearchPhaseResults} implementation that incrementally reduces aggregation results
 * as shard results are consumed.
 * This implementation can be configured to batch up a certain amount of results and reduce
 * them asynchronously in the provided {@link Executor} iff the buffer is exhausted.
 */
class QueryPhaseResultConsumer extends ArraySearchPhaseResults<SearchPhaseResult> {
    private static final Logger logger = LogManager.getLogger(QueryPhaseResultConsumer.class);

    private final Executor executor;
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

    private volatile long aggsMaxBufferSize;
    private volatile long aggsCurrentBufferSize;

    /**
     * Creates a {@link QueryPhaseResultConsumer} that incrementally reduces aggregation results
     * as shard results are consumed.
     */
    QueryPhaseResultConsumer(Executor executor,
                             SearchPhaseController controller,
                             SearchProgressListener progressListener,
                             ReduceContextBuilder aggReduceContextBuilder,
                             NamedWriteableRegistry namedWriteableRegistry,
                             int expectedResultSize,
                             int bufferSize,
                             boolean hasTopDocs,
                             boolean hasAggs,
                             int trackTotalHitsUpTo,
                             int topNSize,
                             boolean performFinalReduce,
                             Consumer<Exception> onPartialMergeFailure) {
        super(expectedResultSize);
        this.executor = executor;
        this.controller = controller;
        this.progressListener = progressListener;
        this.aggReduceContextBuilder = aggReduceContextBuilder;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.topNSize = topNSize;
        this.pendingMerges = new PendingMerges(bufferSize, trackTotalHitsUpTo);
        this.hasTopDocs = hasTopDocs;
        this.hasAggs = hasAggs;
        this.performFinalReduce = performFinalReduce;
        this.onPartialMergeFailure = onPartialMergeFailure;
    }

    @Override
    void consumeResult(SearchPhaseResult result, Runnable next) {
        super.consumeResult(result, () -> {});
        QuerySearchResult querySearchResult = result.queryResult();
        progressListener.notifyQueryResult(querySearchResult.getShardIndex());
        pendingMerges.consume(querySearchResult, next);
    }

    @Override
    SearchPhaseController.ReducedQueryPhase reduce() throws Exception {
        if (pendingMerges.hasPendingMerges()) {
            throw new AssertionError("partial reduce in-flight");
        } else if (pendingMerges.hasFailure()) {
            throw pendingMerges.getFailure();
        }

        logger.trace("aggs final reduction [{}] max [{}]", aggsCurrentBufferSize, aggsMaxBufferSize);
        // ensure consistent ordering
        pendingMerges.sortBuffer();
        final TopDocsStats topDocsStats = pendingMerges.consumeTopDocsStats();
        final List<TopDocs> topDocsList = pendingMerges.consumeTopDocs();
        final List<InternalAggregations> aggsList = pendingMerges.consumeAggs();
        SearchPhaseController.ReducedQueryPhase reducePhase = controller.reducedQueryPhase(results.asList(), aggsList,
            topDocsList, topDocsStats, pendingMerges.numReducePhases, false, aggReduceContextBuilder, performFinalReduce);
        progressListener.notifyFinalReduce(SearchProgressListener.buildSearchShards(results.asList()),
            reducePhase.totalHits, reducePhase.aggregations, reducePhase.numReducePhases);
        return reducePhase;
    }

    private MergeResult partialReduce(MergeTask task,
                                      TopDocsStats topDocsStats,
                                      MergeResult lastMerge,
                                      int numReducePhases) {
        final QuerySearchResult[] toConsume = task.consumeBuffer();
        if (toConsume == null) {
            // the task is cancelled
            return null;
        }
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

        final DelayableWriteable.Serialized<InternalAggregations> newAggs;
        if (hasAggs) {
            List<InternalAggregations> aggsList = new ArrayList<>();
            if (lastMerge != null) {
                aggsList.add(lastMerge.reducedAggs.expand());
            }
            for (QuerySearchResult result : toConsume) {
                aggsList.add(result.consumeAggs().expand());
            }
            InternalAggregations result = InternalAggregations.topLevelReduce(aggsList,
                aggReduceContextBuilder.forPartialReduction());
            newAggs = DelayableWriteable.referencing(result).asSerialized(InternalAggregations::readFrom, namedWriteableRegistry);
            long previousBufferSize = aggsCurrentBufferSize;
            aggsCurrentBufferSize = newAggs.ramBytesUsed();
            aggsMaxBufferSize = Math.max(aggsCurrentBufferSize, aggsMaxBufferSize);
            logger.trace("aggs partial reduction [{}->{}] max [{}]",
                previousBufferSize, aggsCurrentBufferSize, aggsMaxBufferSize);
        } else {
            newAggs = null;
        }
        List<SearchShard> processedShards = new ArrayList<>(task.emptyResults);
        if (lastMerge != null) {
            processedShards.addAll(lastMerge.processedShards);
        }
        for (QuerySearchResult result : toConsume) {
            SearchShardTarget target = result.getSearchShardTarget();
            processedShards.add(new SearchShard(target.getClusterAlias(), target.getShardId()));
        }
        progressListener.onPartialReduce(processedShards, topDocsStats.getTotalHits(), newAggs, numReducePhases);
        return new MergeResult(processedShards, newTopDocs, newAggs);
    }

    public int getNumReducePhases() {
        return pendingMerges.numReducePhases;
    }

    private class PendingMerges {
        private final int bufferSize;

        private int index;
        private final QuerySearchResult[] buffer;
        private final List<SearchShard> emptyResults = new ArrayList<>();

        private final TopDocsStats topDocsStats;
        private MergeResult mergeResult;
        private final ArrayDeque<MergeTask> queue = new ArrayDeque<>();
        private final AtomicReference<MergeTask> runningTask = new AtomicReference<>();
        private final AtomicReference<Exception> failure = new AtomicReference<>();

        private boolean hasPartialReduce;
        private int numReducePhases;

        PendingMerges(int bufferSize, int trackTotalHitsUpTo) {
            this.bufferSize = bufferSize;
            this.topDocsStats = new TopDocsStats(trackTotalHitsUpTo);
            this.buffer = new QuerySearchResult[bufferSize];
        }

        public boolean hasFailure() {
            return failure.get() != null;
        }

        public synchronized boolean hasPendingMerges() {
            return queue.isEmpty() == false || runningTask.get() != null;
        }

        public synchronized void sortBuffer() {
            if (index > 0) {
                Arrays.sort(buffer, 0, index, Comparator.comparingInt(QuerySearchResult::getShardIndex));
            }
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
                    // add one if a partial merge is pending
                    int size = index + (hasPartialReduce ? 1 : 0);
                    if (size >= bufferSize) {
                        hasPartialReduce = true;
                        executeNextImmediately = false;
                        QuerySearchResult[] clone = new QuerySearchResult[index];
                        System.arraycopy(buffer, 0, clone, 0, index);
                        MergeTask task = new MergeTask(clone, new ArrayList<>(emptyResults), next);
                        Arrays.fill(buffer, null);
                        emptyResults.clear();
                        index = 0;
                        queue.add(task);
                        tryExecuteNext();
                    }
                    buffer[index++] = result;
                }
            }
            if (executeNextImmediately) {
                next.run();
            }
        }

        private void onMergeFailure(Exception exc) {
            synchronized (this) {
                if (failure.get() != null) {
                    return;
                }
                failure.compareAndSet(null, exc);
                MergeTask task = runningTask.get();
                runningTask.compareAndSet(task, null);
                onPartialMergeFailure.accept(exc);
                List<MergeTask> toCancel = new ArrayList<>();
                if (task != null) {
                    toCancel.add(task);
                }
                queue.stream().forEach(toCancel::add);
                queue.clear();
                mergeResult = null;
                toCancel.stream().forEach(MergeTask::cancel);
            }
        }

        private void onAfterMerge(MergeTask task, MergeResult newResult) {
            synchronized (this) {
                runningTask.compareAndSet(task, null);
                mergeResult = newResult;
            }
            task.consumeListener();
        }

        private void tryExecuteNext() {
            final MergeTask task;
            synchronized (this) {
                if (queue.isEmpty()
                        || failure.get() != null
                        || runningTask.get() != null) {
                    return;
                }
                task = queue.poll();
                runningTask.compareAndSet(null, task);
            }
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    final MergeResult newMerge;
                    try {
                        newMerge = partialReduce(task, topDocsStats, mergeResult, ++numReducePhases);
                    } catch (Exception t) {
                        onMergeFailure(t);
                        return;
                    }
                    onAfterMerge(task, newMerge);
                    tryExecuteNext();
                }

                @Override
                public void onFailure(Exception exc) {
                    onMergeFailure(exc);
                }
            });
        }

        public TopDocsStats consumeTopDocsStats() {
            for (int i = 0; i < index; i++) {
                QuerySearchResult result = buffer[i];
                topDocsStats.add(result.topDocs(), result.searchTimedOut(), result.terminatedEarly());
            }
            return topDocsStats;
        }

        public List<TopDocs> consumeTopDocs() {
            if (hasTopDocs == false) {
                return Collections.emptyList();
            }
            List<TopDocs> topDocsList = new ArrayList<>();
            if (mergeResult != null) {
                topDocsList.add(mergeResult.reducedTopDocs);
            }
            for (int i = 0; i < index; i++) {
                QuerySearchResult result = buffer[i];
                TopDocsAndMaxScore topDocs = result.consumeTopDocs();
                setShardIndex(topDocs.topDocs, result.getShardIndex());
                topDocsList.add(topDocs.topDocs);
            }
            return topDocsList;
        }

        public List<InternalAggregations> consumeAggs() {
            if (hasAggs == false) {
                return Collections.emptyList();
            }
            List<InternalAggregations> aggsList = new ArrayList<>();
            if (mergeResult != null) {
                aggsList.add(mergeResult.reducedAggs.expand());
            }
            for (int i = 0; i < index; i++) {
                QuerySearchResult result = buffer[i];
                aggsList.add(result.consumeAggs().expand());
            }
            return aggsList;
        }

        public Exception getFailure() {
            return failure.get();
        }
    }

    private static class MergeResult {
        private final List<SearchShard> processedShards;
        private final TopDocs reducedTopDocs;
        private final DelayableWriteable.Serialized<InternalAggregations> reducedAggs;

        private MergeResult(List<SearchShard> processedShards, TopDocs reducedTopDocs,
                            DelayableWriteable.Serialized<InternalAggregations> reducedAggs) {
            this.processedShards = processedShards;
            this.reducedTopDocs = reducedTopDocs;
            this.reducedAggs = reducedAggs;
        }
    }

    private static class MergeTask {
        private final List<SearchShard> emptyResults;
        private QuerySearchResult[] buffer;
        private Runnable next;

        private MergeTask(QuerySearchResult[] buffer, List<SearchShard> emptyResults, Runnable next) {
            this.buffer = buffer;
            this.emptyResults = emptyResults;
            this.next = next;
        }

        public synchronized QuerySearchResult[] consumeBuffer() {
            QuerySearchResult[] toRet = buffer;
            buffer = null;
            return toRet;
        }

        public synchronized void consumeListener() {
            if (next != null) {
                next.run();
                next = null;
            }
        }

        public synchronized void cancel() {
            consumeBuffer();
            consumeListener();
        }
    }
}
