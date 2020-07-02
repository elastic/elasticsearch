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
import org.elasticsearch.common.io.stream.DelayableWriteable.Serialized;
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
    private static final Logger logger = LogManager.getLogger(SearchPhaseController.class);

    private final Executor executor;
    private final SearchPhaseController controller;
    private final SearchProgressListener progressListener;
    private final ReduceContextBuilder aggReduceContextBuilder;
    private final NamedWriteableRegistry namedWriteableRegistry;

    private final int topNSize;
    private final int bufferSize;
    private final boolean hasTopDocs;
    private final boolean hasAggs;
    private final boolean performFinalReduce;

    private final TopDocsStats topDocsStats;
    private volatile int numReducePhases;
    private List<SearchShard> processedShards = new ArrayList<>();
    private boolean hasPartialReduce;
    private volatile TopDocs reducedTopDocs;
    private volatile Serialized<InternalAggregations> reducedAggs;

    private final List<QuerySearchResult> buffer = new ArrayList<>();
    private final ArrayDeque<MergeTask> queue = new ArrayDeque<>();
    private final AtomicReference<MergeTask> runningTask = new AtomicReference<>();
    private final AtomicReference<Exception> fatalFailure = new AtomicReference<>();

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
        this.bufferSize = bufferSize;
        this.hasTopDocs = hasTopDocs;
        this.hasAggs = hasAggs;
        this.performFinalReduce = performFinalReduce;
        this.topDocsStats = new TopDocsStats(trackTotalHitsUpTo);
        this.onPartialMergeFailure = onPartialMergeFailure;
    }

    public int getNumReducePhases() {
        return numReducePhases;
    }

    @Override
    void consumeResult(SearchPhaseResult result, Runnable next) {
        super.consumeResult(result, () -> {});
        QuerySearchResult querySearchResult = result.queryResult();
        consumeInternal(querySearchResult, next);
        progressListener.notifyQueryResult(querySearchResult.getShardIndex());
    }

    private void consumeInternal(QuerySearchResult result, Runnable next) {
        boolean shouldExecuteImmediatly = true;
        synchronized (this) {
            if (hasFailure() || result.isNull()) {
                SearchShardTarget target = result.getSearchShardTarget();
                processedShards.add(new SearchShard(target.getClusterAlias(), target.getShardId()));
                result.consumeAll();
            } else {
                int size = buffer.size() + (hasPartialReduce ? 1 : 0);
                if (size == bufferSize) {
                    hasPartialReduce = true;
                    shouldExecuteImmediatly = false;
                    MergeTask task = new MergeTask(buffer, next);
                    buffer.clear();
                    tryExecute(task);
                }
                buffer.add(result);
            }
        }
        if (shouldExecuteImmediatly) {
            next.run();
        }
    }

    @Override
    SearchPhaseController.ReducedQueryPhase reduce() throws Exception {
        if (hasPendingMerges()) {
            throw new AssertionError("partial reduce in-flight");
        } else if (hasFailure()) {
            throw fatalFailure.get();
        }

        logger.trace("aggs final reduction [{}] max [{}]", aggsCurrentBufferSize, aggsMaxBufferSize);
        Collections.sort(buffer, Comparator.comparingInt(QuerySearchResult::getShardIndex));
        SearchPhaseController.ReducedQueryPhase reducePhase = controller.reducedQueryPhase(results.asList(), consumeAggs(buffer),
            consumeTopDocs(buffer), topDocsStats, numReducePhases, false, aggReduceContextBuilder, performFinalReduce);
        progressListener.notifyFinalReduce(SearchProgressListener.buildSearchShards(results.asList()),
            reducePhase.totalHits, reducePhase.aggregations, reducePhase.numReducePhases);
        return reducePhase;
    }


    private void partialReduce(List<QuerySearchResult> toConsume) {
        final List<TopDocs> topDocsToConsume;
        final List<InternalAggregations> aggsToConsume;
        synchronized (this) {
            if (hasFailure()) {
                return;
            }
            topDocsToConsume = consumeTopDocs(toConsume);
            aggsToConsume = consumeAggs(toConsume);

        }
        final TopDocs newTopDocs;
        if (hasTopDocs) {
            newTopDocs = mergeTopDocs(topDocsToConsume,
                // we have to merge here in the same way we collect on a shard
                topNSize, 0);
        } else {
            newTopDocs = null;
        }

        final Serialized<InternalAggregations> newAggs;
        if (hasAggs) {
            InternalAggregations result = InternalAggregations.topLevelReduce(aggsToConsume,
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
        synchronized (this) {
            if (hasFailure())  {
                return;
            }
            reducedTopDocs = newTopDocs;
            reducedAggs = newAggs;
            ++ numReducePhases;
            for (QuerySearchResult result : toConsume) {
                SearchShardTarget target = result.getSearchShardTarget();
                processedShards.add(new SearchShard(target.getClusterAlias(), target.getShardId()));
            }
            progressListener.onPartialReduce(processedShards, topDocsStats.getTotalHits(), reducedAggs, numReducePhases);
        }
    }

    private List<InternalAggregations> consumeAggs(List<QuerySearchResult> results) {
        if (hasAggs) {
            List<InternalAggregations> aggsList = new ArrayList<>();
            if (reducedAggs != null) {
                aggsList.add(reducedAggs.expand());
                reducedAggs = null;
            }
            for (QuerySearchResult result : results) {
                aggsList.add(result.consumeAggs().expand());
            }
            return aggsList;
        } else {
            return null;
        }
    }

    private List<TopDocs> consumeTopDocs(List<QuerySearchResult> results) {
        if (hasTopDocs) {
            List<TopDocs> topDocsList = new ArrayList<>();
            if (reducedTopDocs != null)  {
                topDocsList.add(reducedTopDocs);
            }
            for (QuerySearchResult result : results) {
                TopDocsAndMaxScore topDocs = result.consumeTopDocs();
                topDocsStats.add(topDocs, result.searchTimedOut(), result.terminatedEarly());
                setShardIndex(topDocs.topDocs, result.getShardIndex());
                topDocsList.add(topDocs.topDocs);
            }
            return topDocsList;
        } else {
            return null;
        }
    }

    private synchronized void onFatalFailure(Exception exc) {
        onPartialMergeFailure.accept(exc);
        fatalFailure.compareAndSet(null, exc);
        MergeTask task = runningTask.get();
        if (task != null) {
            task.cancel();
        }
        runningTask.compareAndSet(task, null);
        queue.stream().forEach(MergeTask::cancel);
        queue.clear();
        reducedTopDocs = null;
        reducedAggs = null;
    }

    private boolean hasFailure() {
        return fatalFailure.get() != null;
    }

    synchronized boolean hasPendingMerges() {
        return queue.isEmpty() == false || runningTask.get() != null;
    }

    synchronized void tryExecute(MergeTask task) {
        queue.add(task);
        tryExecuteNext();
    }

    private void tryExecuteNext() {
        final MergeTask task;
        synchronized (this) {
            if (queue.isEmpty()
                    || runningTask.get() != null
                    || hasFailure()) {
                return;
            }
            runningTask.compareAndSet(null, queue.peek());
            task = queue.poll();
        }
        executor.execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                List<QuerySearchResult> toConsume = task.consumeBuffer();
                if (toConsume != null) {
                    partialReduce(toConsume);
                }
            }

            @Override
            public void onAfter() {
                runningTask.compareAndSet(task, null);
                task.consumeListener();
                tryExecuteNext();
            }

            @Override
            public void onFailure(Exception exc) {
                onFatalFailure(exc);
            }
        });
    }

    private static class MergeTask {
        List<QuerySearchResult> buffer;
        Runnable next;

        private MergeTask(List<QuerySearchResult> results, Runnable next) {
            this.buffer = new ArrayList<>(results);
            this.next = next;
        }

        synchronized List<QuerySearchResult> consumeBuffer() {
            List<QuerySearchResult> toRet = buffer;
            buffer = null;
            return toRet;
        }

        synchronized void consumeListener() {
            if (next != null) {
                next.run();
                next = null;
            }
        }

        synchronized void cancel() {
            consumeBuffer();
            consumeListener();
        }
    }
}
