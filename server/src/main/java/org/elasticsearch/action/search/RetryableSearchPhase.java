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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

abstract class RetryableSearchPhase<Result extends SearchPhaseResult> extends SearchPhase {
    private static final TimeValue INITIAL_RETRY_DELAY = TimeValue.timeValueMillis(10);

    private final Logger logger = LogManager.getLogger(RetryableSearchPhase.class);
    private final ThreadPool threadPool;
    private final SearchPhaseContext searchPhaseContext;
    private final TimeoutChecker timeoutChecker;
    private final int shardCount;
    private final SearchShardIteratorRefresher shardIteratorRefresher;
    private final ArraySearchPhaseResults<Result> searchPhaseResults;
    private final List<SearchShardAction> runningSearches;
    private final AtomicBoolean done = new AtomicBoolean(false);
    private final ShardInformationValidator shardInformationValidator;
    private final AtomicInteger pendingShardSearches;

    private volatile Scheduler.ScheduledCancellable timeoutHandler;

    protected RetryableSearchPhase(String name,
                                   ThreadPool threadPool,
                                   SearchPhaseContext searchPhaseContext,
                                   TimeoutChecker timeoutChecker,
                                   SearchShardIteratorRefresher shardIteratorRefresher,
                                   GroupShardsIterator<RetryableSearchShardIterator> shardsIterator,
                                   ShardInformationValidator shardInformationValidator) {
        super(name);
        this.threadPool = threadPool;
        this.searchPhaseContext = searchPhaseContext;
        this.timeoutChecker = timeoutChecker;
        this.shardIteratorRefresher = shardIteratorRefresher;
        this.shardInformationValidator = shardInformationValidator;
        this.shardCount = getShardCount(shardsIterator);
        this.searchPhaseResults = new ArraySearchPhaseResults<>(shardCount);
        this.pendingShardSearches = new AtomicInteger(shardCount);

        ArrayList<SearchShardAction> runningSearches = new ArrayList<>(shardsIterator.size());
        for (int shardIndex = 0; shardIndex < shardsIterator.size(); shardIndex++) {
            final int shardIdx = shardIndex;
            RetryableSearchShardIterator searchShardIterator = shardsIterator.get(shardIndex);

            if (searchShardIterator.skip()) {
                continue;
            }

            SearchShardAction searchShardAction = new SearchShardAction(shardIndex, searchShardIterator, new ActionListener<>() {
                @Override
                public void onResponse(Result result) {
                    try {
                        // TODO should we limit if we set the result after done is set to true?
                        searchPhaseResults.consumeResult(result, () -> decrementPendingShards());
                    } catch (Exception e) {
                        searchPhaseContext.onPhaseFailure(RetryableSearchPhase.this, "", e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        // TODO should we limit if we set the result after done is set to true?
                        searchPhaseContext.onShardFailure(shardIdx, null, e);
                    } finally {
                        decrementPendingShards();
                    }
                }
            });

            runningSearches.add(searchShardAction);
        }
        this.runningSearches = Collections.unmodifiableList(runningSearches);
    }

    protected SearchPhaseContext getSearchPhaseContext() {
        return searchPhaseContext;
    }

    private static int getShardCount(GroupShardsIterator<RetryableSearchShardIterator> shardsIterator) {
        int shardCount = 0;
        for (RetryableSearchShardIterator searchShardIterator : shardsIterator) {
            if (searchShardIterator.skip()) {
                continue;
            }
            shardCount++;
        }
        return shardCount;
    }

    @Override
    public void run() throws IOException {
        TimeValue remainingTime = timeoutChecker.remainingTimeUntilTimeout();
        if (remainingTime.millis() <= 0) {
            searchPhaseContext.onPhaseFailure(this, "", new RuntimeException("timeout"));
            return;
        }

        if (shardCount == 0) {
            afterFinish();
            return;
        }

        runningSearches.forEach(SearchShardAction::run);
        this.timeoutHandler = threadPool.schedule(this::onTimeout, remainingTime, ThreadPool.Names.GENERIC);
    }

    private void decrementPendingShards() {
        if (pendingShardSearches.decrementAndGet() == 0) {
            afterFinish();
        }
    }

    private void onTimeout() {
        if (done.compareAndSet(false, true)) {
            cancelActions(runningSearches, new RuntimeException("timeout"));
            onPhaseDone();
        }
    }

    private void afterFinish() {
        if (done.compareAndSet(false, true)) {
            try {
                Scheduler.ScheduledCancellable timeoutHandler = this.timeoutHandler;
                if (timeoutHandler != null) {
                    timeoutHandler.cancel();
                }
                onPhaseDone();
            } catch (Exception e) {
                logger.warn("Error while calling onPhaseDone", e);
            }
        }
    }

    protected abstract void executePhaseOnShard(SearchShardTarget shard,
                                                ActionListener<Result> listener);

    protected abstract boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget);

    protected abstract boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e);

    protected abstract void onPhaseDone();

    private void cancelActions(List<SearchShardAction> toCancel, Exception e) {
        toCancel.forEach(action -> action.cancel(e));
    }

    protected boolean shouldRetryOnAnotherCopy(int shardIndex, ShardId shardId, Exception e) {
        return isShardIdStillValid(shardId) && shouldTryOnDifferentShardCopy(shardIndex, shardId, e);
    }

    private boolean isShardIdStillValid(ShardId shardId) {
        return shardInformationValidator.isShardIdStillValid(shardId);
    }

    private boolean isShardCopyStillRelevant(SearchShardTarget searchShardTarget) {
        return shardInformationValidator.isShardRoutingStillValid(searchShardTarget.getShardRouting());
    }

    private class SearchShardAction implements ActionListener<Result> {
        private final int shardIndex;
        private final ShardId shardId;
        private final ActionListener<Result> listener;
        private final AtomicBoolean done = new AtomicBoolean(false);
        private final AtomicReference<NodeSearchExecution> nodeSearch = new AtomicReference<>(null);
        private volatile RetryableSearchShardIterator searchShardIterator;

        private SearchShardAction(int shardIndex,
                                  RetryableSearchShardIterator searchShardIterator,
                                  ActionListener<Result> listener) {
            this.shardIndex = shardIndex;
            this.shardId = searchShardIterator.shardId();
            this.searchShardIterator = searchShardIterator;
            this.listener = listener;
        }

        private void run() {
            if (done.get()) {
                return;
            }

            if (timeoutChecker.hasTimedOut()) {
                onFinalFailure(new RuntimeException("timeout"));
                return;
            }

            SearchShardTarget searchShardTarget = getNextSearchShardTarget();
            if (searchShardTarget == null) {
                refreshSearchTargets();
                return;
            }

            NodeSearchExecution nodeSearchExecution =
                new NodeSearchExecution(shardIndex, searchShardTarget, threadPool, timeoutChecker, this);
            if (nodeSearch.compareAndSet(null, nodeSearchExecution) == false) {
                throw new IllegalStateException("A search against a node for this shard is already running");
            }

            nodeSearch.get().run();
        }

        @Nullable
        private SearchShardTarget getNextSearchShardTarget() {
            while (true) {
                SearchShardTarget searchShardTarget = searchShardIterator.nextOrNull();
                if (searchShardTarget == null || isShardCopyStillRelevant(searchShardTarget)) {
                    return searchShardTarget;
                }
            }
        }

        @Override
        public void onResponse(Result result) {
            if (done.compareAndSet(false, true)) {
                listener.onResponse(result);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (done.get() == false && timeoutChecker.hasTimedOut() == false && shouldRetryOnAnotherCopy(shardIndex, shardId, e)) {
                nodeSearch.set(null);
                threadPool.executor(ThreadPool.Names.GENERIC).submit(this::run);
            } else {
                onFinalFailure(e);
            }
        }

        private void onFinalFailure(Exception e) {
            // TODO accumulate errors
            if (done.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }

        private void refreshSearchTargets() {
            TimeValue timeout = timeoutChecker.remainingTimeUntilTimeout();
            shardIteratorRefresher.refreshSearchTargets(searchShardIterator, timeout, new ActionListener<>() {
                @Override
                public void onResponse(RetryableSearchShardIterator searchShardIterator) {
                    setSearchShardIterator(searchShardIterator);
                    run();
                }

                @Override
                public void onFailure(Exception refreshFailure) {
                    onFinalFailure(refreshFailure);
                }
            });
        }

        private void setSearchShardIterator(RetryableSearchShardIterator searchShardIterator) {
            if (done.get() == false) {
                this.searchShardIterator = searchShardIterator;
            }
        }

        void cancel(Exception reason) {
            if (done.compareAndSet(false, true)) {
                NodeSearchExecution runningAction = nodeSearch.get();
                if (runningAction != null) {
                    runningAction.cancel(reason);
                }
                listener.onFailure(reason);
            }
        }
    }

    private class NodeSearchExecution extends RetryableAction<Result> {
        private final int shardIndex;
        private final SearchShardTarget searchShardTarget;

        private NodeSearchExecution(int shardIndex,
                                   SearchShardTarget searchShardTarget,
                                   ThreadPool threadPool,
                                   TimeoutChecker timeoutChecker,
                                   ActionListener<Result> listener) {
            super(logger, threadPool, INITIAL_RETRY_DELAY, timeoutChecker.remainingTimeUntilTimeout(), listener, ThreadPool.Names.SAME);
            this.shardIndex = shardIndex;
            this.searchShardTarget = searchShardTarget;
        }

        @Override
        public void tryAction(ActionListener<Result> listener) {
            try {
                executePhaseOnShard(searchShardTarget, new SearchActionListener<>(searchShardTarget, shardIndex) {
                    @Override
                    protected void innerOnResponse(Result response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return isShardCopyStillRelevant(searchShardTarget) && shouldRetryOnSameShardCopy(e, searchShardTarget);
        }
    }
}
