/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * Mock single threaded {@link PrioritizedEsThreadPoolExecutor} based on {@link DeterministicTaskQueue},
 * simulating the behaviour of an executor returned by {@link EsExecutors#newSinglePrioritizing}.
 */
public class MockSinglePrioritizingExecutor extends PrioritizedEsThreadPoolExecutor {

    public MockSinglePrioritizingExecutor(String name, DeterministicTaskQueue deterministicTaskQueue, ThreadPool threadPool) {
        super(
            name,
            0,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            r -> new Thread() {
                @Override
                public void start() {
                    deterministicTaskQueue.scheduleNow(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                r.run();
                            } catch (KillWorkerError kwe) {
                                // hacks everywhere
                            }
                        }

                        @Override
                        public String toString() {
                            return r.toString();
                        }
                    });
                }
            },
            threadPool.getThreadContext(),
            threadPool.scheduler(),
            StarvationWatcher.NOOP_STARVATION_WATCHER);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        // kill worker so that next one will be scheduled, using cached Error instance to not incur the cost of filling in the stack trace
        // on every task
        throw KillWorkerError.INSTANCE;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        // ensures we don't block
        return false;
    }

    private static final class KillWorkerError extends Error {
        private static final KillWorkerError INSTANCE = new KillWorkerError();
    }
}
