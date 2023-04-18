/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.tasks.TaskCancelledException;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A response cache for capacity API that ensures that concurrent requests may be served by a single calculation and that only one thread
 * is active at any time calculating the capacity API response. Work is delegated to the MANAGEMENT thread pool.
 * This protects the master from overload due to capacity API requests, which are expensive in nature.
 *
 * The generic arg mainly helps ease testing (and we may want to generalize this in the future)
 */
class CapacityResponseCache<Response> {

    private static final Logger logger = LogManager.getLogger(CapacityResponseCache.class);

    private final Queue<Job> jobQueue = ConcurrentCollections.newQueue();
    private final AtomicInteger jobQueueSize = new AtomicInteger();
    private final Function<Runnable, Response> refresher;
    private final Consumer<Runnable> runOnThread;

    CapacityResponseCache(Consumer<Runnable> runOnThread, Function<Runnable, Response> refresher) {
        this.runOnThread = runOnThread;
        this.refresher = refresher;
    }

    public void get(BooleanSupplier isCancelled, ActionListener<Response> listener) {
        jobQueue.offer(new Job(isCancelled, listener));
        assert jobQueueSize.get() >= 0;
        if (jobQueueSize.getAndIncrement() == 0) {
            try {
                runOnThread.accept(this::singleThreadRefresh);
            } catch (Exception e) {
                // If this ever happens it's likely a server-side problem rather than the
                // user's fault, so we need this logging as a means to get the stack trace
                logger.debug("Error calculating autoscaling response", e);
                do {
                    Job jobToFail = jobQueue.poll();
                    assert jobToFail != null;
                    jobToFail.onFailure(e);
                } while (jobQueueSize.decrementAndGet() > 0);
            }
        }
    }

    private void singleThreadRefresh() {
        assert jobQueueSize.get() > 0 : "poor man's single thread check";
        int jobCount = jobQueueSize.get();
        do {
            try {
                ListenableFuture<Response> future = new ListenableFuture<>();
                List<Job> jobs = new ArrayList<>(jobCount);
                for (int i = 0; i < jobCount; ++i) {
                    Job job = jobQueue.remove();
                    assert job != null : jobQueueSize.get() + " queue size is out of sync";
                    jobs.add(job);
                    future.addListener(job.listener);
                }

                Runnable ensureNotCancelled = () -> {
                    for (Job job : jobs) {
                        if (job.isCancelled() == false) {
                            return;
                        }
                    }
                    throw new TaskCancelledException("task cancelled");
                };

                ActionListener.completeWith(future, () -> refresher.apply(ensureNotCancelled));
            } finally {
                jobCount = jobQueueSize.addAndGet(-jobCount);
            }
        } while (jobCount > 0);
    }

    // for tests
    int jobQueueSize() {
        return jobQueueSize.get();
    }

    // for tests
    int jobQueueCount() {
        return jobQueue.size();
    }

    private class Job {
        private final BooleanSupplier isCancelled;
        private final ActionListener<Response> listener;

        private Job(BooleanSupplier isCancelled, ActionListener<Response> listener) {
            this.isCancelled = isCancelled;
            this.listener = listener;
        }

        public boolean isCancelled() {
            return isCancelled.getAsBoolean();
        }

        public void onFailure(Exception e) {
            try {
                listener.onFailure(e);
            } catch (Exception e2) {
                assert false : e2;
            }
        }
    }
}
