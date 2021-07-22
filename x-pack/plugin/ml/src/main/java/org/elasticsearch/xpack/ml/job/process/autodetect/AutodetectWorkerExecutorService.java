/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/*
 * The autodetect native process can only handle a single operation at a time. In order to guarantee that, all
 * operations are initially added to a queue and a worker thread from ml autodetect threadpool will process each
 * operation at a time.
 */
class AutodetectWorkerExecutorService extends AbstractExecutorService {

    private static final Logger logger = LogManager.getLogger(AutodetectWorkerExecutorService.class);

    private final ThreadContext contextHolder;
    private final CountDownLatch awaitTermination = new CountDownLatch(1);
    private final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(100);

    private volatile boolean running = true;

    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    AutodetectWorkerExecutorService(ThreadContext contextHolder) {
        this.contextHolder = contextHolder;
    }

    @Override
    public void shutdown() {
        running = false;
    }

    @Override
    public List<Runnable> shutdownNow() {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public boolean isShutdown() {
        return running == false;
    }

    @Override
    public boolean isTerminated() {
        return awaitTermination.getCount() == 0;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return awaitTermination.await(timeout, unit);
    }

    @Override
    public synchronized void execute(Runnable command) {
        if (isShutdown()) {
            EsRejectedExecutionException rejected = new EsRejectedExecutionException("autodetect worker service has shutdown", true);
            if (command instanceof AbstractRunnable) {
                ((AbstractRunnable) command).onRejection(rejected);
            } else {
                throw rejected;
            }
        }

        boolean added = queue.offer(contextHolder.preserveContext(command));
        if (added == false) {
            throw new ElasticsearchStatusException("Unable to submit operation", RestStatus.TOO_MANY_REQUESTS);
        }
    }

    void start() {
        try {
            while (running) {
                Runnable runnable = queue.poll(500, TimeUnit.MILLISECONDS);
                if (runnable != null) {
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        logger.error("error handling job operation", e);
                    }
                    EsExecutors.rethrowErrors(contextHolder.unwrap(runnable));
                }
            }

            synchronized (this) {
                // if shutdown with tasks pending notify the handlers
                if (queue.isEmpty() == false) {
                    List<Runnable> notExecuted = new ArrayList<>();
                    queue.drainTo(notExecuted);

                    for (Runnable runnable : notExecuted) {
                        if (runnable instanceof AbstractRunnable) {
                            ((AbstractRunnable) runnable).onRejection(
                                new EsRejectedExecutionException("unable to process as autodetect worker service has shutdown", true));
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            awaitTermination.countDown();
        }
    }
}
