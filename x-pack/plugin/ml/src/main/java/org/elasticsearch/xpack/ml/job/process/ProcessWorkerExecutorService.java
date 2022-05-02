/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/*
 * Native ML processes can only handle a single operation at a time. In order to guarantee that, all
 * operations are initially added to a queue and a worker thread from an ML threadpool will process each
 * operation at a time.
 */
public class ProcessWorkerExecutorService extends AbstractExecutorService {

    private static final Logger logger = LogManager.getLogger(ProcessWorkerExecutorService.class);

    private final ThreadContext contextHolder;
    private final String processName;
    private final CountDownLatch awaitTermination = new CountDownLatch(1);
    private final BlockingQueue<Runnable> queue;
    private final AtomicReference<Exception> error = new AtomicReference<>();

    private volatile boolean running = true;

    /**
     * @param contextHolder the thread context holder
     * @param processName the name of the process to be used in logging
     * @param queueCapacity the capacity of the queue holding operations. If an operation is added
     *                  for execution when the queue is full a 429 error is thrown.
     */
    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    public ProcessWorkerExecutorService(ThreadContext contextHolder, String processName, int queueCapacity) {
        this.contextHolder = Objects.requireNonNull(contextHolder);
        this.processName = Objects.requireNonNull(processName);
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
    }

    public int queueSize() {
        return queue.size();
    }

    public void shutdownWithError(Exception e) {
        error.set(e);
        shutdown();
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
            EsRejectedExecutionException rejected = new EsRejectedExecutionException(processName + " worker service has shutdown", true);
            if (command instanceof AbstractRunnable runnable) {
                runnable.onRejection(rejected);
                return;
            } else {
                throw rejected;
            }
        }

        boolean added = queue.offer(contextHolder.preserveContext(command));
        if (added == false) {
            throw new EsRejectedExecutionException(processName + " queue is full. Unable to execute command", false);
        }
    }

    public void start() {
        try {
            while (running) {
                Runnable runnable = queue.poll(500, TimeUnit.MILLISECONDS);
                if (runnable != null) {
                    try {
                        runnable.run();
                    } catch (Exception e) {
                        logger.error(() -> new ParameterizedMessage("error handling process [{}] operation", processName), e);
                    }
                    EsExecutors.rethrowErrors(ThreadContext.unwrap(runnable));
                }
            }

            synchronized (this) {
                // if shutdown with tasks pending notify the handlers
                if (queue.isEmpty() == false) {
                    List<Runnable> notExecuted = new ArrayList<>();
                    queue.drainTo(notExecuted);

                    String msg = "unable to process as " + processName + " worker service has shutdown";
                    Exception ex = error.get();
                    for (Runnable runnable : notExecuted) {
                        if (runnable instanceof AbstractRunnable ar) {
                            if (ex != null) {
                                ar.onFailure(ex);
                            } else {
                                ar.onRejection(new EsRejectedExecutionException(msg, true));
                            }
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
