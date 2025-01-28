/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http;

import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>Runs a command synchronously on at most one thread.  Threads make a request to run the command.  If no thread is running the command,
 * then the command will start on the provided {@link #threadPool}'s {@link #executorServiceName}.  If a thread is currently running the
 * command, then that thread is notified to rerun the command after it is finished.</p>
 *
 * <p>This guarantees only one thread is working on a command at a given point in time.</p>
 */
class RequestBasedTaskRunner {
    private final Runnable command;
    private final ThreadPool threadPool;
    private final String executorServiceName;
    private final AtomicInteger loopCount = new AtomicInteger(0);
    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    RequestBasedTaskRunner(Runnable command, ThreadPool threadPool, String executorServiceName) {
        this.command = Objects.requireNonNull(command);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.executorServiceName = Objects.requireNonNull(executorServiceName);
    }

    /**
     * If there is currently a thread running in a loop, it should pick up this new request.
     * If not, check if this thread is one of ours and reuse it.
     * Else, offload to a new thread so we do not block another threadpool's thread.
     */
    public void requestNextRun() {
        if (isRunning.get() && loopCount.getAndIncrement() == 0) {
            var currentThreadPool = EsExecutors.executorName(Thread.currentThread().getName());
            if (executorServiceName.equalsIgnoreCase(currentThreadPool)) {
                run();
            } else {
                threadPool.executor(executorServiceName).execute(this::run);
            }
        }
    }

    public void cancel() {
        isRunning.set(false);
    }

    private void run() {
        do {
            command.run();
        } while (isRunning.get() && loopCount.decrementAndGet() > 0);
    }
}
