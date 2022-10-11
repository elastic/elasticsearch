/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.SuppressForbidden;

import java.util.concurrent.LinkedBlockingQueue;

/*
 * Native ML processes can only handle a single operation at a time. In order to guarantee that, all
 * operations are initially added to a queue and a worker thread from an ML threadpool will process each
 * operation at a time.
 */
public class ProcessWorkerExecutorService extends AbstractProcessWorkerExecutorService<Runnable> {

    /**
     * @param contextHolder the thread context holder
     * @param processName the name of the process to be used in logging
     * @param queueCapacity the capacity of the queue holding operations. If an operation is added
     *                  for execution when the queue is full a 429 error is thrown.
     */
    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    public ProcessWorkerExecutorService(ThreadContext contextHolder, String processName, int queueCapacity) {
        super(contextHolder, processName, queueCapacity, LinkedBlockingQueue::new);
    }

    @Override
    public synchronized void execute(Runnable command) {
        if (command instanceof AbstractInitializableRunnable initializableRunnable) {
            initializableRunnable.init();
        }
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
}
