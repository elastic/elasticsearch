/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.metrics.CounterMetric;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public class EsAbortPolicy implements XRejectedExecutionHandler {
    private final CounterMetric rejected = new CounterMetric();

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (r instanceof AbstractRunnable) {
            if (((AbstractRunnable) r).isForceExecution()) {
                BlockingQueue<Runnable> queue = executor.getQueue();
                if ((queue instanceof SizeBlockingQueue) == false) {
                    throw new IllegalStateException("forced execution, but expected a size queue");
                }
                try {
                    ((SizeBlockingQueue<Runnable>) queue).forcePut(r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("forced execution, but got interrupted", e);
                }
                return;
            }
        }
        rejected.inc();
        throw new EsRejectedExecutionException("rejected execution of " + r + " on " + executor, executor.isShutdown());
    }

    @Override
    public long rejected() {
        return rejected.count();
    }
}
