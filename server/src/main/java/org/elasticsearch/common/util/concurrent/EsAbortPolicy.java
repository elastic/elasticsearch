/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.ThreadPoolExecutor;

public class EsAbortPolicy extends EsRejectedExecutionHandler {

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        if (executor.isShutdown() == false && isForceExecution(r)) {
            if (executor.getQueue() instanceof SizeBlockingQueue<Runnable> sizeBlockingQueue) {
                try {
                    sizeBlockingQueue.forcePut(r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("forced execution, but got interrupted", e);
                }
                if ((executor.isShutdown() && sizeBlockingQueue.remove(r)) == false) {
                    return;
                } // else fall through and reject the task since the executor is shut down
            } else {
                throw new IllegalStateException("expected but did not find SizeBlockingQueue: " + executor);
            }
        }
        incrementRejections();
        throw newRejectedException(r, executor, executor.isShutdown());
    }

    private static boolean isForceExecution(Runnable r) {
        return r instanceof AbstractRunnable abstractRunnable && abstractRunnable.isForceExecution();
    }
}
