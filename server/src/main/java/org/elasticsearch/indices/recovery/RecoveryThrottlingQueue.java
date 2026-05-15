/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.threadpool.ThreadPool;

public class RecoveryThrottlingQueue {

    private final ThreadPool threadPool;
    private final int maxConcurrentRecoveries;

    public RecoveryThrottlingQueue(ThreadPool threadPool, int maxConcurrentRecoveries) {
        this.threadPool = threadPool;
        this.maxConcurrentRecoveries = maxConcurrentRecoveries;
    }

    public void enqueue(RecoveryTask recoveryTask) {
        threadPool.generic().execute(recoveryTask);
    }

    @FunctionalInterface
    public interface RecoveryTask extends Runnable {}
}
