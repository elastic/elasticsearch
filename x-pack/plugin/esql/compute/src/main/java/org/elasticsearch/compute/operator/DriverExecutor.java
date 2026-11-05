/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;

import java.util.concurrent.Executor;

/**
 * Runs the tasks of {@link Driver}s, telling apart a driver resuming after it was blocked from new work.
 */
public interface DriverExecutor extends Executor {
    /**
     * Runs {@code task} ahead of normal work, for work that has already waited its turn, such as a driver resuming
     * after its exchange was blocked. If the pool is full the task is rejected via {@link AbstractRunnable#onRejection}.
     */
    void executeResumed(AbstractRunnable task);
}
