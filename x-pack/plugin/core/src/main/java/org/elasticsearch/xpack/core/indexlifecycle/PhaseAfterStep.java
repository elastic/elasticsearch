/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.StepResult;

import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

public class PhaseAfterStep extends Step {
    private final CompletableFuture<StepResult> timeUp;
    private final ThreadPool threadPool;
    private final long indexCreationDate;
    private final LongSupplier nowSupplier;
    private final TimeValue after;

    public PhaseAfterStep(ThreadPool threadPool, long indexCreationDate, LongSupplier nowSupplier, TimeValue after, String name,
                          String index, String phase, String action) {
        super(name, action, phase, index);
        this.threadPool = threadPool;
        this.indexCreationDate = indexCreationDate;
        this.nowSupplier = nowSupplier;
        this.timeUp = new CompletableFuture<>();
        this.after = after;
    }

    public StepResult execute(ClusterState currentState) {
        LongSupplier elapsed = () -> nowSupplier.getAsLong() - indexCreationDate;
        BooleanSupplier isReady = () -> after.getSeconds() <= elapsed.getAsLong();
        if (isReady.getAsBoolean()) {
            return new StepResult("phase starting", null, currentState, true, true);
        } else {
            threadPool.schedule(TimeValue.timeValueSeconds(elapsed.getAsLong()), ThreadPool.Names.GENERIC, () -> {
                if (after.getSeconds() <= elapsed.getAsLong()) {
                    // IndexLifecycleService.triggerPolicies()
                }
            });
            return new StepResult("phase-check-rescheduled", null, currentState, true, false);
        }
    }
}
