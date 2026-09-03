/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.exchange.ExchangeSourceHandler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.sameInstance;

/**
 * Setup-failure accounting for {@link MergeLevelExecutor}: the constructor pre-acquires one {@link ComputeListener}
 * reference per branch, and a throw after that must still discharge the unused ones or the level never completes.
 */
public class MergeLevelExecutorTests extends ESTestCase {

    /**
     * Closing the {@link ComputeListener} is not enough after the executor has been constructed: the pre-acquired
     * branch references keep it open. {@link MergeLevelExecutor#abortUndispatched} fails those unused references so
     * the completion listener fires — without it the nested exchange source handler would stay registered.
     */
    public void testAbortUndispatchedReleasesPreAcquiredListeners() throws Exception {
        PlainActionFuture<DriverCompletionInfo> done = new PlainActionFuture<>();
        ExchangeSourceHandler source = new ExchangeSourceHandler(10, Runnable::run);
        SubPlanTaskRunner runner = new SubPlanTaskRunner(2, Runnable::run);
        var context = new MergeLevelExecutor.QueryContext("s", null, null, null, null, null, null, Map.of(), runner);

        int branches = randomIntBetween(1, 5);
        Exception failure = new IllegalStateException("setup failed");
        ComputeListener computeListener = new ComputeListener(() -> {}, done);
        MergeLevelExecutor executor = new MergeLevelExecutor(
            null,
            context,
            Collections.nCopies(branches, (PhysicalPlan) null),
            computeListener,
            source,
            null
        );
        // Matches ComputeService / executeSubPlanWithNestedSubPlans: the try-with-resources closes the construction
        // ref, then the catch must still discharge the branch refs the constructor acquired.
        computeListener.close();
        assertFalse("pre-acquired branch refs must keep the ComputeListener open after close()", done.isDone());

        executor.abortUndispatched(failure);

        assertTrue("ComputeListener must reach zero after unused branches are aborted", done.isDone());
        Exception completed = expectThrows(Exception.class, () -> done.actionGet(10, TimeUnit.SECONDS));
        assertThat(completed, sameInstance(failure));
        assertThat(runner.failure(), sameInstance(failure));
    }
}
