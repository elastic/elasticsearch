/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;

/**
 * Async STOP that arrives before compute starts must complete the plan empty and mark the result partial.
 */
public class ComputeServiceStopTests extends ESTestCase {

    public void testCompleteIfStoppedNoopsWhenNotStopped() {
        EsqlExecutionInfo info = new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        PlainActionFuture<Result> future = new PlainActionFuture<>();
        assertFalse(ComputeService.completeIfStopped(info, plan(), configuration(new QueryPragmas(Settings.EMPTY)), null, future));
        assertFalse(future.isDone());
        assertFalse(info.isPartial());
    }

    public void testCompleteIfStoppedFinishesEmptyAndMarksPartial() {
        EsqlExecutionInfo info = new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        info.markAsStopped();
        PlainActionFuture<Result> future = new PlainActionFuture<>();
        PhysicalPlan plan = plan();
        assertTrue(ComputeService.completeIfStopped(info, plan, configuration(new QueryPragmas(Settings.EMPTY)), null, future));
        Result result = future.actionGet();
        assertTrue(info.isPartial());
        assertTrue(result.pages().isEmpty());
        assertEquals(plan.output(), result.schema());
    }

    public void testCompleteIfStoppedNoopsOnceComputeHasStarted() {
        EsqlExecutionInfo info = new EsqlExecutionInfo(s -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        info.markAsStopped();
        info.markComputeStarted();
        PlainActionFuture<Result> future = new PlainActionFuture<>();
        assertFalse(ComputeService.completeIfStopped(info, plan(), configuration(new QueryPragmas(Settings.EMPTY)), null, future));
        assertFalse(future.isDone());
        assertFalse(info.isPartial());
    }

    private static PhysicalPlan plan() {
        return new LocalSourceExec(Source.EMPTY, List.of(), EmptyLocalSupplier.EMPTY);
    }
}
