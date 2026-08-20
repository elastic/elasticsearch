/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.compute.operator.DriverCompletionInfo;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;

public class EsqlStreamLogContextBuilderTests extends ESTestCase {

    public void testBuildWithNullOverallTookUsesElapsed() {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(clusterAlias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        // overallTook() returns null because markEndQuery was never called.
        assertNull("precondition: overallTook must be null", executionInfo.overallTook());

        PageStreamPublisher publisher = new PageStreamPublisher(10);
        Result result = makeResult(executionInfo);

        EsqlStreamLogContextBuilder builder = new EsqlStreamLogContextBuilder(
            task(),
            syncEsqlQueryRequest("FROM test | LIMIT 1"),
            executionInfo,
            publisher,
            () -> result
        );

        EsqlLogContext context = builder.build(ActionResponse.Empty.INSTANCE);

        assertThat("context must be streaming-specific on success path", context, instanceOf(EsqlStreamLogContext.class));
        assertThat(context.getTookInNanos(), greaterThanOrEqualTo(0L));
        assertThat(context.getResultCount(), equalTo(0)); // publisher has no rows
    }

    public void testBuildWithNullResultDegradesGracefully() {
        EsqlExecutionInfo executionInfo = makeExecutionInfoWithTook();

        PageStreamPublisher publisher = new PageStreamPublisher(10);

        EsqlStreamLogContextBuilder builder = new EsqlStreamLogContextBuilder(
            task(),
            syncEsqlQueryRequest("FROM test | LIMIT 1"),
            executionInfo,
            publisher,
            () -> null
        );

        EsqlLogContext context = builder.build(ActionResponse.Empty.INSTANCE);

        assertFalse("a null Result must produce a plain EsqlLogContext, not a streaming subclass", context instanceof EsqlStreamLogContext);
        assertThat(context.getResultCount(), equalTo(0));
        assertNull(context.getIndices());
    }

    public void testBuildWithExceptionProducesFailureContext() {
        EsqlQueryRequest request = syncEsqlQueryRequest("FROM test | LIMIT 1");
        PageStreamPublisher publisher = new PageStreamPublisher(10);

        EsqlStreamLogContextBuilder builder = new EsqlStreamLogContextBuilder(
            task(),
            request,
            makeExecutionInfoWithTook(),
            publisher,
            () -> null
        );
        RuntimeException ex = new RuntimeException("query failed");

        EsqlLogContext context = builder.build(ex);

        assertFalse(context instanceof EsqlStreamLogContext);
        assertFalse("failure context must report isSuccess() == false", context.isSuccess());
        assertThat(context.getTookInNanos(), greaterThanOrEqualTo(0L));
    }

    public void testBuildSuccessProducesStreamingContext() {
        EsqlExecutionInfo executionInfo = makeExecutionInfoWithTook();
        PageStreamPublisher publisher = new PageStreamPublisher(10);
        Result result = makeResult(executionInfo);

        EsqlStreamLogContextBuilder builder = new EsqlStreamLogContextBuilder(
            task(),
            syncEsqlQueryRequest("FROM test | LIMIT 1"),
            executionInfo,
            publisher,
            () -> result
        );
        EsqlLogContext context = builder.build(ActionResponse.Empty.INSTANCE);

        assertThat("successful build with a Result must yield a streaming context", context, instanceOf(EsqlStreamLogContext.class));
        assertThat(context.getTookInNanos(), greaterThan(0L));
        assertThat(context.getResultCount(), equalTo(0)); // no rows published
    }

    private static Task task() {
        return new Task(1, "transport", EsqlQueryAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static EsqlExecutionInfo makeExecutionInfoWithTook() {
        EsqlExecutionInfo info = new EsqlExecutionInfo(clusterAlias -> false, EsqlExecutionInfo.IncludeExecutionMetadata.NEVER);
        info.markEndQuery();
        return info;
    }

    private static Result makeResult(EsqlExecutionInfo executionInfo) {
        return new Result(List.of(), List.of(), Map.of(), EsqlTestUtils.TEST_CFG, DriverCompletionInfo.EMPTY, executionInfo);
    }
}
