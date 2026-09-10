/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.logging.activity.ActivityLoggerContextBuilder;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.session.Result;

import java.util.function.Supplier;

/**
 * {@link ActivityLoggerContextBuilder} for streaming ES|QL queries ({@code POST /_query/stream}).
 * Because the response type is {@link ActionResponse.Empty} and carries no query data, this builder
 * holds the collaborators it needs: {@link EsqlExecutionInfo}, {@link PageStreamPublisher}, and a
 * {@link Supplier} of the final {@link Result} populated once the query completes.
 */
public class EsqlStreamLogContextBuilder extends ActivityLoggerContextBuilder<EsqlLogContext, EsqlQueryRequest, ActionResponse.Empty> {

    private final EsqlExecutionInfo executionInfo;
    private final PageStreamPublisher publisher;
    private final Supplier<Result> resultSupplier;

    public EsqlStreamLogContextBuilder(
        Task task,
        EsqlQueryRequest request,
        EsqlExecutionInfo executionInfo,
        PageStreamPublisher publisher,
        Supplier<Result> resultSupplier
    ) {
        super(task, request);
        this.executionInfo = executionInfo;
        this.publisher = publisher;
        this.resultSupplier = resultSupplier;
    }

    @Override
    public EsqlLogContext build(ActionResponse.Empty empty) {
        long tookInNanos = executionInfo.overallTook() != null ? executionInfo.overallTook().nanos() : elapsed();
        Result result = resultSupplier.get();
        if (result == null) {
            return new EsqlLogContext(task, request, tookInNanos);
        }
        return new EsqlStreamLogContext(task, request, tookInNanos, executionInfo, publisher, result);
    }

    @Override
    public EsqlLogContext build(Exception e) {
        return new EsqlLogContext(task, request, elapsed(), e);
    }
}
