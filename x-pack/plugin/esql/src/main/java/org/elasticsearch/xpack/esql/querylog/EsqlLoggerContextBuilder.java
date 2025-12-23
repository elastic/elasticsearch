/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.common.logging.action.ActionLoggerContextBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

public class EsqlLoggerContextBuilder implements ActionLoggerContextBuilder<EsqlLoggerContext, EsqlQueryResponse> {

    private final EsqlQueryRequest request;
    private final Task task;

    public EsqlLoggerContextBuilder(Task task, EsqlQueryRequest request) {
        this.request = request;
        this.task = task;
    }

    @Override
    public EsqlLoggerContext build(EsqlQueryResponse response) {
        return new EsqlLoggerContext(task, request, response);
    }

    @Override
    public EsqlLoggerContext build(Exception e) {
        return new EsqlLoggerContext(task, request, e);
    }
}
