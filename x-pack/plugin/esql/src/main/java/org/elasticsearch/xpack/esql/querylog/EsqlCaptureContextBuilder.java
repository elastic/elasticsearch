/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querylog;

import org.elasticsearch.common.logging.activity.QueryCaptureContextBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

public class EsqlCaptureContextBuilder extends QueryCaptureContextBuilder<EsqlCaptureContext, EsqlQueryRequest, EsqlQueryResponse> {
    public EsqlCaptureContextBuilder(Task task, EsqlQueryRequest request) {
        super(task, request);
    }

    @Override
    public EsqlCaptureContext build(EsqlQueryResponse response) {
        if (response.getExecutionInfo() == null) {
            throw new IllegalStateException("Execution info is missing for EsqlCaptureContext");
        }
        return new EsqlCaptureContext(task, request, response);
    }

    @Override
    public EsqlCaptureContext build(Exception e) {
        return new EsqlCaptureContext(task, request, elapsed(), e);
    }
}
