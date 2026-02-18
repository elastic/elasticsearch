/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.logging;

import org.elasticsearch.common.logging.activity.ActivityLoggerContextBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

public class EqlLogContextBuilder extends ActivityLoggerContextBuilder<EqlLogContext, EqlSearchRequest, EqlSearchResponse> {

    public EqlLogContextBuilder(Task task, EqlSearchRequest request) {
        super(task, request);
    }

    @Override
    public EqlLogContext build(EqlSearchResponse response) {
        return new EqlLogContext(task, request, response);
    }

    @Override
    public EqlLogContext build(Exception e) {
        return new EqlLogContext(task, request, elapsed(), e);
    }
}
