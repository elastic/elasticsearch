/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.logging;

import org.elasticsearch.common.logging.activity.ActivityCaptureContextBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;

public class EqlCaptureContextBuilder extends ActivityCaptureContextBuilder<EqlCaptureContext, EqlSearchRequest, EqlSearchResponse> {

    public EqlCaptureContextBuilder(Task task, EqlSearchRequest request) {
        super(task, request);
    }

    @Override
    public EqlCaptureContext build(EqlSearchResponse response) {
        return new EqlCaptureContext(task, request, response);
    }

    @Override
    public EqlCaptureContext build(Exception e) {
        return new EqlCaptureContext(task, request, elapsed(), e);
    }
}
