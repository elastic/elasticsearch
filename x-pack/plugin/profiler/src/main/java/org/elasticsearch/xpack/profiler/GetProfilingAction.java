/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.profiler;

import org.elasticsearch.action.ActionType;

public final class GetProfilingAction extends ActionType<GetProfilingResponse> {
    public static final GetProfilingAction INSTANCE = new GetProfilingAction();
    public static final String NAME = "indices:data/read/profiling";

    private GetProfilingAction() {
        super(NAME, GetProfilingResponse::new);
    }
}
