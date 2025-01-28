/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.action.ActionType;

public final class GetStackTracesAction extends ActionType<GetStackTracesResponse> {
    public static final GetStackTracesAction INSTANCE = new GetStackTracesAction();
    public static final String NAME = "indices:data/read/profiling/stack_traces";

    private GetStackTracesAction() {
        super(NAME);
    }
}
