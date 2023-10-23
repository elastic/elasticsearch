/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.profiling;

import org.elasticsearch.action.ActionType;

public final class GetFlamegraphAction extends ActionType<GetFlamegraphResponse> {
    public static final GetFlamegraphAction INSTANCE = new GetFlamegraphAction();
    public static final String NAME = "indices:data/read/profiling/flamegraph";

    private GetFlamegraphAction() {
        super(NAME, GetFlamegraphResponse::new);
    }
}
