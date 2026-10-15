/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionType;

/**
 * Admin action: runs exact searches over user data with the privileges of the caller and writes to a permanent index.
 */
public final class QuerySamplingGoldenUpdateAction extends ActionType<QuerySamplingGoldenUpdateResponse> {

    public static final QuerySamplingGoldenUpdateAction INSTANCE = new QuerySamplingGoldenUpdateAction();

    public static final String NAME = "cluster:admin/xpack/query_sampling/golden/update";

    private QuerySamplingGoldenUpdateAction() {
        super(NAME);
    }
}
