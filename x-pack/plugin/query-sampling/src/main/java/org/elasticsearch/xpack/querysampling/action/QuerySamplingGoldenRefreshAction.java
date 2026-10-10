/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionType;

/**
 * Makes a new version of the golden dataset in which the ground truth of the queries whose data changed is computed again.
 * It is an admin action: it runs exact searches over the data of the users, with the privileges of whoever calls it, and
 * writes to an index that is kept for good.
 */
public final class QuerySamplingGoldenRefreshAction extends ActionType<QuerySamplingGoldenRefreshResponse> {

    public static final QuerySamplingGoldenRefreshAction INSTANCE = new QuerySamplingGoldenRefreshAction();

    public static final String NAME = "cluster:admin/xpack/query_sampling/golden/refresh";

    private QuerySamplingGoldenRefreshAction() {
        super(NAME);
    }
}
