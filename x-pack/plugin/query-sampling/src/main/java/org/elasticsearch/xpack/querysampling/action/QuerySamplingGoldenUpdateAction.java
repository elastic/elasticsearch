/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionType;

/**
 * Brings the golden dataset up to date by a version: stored sampled queries that are not in it are added and the ground truth of
 * the queries whose data changed is computed again. It is an admin action: it runs exact searches over the data of the users, with
 * the privileges of whoever calls it, and writes to an index that is kept for good.
 */
public final class QuerySamplingGoldenUpdateAction extends ActionType<QuerySamplingGoldenUpdateResponse> {

    public static final QuerySamplingGoldenUpdateAction INSTANCE = new QuerySamplingGoldenUpdateAction();

    public static final String NAME = "cluster:admin/xpack/query_sampling/golden/update";

    private QuerySamplingGoldenUpdateAction() {
        super(NAME);
    }
}
