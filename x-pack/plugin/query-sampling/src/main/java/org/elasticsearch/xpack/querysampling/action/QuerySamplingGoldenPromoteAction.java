/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionType;

/**
 * Promotes stored sampled queries that have their ground truth to a new version of the golden dataset. It is an admin
 * action: it writes to an index that is kept for good.
 */
public final class QuerySamplingGoldenPromoteAction extends ActionType<QuerySamplingGoldenPromoteResponse> {

    public static final QuerySamplingGoldenPromoteAction INSTANCE = new QuerySamplingGoldenPromoteAction();

    public static final String NAME = "cluster:admin/xpack/query_sampling/golden/promote";

    private QuerySamplingGoldenPromoteAction() {
        super(NAME);
    }
}
