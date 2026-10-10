/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionType;

public final class QuerySamplingStatsAction extends ActionType<QuerySamplingStatsResponse> {

    public static final QuerySamplingStatsAction INSTANCE = new QuerySamplingStatsAction();

    public static final String NAME = "cluster:monitor/xpack/query_sampling/stats";

    private QuerySamplingStatsAction() {
        super(NAME);
    }
}
