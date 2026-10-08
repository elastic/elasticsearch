/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionType;

/**
 * Estimates the recall of the search from the stored sampled queries whose ground truth is known.
 */
public final class QuerySamplingRecallAction extends ActionType<QuerySamplingRecallResponse> {

    public static final QuerySamplingRecallAction INSTANCE = new QuerySamplingRecallAction();

    public static final String NAME = "cluster:monitor/xpack/query_sampling/recall";

    private QuerySamplingRecallAction() {
        super(NAME);
    }
}
