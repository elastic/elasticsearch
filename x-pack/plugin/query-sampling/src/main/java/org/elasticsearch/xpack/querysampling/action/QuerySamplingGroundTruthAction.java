/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionType;

/**
 * Computes the ground truth of the sampled queries held by the nodes. It is an admin action: it runs exact
 * searches, which scan the index, with the privileges of whoever calls it.
 */
public final class QuerySamplingGroundTruthAction extends ActionType<QuerySamplingGroundTruthResponse> {

    public static final QuerySamplingGroundTruthAction INSTANCE = new QuerySamplingGroundTruthAction();

    public static final String NAME = "cluster:admin/xpack/query_sampling/ground_truth";

    private QuerySamplingGroundTruthAction() {
        super(NAME);
    }
}
