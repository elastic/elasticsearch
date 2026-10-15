/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionType;

/** Admin action: runs exact searches (full index scans) with the privileges of the caller. */
public final class QuerySamplingGroundTruthAction extends ActionType<QuerySamplingGroundTruthResponse> {

    public static final QuerySamplingGroundTruthAction INSTANCE = new QuerySamplingGroundTruthAction();

    public static final String NAME = "cluster:admin/xpack/query_sampling/ground_truth";

    private QuerySamplingGroundTruthAction() {
        super(NAME);
    }
}
