/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionType;

public final class QuerySamplingActions {

    public static final ActionType<QuerySamplingStatsResponse> STATS = new ActionType<>("cluster:monitor/xpack/query_sampling/stats");

    public static final ActionType<QuerySamplingRecallResponse> RECALL = new ActionType<>("cluster:monitor/xpack/query_sampling/recall");

    /** Admin action: runs exact searches (full index scans) with the privileges of the caller. */
    public static final ActionType<QuerySamplingGroundTruthResponse> GROUND_TRUTH = new ActionType<>(
        "cluster:admin/xpack/query_sampling/ground_truth"
    );

    /** Admin action: runs exact searches over user data with the privileges of the caller and writes to a permanent index. */
    public static final ActionType<QuerySamplingGoldenUpdateResponse> GOLDEN_UPDATE = new ActionType<>(
        "cluster:admin/xpack/query_sampling/golden/update"
    );

    /** Admin action: runs searches over user data with the privileges of the caller. */
    public static final ActionType<QuerySamplingGoldenCheckResponse> GOLDEN_CHECK = new ActionType<>(
        "cluster:admin/xpack/query_sampling/golden/check"
    );

    private QuerySamplingActions() {}
}
