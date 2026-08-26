/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.action.ActionType;

public class GpuStatsAction extends ActionType<GpuStatsResponse> {

    public static final GpuStatsAction INSTANCE = new GpuStatsAction();
    public static final String NAME = "cluster:monitor/xpack/gpu_vector_indexing/stats/dist";

    private GpuStatsAction() {
        super(NAME);
    }
}
