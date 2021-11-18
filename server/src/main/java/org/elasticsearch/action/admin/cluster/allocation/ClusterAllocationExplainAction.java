/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionType;

/**
 * ActionType for explaining shard allocation for a shard in the cluster
 */
public class ClusterAllocationExplainAction extends ActionType<ClusterAllocationExplainResponse> {

    public static final ClusterAllocationExplainAction INSTANCE = new ClusterAllocationExplainAction();
    public static final String NAME = "cluster:monitor/allocation/explain";

    private ClusterAllocationExplainAction() {
        super(NAME, ClusterAllocationExplainResponse::new);
    }
}
