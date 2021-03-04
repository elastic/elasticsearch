/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.action.ActionType;

public class ClusterStatsAction extends ActionType<ClusterStatsResponse> {

    public static final ClusterStatsAction INSTANCE = new ClusterStatsAction();
    public static final String NAME = "cluster:monitor/stats";

    private ClusterStatsAction() {
        super(NAME, ClusterStatsResponse::new);
    }
}
