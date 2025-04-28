/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.stats.rrc;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;

public class ShardStatsAction extends ActionType<ShardStatsResponse> {

    public static final ShardStatsAction INSTANCE = new ShardStatsAction();
    public static final String NAME = "shards:monitor/stats";
    public static final RemoteClusterActionType<ShardStatsResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        ShardStatsResponse::new
    );

    private ShardStatsAction() {
        super(NAME);
    }
}
