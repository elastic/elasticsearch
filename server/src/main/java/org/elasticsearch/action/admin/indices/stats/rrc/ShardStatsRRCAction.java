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

public class ShardStatsRRCAction extends ActionType<ShardStatsRRCResponse> {

    public static final ShardStatsRRCAction INSTANCE = new ShardStatsRRCAction();
    public static final String NAME = "indices:monitor/stats/rrc";
    public static final RemoteClusterActionType<ShardStatsRRCResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        ShardStatsRRCResponse::new
    );

    private ShardStatsRRCAction() {
        super(NAME);
    }
}
