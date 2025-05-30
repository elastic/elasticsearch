/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.action.search.load;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;

/**
 * Action definition for retrieving shard-level search load statistics.
 * <p>
 * This action serves as a marker for executing {@link TransportShardSearchLoadStatsAction}
 * </p>
 */
public class ShardSearchLoadStatsAction extends ActionType<ShardSearchLoadStatsResponse> {

    /**
     * Singleton instance of the action type.
     */
    public static final ShardSearchLoadStatsAction INSTANCE = new ShardSearchLoadStatsAction();
    public static final String NAME = "internal:monitor/stats";
    public static final RemoteClusterActionType<ShardSearchLoadStatsResponse> REMOTE_TYPE = new RemoteClusterActionType<>(
        NAME,
        ShardSearchLoadStatsResponse::new
    );

    private ShardSearchLoadStatsAction() {
        super(NAME);
    }
}
