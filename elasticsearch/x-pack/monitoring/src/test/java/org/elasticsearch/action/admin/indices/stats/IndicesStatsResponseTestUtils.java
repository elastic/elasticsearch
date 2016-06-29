/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.ShardOperationFailedException;

import java.util.List;

public class IndicesStatsResponseTestUtils {

    /**
     * Gives access to package private IndicesStatsResponse constructor for test purpose.
     **/
    public static IndicesStatsResponse newIndicesStatsResponse(ShardStats[] shards, int totalShards, int successfulShards,
                                                               int failedShards, List<ShardOperationFailedException> shardFailures) {
        return new IndicesStatsResponse(shards, totalShards, successfulShards, failedShards, shardFailures);
    }
}
