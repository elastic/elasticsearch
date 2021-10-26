/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.shards;

import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ShardCounts {
    private final int shardsPerNode;

    private final int firstIndexShards;
    private final int firstIndexReplicas;

    private final int failingIndexShards;
    private final int failingIndexReplicas;

    private ShardCounts(int shardsPerNode,
                        int firstIndexShards,
                        int firstIndexReplicas,
                        int failingIndexShards,
                        int failingIndexReplicas) {
        this.shardsPerNode = shardsPerNode;
        this.firstIndexShards = firstIndexShards;
        this.firstIndexReplicas = firstIndexReplicas;
        this.failingIndexShards = failingIndexShards;
        this.failingIndexReplicas = failingIndexReplicas;
    }

    public static ShardCounts forDataNodeCount(int dataNodes) {
        Assert.assertThat("this method will not work reliably with this many data nodes due to the limit of shards in a single index," +
            "use fewer data nodes or multiple indices", dataNodes, lessThanOrEqualTo(90));
        int mainIndexReplicas = ESTestCase.between(0, dataNodes - 1);
        int mainIndexShards = ESTestCase.between(1, 10);
        int totalShardsInIndex = (mainIndexReplicas + 1) * mainIndexShards;
        // Sometimes add some headroom to the limit to check that it works even if you're not already right up against the limit
        int shardsPerNode = (int) Math.ceil((double) totalShardsInIndex / dataNodes) + ESTestCase.between(0, 10);
        int totalCap = shardsPerNode * dataNodes;

        int failingIndexShards;
        int failingIndexReplicas;
        if (dataNodes > 1 && ESTestCase.frequently()) {
            failingIndexShards = Math.max(1, totalCap - totalShardsInIndex);
            failingIndexReplicas = ESTestCase.between(1, dataNodes - 1);
        } else {
            failingIndexShards = totalCap - totalShardsInIndex + ESTestCase.between(1, 10);
            failingIndexReplicas = 0;
        }

        return new ShardCounts(shardsPerNode, mainIndexShards, mainIndexReplicas, failingIndexShards, failingIndexReplicas);
    }

    public int getShardsPerNode() {
        return shardsPerNode;
    }

    public int getFirstIndexShards() {
        return firstIndexShards;
    }

    public int getFirstIndexReplicas() {
        return firstIndexReplicas;
    }

    public int getFailingIndexShards() {
        return failingIndexShards;
    }

    public int getFailingIndexReplicas() {
        return failingIndexReplicas;
    }
}
