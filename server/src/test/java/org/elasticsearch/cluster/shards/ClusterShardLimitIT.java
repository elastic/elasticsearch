/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.elasticsearch.cluster.shards;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ClusterShardLimitIT extends ESIntegTestCase {
    private static final String shardsPerNodeKey = MetaData.SETTING_CLUSTER_MAX_SHARDS_PER_NODE.getKey();

    public void testSettingClusterMaxShards() {
        int shardsPerNode = between(1, 500_000);
        setShardsPerNode(shardsPerNode);
    }

    public void testMinimumPerNode() {
        int negativeShardsPerNode = between(-50_000, 0);
        try {
            if (frequently()) {
                client().admin().cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(shardsPerNodeKey, negativeShardsPerNode).build())
                    .get();
            } else {
                client().admin().cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(shardsPerNodeKey, negativeShardsPerNode).build())
                    .get();
            }
            fail("should not be able to set negative shards per node");
        } catch (IllegalArgumentException ex) {
            assertEquals("Failed to parse value [" + negativeShardsPerNode + "] for setting [cluster.max_shards_per_node] must be >= 1",
                ex.getMessage());
        }
    }

    private void setShardsPerNode(int shardsPerNode) {
        try {
            ClusterUpdateSettingsResponse response;
            if (frequently()) {
                response = client().admin().cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().put(shardsPerNodeKey, shardsPerNode).build())
                    .get();
                assertEquals(shardsPerNode, response.getPersistentSettings().getAsInt(shardsPerNodeKey, -1).intValue());
            } else {
                response = client().admin().cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(shardsPerNodeKey, shardsPerNode).build())
                    .get();
                assertEquals(shardsPerNode, response.getTransientSettings().getAsInt(shardsPerNodeKey, -1).intValue());
            }
        } catch (IllegalArgumentException ex) {
            fail(ex.getMessage());
        }
    }

    public static class ShardCounts {
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
            int mainIndexReplicas = between(0, dataNodes - 1);
            int mainIndexShards = between(1, 10);
            int totalShardsInIndex = (mainIndexReplicas + 1) * mainIndexShards;
            int shardsPerNode = (int) Math.ceil((double) totalShardsInIndex / dataNodes);
            int totalCap = shardsPerNode * dataNodes;

            int failingIndexShards;
            int failingIndexReplicas;
            if (dataNodes > 1 && frequently()) {
                failingIndexShards = Math.max(1, totalCap - totalShardsInIndex);
                failingIndexReplicas = between(1, dataNodes - 1);
            } else {
                failingIndexShards = totalCap - totalShardsInIndex + between(1, 10);
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
}
