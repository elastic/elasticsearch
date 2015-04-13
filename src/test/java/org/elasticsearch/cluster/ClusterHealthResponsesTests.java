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

package org.elasticsearch.cluster;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth;
import org.elasticsearch.action.admin.cluster.health.ClusterShardHealth;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.empty;

public class ClusterHealthResponsesTests extends ElasticsearchTestCase {


    private void assertIndexHealth(ClusterIndexHealth indexHealth, ShardCounter counter, IndexMetaData indexMetaData) {
        assertThat(indexHealth.getStatus(), equalTo(counter.status()));
        assertThat(indexHealth.getNumberOfShards(), equalTo(indexMetaData.getNumberOfShards()));
        assertThat(indexHealth.getNumberOfReplicas(), equalTo(indexMetaData.getNumberOfReplicas()));
        assertThat(indexHealth.getActiveShards(), equalTo(counter.active));
        assertThat(indexHealth.getRelocatingShards(), equalTo(counter.relocating));
        assertThat(indexHealth.getInitializingShards(), equalTo(counter.initializing));
        assertThat(indexHealth.getUnassignedShards(), equalTo(counter.unassigned));
        assertThat(indexHealth.getShards().size(), equalTo(indexMetaData.getNumberOfShards()));
        assertThat(indexHealth.getValidationFailures(), empty());
        int totalShards = 0;
        for (ClusterShardHealth shardHealth : indexHealth.getShards().values()) {
            totalShards += shardHealth.getActiveShards() + shardHealth.getInitializingShards() + shardHealth.getUnassignedShards();
        }

        assertThat(totalShards, equalTo(indexMetaData.getNumberOfShards() * (1 + indexMetaData.getNumberOfReplicas())));
    }

    protected class ShardCounter {
        public int active;
        public int relocating;
        public int initializing;
        public int unassigned;
        public int primaryActive;
        public int primaryInactive;

        public ClusterHealthStatus status() {
            if (primaryInactive > 0) {
                return ClusterHealthStatus.RED;
            }
            if (unassigned > 0 || initializing > 0) {
                return ClusterHealthStatus.YELLOW;
            }
            return ClusterHealthStatus.GREEN;
        }

        public void update(ShardRouting shardRouting) {
            if (shardRouting.active()) {
                active++;
                if (shardRouting.primary()) {
                    primaryActive++;
                }
                if (shardRouting.relocating()) {
                    relocating++;
                }
                return;
            }

            if (shardRouting.primary()) {
                primaryInactive++;
            }
            if (shardRouting.initializing()) {
                initializing++;
            } else {
                unassigned++;
            }
        }
    }

    static int node_id = 1;

    private ImmutableShardRouting genShardRouting(String index, int shardId, boolean primary) {

        ShardRoutingState state;

        int i = randomInt(40);
        if (i > 5) {
            state = ShardRoutingState.STARTED;
        } else if (i > 3) {
            state = ShardRoutingState.RELOCATING;
        } else if (i > 1) {
            state = ShardRoutingState.INITIALIZING;
        } else {
            state = ShardRoutingState.UNASSIGNED;
        }

        switch (state) {
            case UNASSIGNED:
                return new MutableShardRouting(index, shardId, null, primary, ShardRoutingState.UNASSIGNED, 1);
            case STARTED:
                return new MutableShardRouting(index, shardId, "node_" + Integer.toString(node_id++), primary, ShardRoutingState.STARTED, 1);
            case INITIALIZING:
                return new MutableShardRouting(index, shardId, "node_" + Integer.toString(node_id++), primary, ShardRoutingState.INITIALIZING, 1);
            case RELOCATING:
                return new MutableShardRouting(index, shardId, "node_" + Integer.toString(node_id++), "node_" + Integer.toString(node_id++), primary, ShardRoutingState.RELOCATING, 1);
            default:
                throw new ElasticsearchException("Unknown state: " + state.name());
        }

    }

    private IndexShardRoutingTable genShardRoutingTable(String index, int shardId, int replicas, ShardCounter counter) {
        IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(new ShardId(index, shardId), true);
        ImmutableShardRouting shardRouting = genShardRouting(index, shardId, true);
        counter.update(shardRouting);
        builder.addShard(shardRouting);
        for (; replicas > 0; replicas--) {
            shardRouting = genShardRouting(index, shardId, false);
            counter.update(shardRouting);
            builder.addShard(shardRouting);
        }

        return builder.build();
    }

    IndexRoutingTable genIndexRoutingTable(IndexMetaData indexMetaData, ShardCounter counter) {
        IndexRoutingTable.Builder builder = IndexRoutingTable.builder(indexMetaData.index());
        for (int shard = 0; shard < indexMetaData.numberOfShards(); shard++) {
            builder.addIndexShard(genShardRoutingTable(indexMetaData.index(), shard, indexMetaData.getNumberOfReplicas(), counter));
        }
        return builder.build();
    }

    @Test
    public void testClusterIndexHealth() {
        int numberOfShards = randomInt(3) + 1;
        int numberOfReplicas = randomInt(4);
        IndexMetaData indexMetaData = IndexMetaData.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(numberOfShards).numberOfReplicas(numberOfReplicas).build();
        ShardCounter counter = new ShardCounter();
        IndexRoutingTable indexRoutingTable = genIndexRoutingTable(indexMetaData, counter);

        ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetaData, indexRoutingTable);
        logger.info("index status: {}, expected {}", indexHealth.getStatus(), counter.status());
        assertIndexHealth(indexHealth, counter, indexMetaData);
    }

    private void assertClusterHealth(ClusterHealthResponse clusterHealth, ShardCounter counter) {
        assertThat(clusterHealth.getStatus(), equalTo(counter.status()));
        assertThat(clusterHealth.getActiveShards(), equalTo(counter.active));
        assertThat(clusterHealth.getActivePrimaryShards(), equalTo(counter.primaryActive));
        assertThat(clusterHealth.getInitializingShards(), equalTo(counter.initializing));
        assertThat(clusterHealth.getRelocatingShards(), equalTo(counter.relocating));
        assertThat(clusterHealth.getUnassignedShards(), equalTo(counter.unassigned));
        assertThat(clusterHealth.getValidationFailures(), empty());
    }

    @Test
    public void testClusterHealth() throws IOException {
        ShardCounter counter = new ShardCounter();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        MetaData.Builder metaData = MetaData.builder();
        for (int i = randomInt(4); i >= 0; i--) {
            int numberOfShards = randomInt(3) + 1;
            int numberOfReplicas = randomInt(4);
            IndexMetaData indexMetaData = IndexMetaData.builder("test_" + Integer.toString(i)).settings(settings(Version.CURRENT)).numberOfShards(numberOfShards).numberOfReplicas(numberOfReplicas).build();
            IndexRoutingTable indexRoutingTable = genIndexRoutingTable(indexMetaData, counter);
            metaData.put(indexMetaData, true);
            routingTable.add(indexRoutingTable);
        }
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        int pendingTasks = randomIntBetween(0, 200);
        ClusterHealthResponse clusterHealth = new ClusterHealthResponse("bla", clusterState.metaData().concreteIndices(IndicesOptions.strictExpand(), (String[]) null), clusterState, pendingTasks);
        logger.info("cluster status: {}, expected {}", clusterHealth.getStatus(), counter.status());
        clusterHealth = maybeSerialize(clusterHealth);
        assertClusterHealth(clusterHealth, counter);
        assertThat(clusterHealth.getNumberOfPendingTasks(), Matchers.equalTo(pendingTasks));
    }

    ClusterHealthResponse maybeSerialize(ClusterHealthResponse clusterHealth) throws IOException {
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            clusterHealth.writeTo(out);
            BytesStreamInput in = new BytesStreamInput(out.bytes());
            clusterHealth = ClusterHealthResponse.readResponseFrom(in);
        }
        return clusterHealth;
    }

    @Test
    public void testValidations() throws IOException {
        IndexMetaData indexMetaData = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(2).build();
        ShardCounter counter = new ShardCounter();
        IndexRoutingTable indexRoutingTable = genIndexRoutingTable(indexMetaData, counter);
        indexMetaData = IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(2).numberOfReplicas(3).build();

        ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetaData, indexRoutingTable);
        assertThat(indexHealth.getValidationFailures(), Matchers.hasSize(2));

        RoutingTable.Builder routingTable = RoutingTable.builder();
        MetaData.Builder metaData = MetaData.builder();
        metaData.put(indexMetaData, true);
        routingTable.add(indexRoutingTable);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
        ClusterHealthResponse clusterHealth = new ClusterHealthResponse("bla", clusterState.metaData().concreteIndices(IndicesOptions.strictExpand(), (String[]) null), clusterState, 0);
        clusterHealth = maybeSerialize(clusterHealth);
        // currently we have no cluster level validation failures as index validation issues are reported per index.
        assertThat(clusterHealth.getValidationFailures(), Matchers.hasSize(0));
    }
}
