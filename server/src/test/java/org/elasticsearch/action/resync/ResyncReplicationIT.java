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

package org.elasticsearch.action.resync;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.engine.MockInternalEngine;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ResyncReplicationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final Set<Class<? extends Plugin>> mockPlugins = new HashSet<>(super.getMockPlugins());
        mockPlugins.add(MockEngineFactoryPlugin.class);
        return mockPlugins;
    }

    /**
     * This test asserts that replicas failed to execute resync operations will be failed but not marked as stale.
     */
    public void testFailResyncFailedReplicasButNotMarkAsStale() throws Exception {
        final int numberOfReplicas = between(2, 5);
        internalCluster().startMasterOnlyNode();
        final String primaryNode = internalCluster().startDataOnlyNode();
        assertAcked(
            prepareCreate("test", Settings.builder().put(indexSettings())
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)));
        ensureYellow();
        final List<String> replicaNodes = internalCluster().startDataOnlyNodes(numberOfReplicas);
        ensureGreen();
        logger.info("--> Disable shard allocation");
        assertAcked(
            client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.allocation.enable", "none")).get());
        final ShardId shardId = new ShardId(clusterService().state().metaData().index("test").getIndex(), 0);
        logger.info("--> Indexing with gap in seqno to ensure that some operations will be replayed in resync");
        int numDocs = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            index("test", "doc", Integer.toString(i));
        }
        getEngine(primaryNode, shardId).getLocalCheckpointTracker().generateSeqNo(); // Make gap
        int moreDocs = scaledRandomIntBetween(1, 10);
        for (int i = 0; i < moreDocs; i++) {
            index("test", "doc", Integer.toString(numDocs + i));
        }
        logger.info("--> Promote a new primary and fail one resync operation on one replica");
        final AtomicReference<String> failedNode = new AtomicReference<>();
        for (String replicaNode : replicaNodes) {
            getEngine(replicaNode, shardId).setPreIndexingInterceptor((op) -> {
                if (failedNode.compareAndSet(null, replicaNode)) {
                    throw new EngineException(shardId, "Intercepted to fail an indexing operation on [{}]", replicaNode);
                }
            });
        }
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
        assertBusy(() -> {
            final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            assertThat(clusterState.routingTable().shardRoutingTable(shardId).activeShards(), hasSize(numberOfReplicas - 1));
            assertThat(clusterState.metaData().index("test").inSyncAllocationIds(shardId.id()), hasSize(numberOfReplicas + 1));
            for (String replicaNode : replicaNodes) {
                final IndexShard shard = internalCluster().getInstance(IndicesService.class, replicaNode).getShardOrNull(shardId);
                if (replicaNode.equals(failedNode.get()) == false) {
                    assertThat(shard.getLocalCheckpoint(), equalTo((long) numDocs + moreDocs));
                } else {
                    assertThat(shard, nullValue());
                }
            }
        });
        logger.info("--> Re-enable shard allocation; resync-failed shard will be recovered");
        assertAcked(
            client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.allocation.enable", "all")).get());
        assertBusy(() -> {
            final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            assertThat(clusterState.routingTable().shardRoutingTable(shardId).activeShards(), hasSize(numberOfReplicas));
            assertThat(clusterState.metaData().index("test").inSyncAllocationIds(shardId.id()), hasSize(numberOfReplicas + 1));
            assertThat(internalCluster().getInstance(IndicesService.class, failedNode.get()).getShardOrNull(shardId), notNullValue());
            for (String replicaNode : replicaNodes) {
                final IndexShard shard = internalCluster().getInstance(IndicesService.class, replicaNode).getShardOrNull(shardId);
                assertThat(shard.getLocalCheckpoint(), equalTo((long) numDocs + moreDocs));
            }
        });
    }

    MockInternalEngine getEngine(String node, ShardId shardId) {
        final IndexShard indexShard = internalCluster().getInstance(IndicesService.class, node).getShardOrNull(shardId);
        return (MockInternalEngine) IndexShardTestCase.getEngine(indexShard);
    }
}
