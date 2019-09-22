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

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MockEngineFactoryPlugin;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReplicaShardAllocatorIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(MockEngineFactoryPlugin.class);
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    /**
     * Verify that if we found a new copy where it can perform an operation-based recovery,
     * then we will cancel the current recovery and allocate replica to the new copy.
     */
    public void testPreferCopyCanPerformOperationBasedRecovery() throws Exception {
        String indexName = "test";
        String nodeWithPrimary = internalCluster().startNode();
        assertAcked(
            client().admin().indices().prepareCreate(indexName)
                .setSettings(Settings.builder()
                    .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), 1.0f)
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                    .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "1ms")));
        String nodeWithReplica = internalCluster().startDataOnlyNode();
        Settings nodeWithReplicaSettings = internalCluster().dataPathSettings(nodeWithReplica);
        ensureGreen(indexName);
        indexRandom(randomBoolean(), randomBoolean(), randomBoolean(), IntStream.range(0, between(100, 1000))
            .mapToObj(n -> client().prepareIndex(indexName, "_doc", Integer.toString(n)).setSource("f", "v")).collect(Collectors.toList()));
        ensureGlobalCheckpointSyncedAndPersisted(indexName);
        if (randomBoolean()) {
            client().admin().indices().prepareFlush(indexName).get();
        }
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeWithReplica));
        if (randomBoolean()) {
            indexRandom(randomBoolean(), randomBoolean(), randomBoolean(), IntStream.range(0, between(10, 200))
                .mapToObj(n -> client().prepareIndex(indexName, "_doc", Integer.toString(n)).setSource("f", "v"))
                .collect(Collectors.toList()));
        }
        if (randomBoolean()) {
            client().admin().indices().prepareForceMerge(indexName).setFlush(true).get();
        }
        CountDownLatch blockRecovery = new CountDownLatch(1);
        CountDownLatch recoveryStarted = new CountDownLatch(1);
        MockTransportService transportServiceOnPrimary
            = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeWithPrimary);
        transportServiceOnPrimary.addSendBehavior((connection, requestId, action, request, options) -> {
            if (PeerRecoveryTargetService.Actions.FILES_INFO.equals(action)) {
                recoveryStarted.countDown();
                try {
                    blockRecovery.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });
        internalCluster().startDataOnlyNode();
        recoveryStarted.await();
        nodeWithReplica = internalCluster().startDataOnlyNode(nodeWithReplicaSettings);
        // AllocationService only calls GatewayAllocator if there're unassigned shards
        assertAcked(client().admin().indices().prepareCreate("dummy-index").setWaitForActiveShards(0));
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), Matchers.hasItem(nodeWithReplica));

        blockRecovery.countDown();
        transportServiceOnPrimary.clearAllRules();
    }

    private void ensureGlobalCheckpointSyncedAndPersisted(String indexName) throws Exception {
        assertBusy(() -> {
            Index index = resolveIndex(indexName);
            for (String node : internalCluster().nodesInclude(indexName)) {
                IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
                final IndexService indexService = indicesService.indexService(index);
                if (indexService != null) {
                    for (IndexShard shard : indexService) {
                        assertThat(shard.getLastSyncedGlobalCheckpoint(), equalTo(shard.getLocalCheckpoint()));
                    }
                }
            }
        });
    }
}
