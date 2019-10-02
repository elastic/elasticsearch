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

import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReplicaShardAllocatorIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class);
    }

    public void testRecentPrimaryInformation() throws Exception {
        String indexName = "test";
        String nodeWithPrimary = internalCluster().startNode();
        assertAcked(
            client().admin().indices().prepareCreate(indexName)
                .setSettings(Settings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.FILE_BASED_RECOVERY_THRESHOLD_SETTING.getKey(), 1.0f)
                    .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "100ms")
                    .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "1ms")));
        String nodeWithReplica = internalCluster().startDataOnlyNode();
        Settings nodeWithReplicaSettings = internalCluster().dataPathSettings(nodeWithReplica);
        ensureGreen(indexName);
        indexRandom(randomBoolean(), randomBoolean(), randomBoolean(), IntStream.range(0, between(10, 100))
            .mapToObj(n -> client().prepareIndex(indexName, "_doc").setSource("f", "v")).collect(Collectors.toList()));
        assertBusy(() -> {
            SyncedFlushResponse syncedFlushResponse = client().admin().indices().prepareSyncedFlush(indexName).get();
            assertThat(syncedFlushResponse.successfulShards(), equalTo(2));
        });
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeWithReplica));
        if (randomBoolean()) {
            indexRandom(randomBoolean(), randomBoolean(), randomBoolean(), IntStream.range(0, between(10, 100))
                .mapToObj(n -> client().prepareIndex(indexName, "_doc").setSource("f", "v")).collect(Collectors.toList()));
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
        String newNode = internalCluster().startDataOnlyNode();
        recoveryStarted.await();
        // destroy sync_id after the recovery on the new node has started
        client().admin().indices().prepareFlush(indexName).setForce(true).get();
        // AllocationService only calls GatewayAllocator if there are unassigned shards
        assertAcked(client().admin().indices().prepareCreate("dummy-index").setWaitForActiveShards(0)
            .setSettings(Settings.builder().put("index.routing.allocation.require.attr", "not-found")));
        internalCluster().startDataOnlyNode(nodeWithReplicaSettings);
        // need to wait for events to ensure the reroute has happened since we perform it async when a new node joins.
        client().admin().cluster().prepareHealth(indexName).setWaitForYellowStatus().setWaitForEvents(Priority.LANGUID).get();
        blockRecovery.countDown();
        ensureGreen(indexName);
        assertThat(internalCluster().nodesInclude(indexName), hasItem(newNode));
        transportServiceOnPrimary.clearAllRules();
    }
}
