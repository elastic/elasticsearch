package org.elasticsearch.action.support.master;

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

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.discovery.zen.fd.FaultDetection;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisconnectPartition;
import org.elasticsearch.test.disruption.NetworkPartition;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@ESIntegTestCase.SuppressLocalMode
public class IndexingMasterFailoverIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final HashSet<Class<? extends Plugin>> classes = new HashSet<>(super.nodePlugins());
        classes.add(MockTransportService.TestPlugin.class);
        return classes;
    }

    /**
     * Indexing operations which entail mapping changes require a blocking request to the master node to update the mapping.
     * If the master node is being disrupted or if it cannot commit cluster state changes, it needs to retry within timeout limits.
     * This retry logic is implemented in TransportMasterNodeAction and tested by the following master failover scenario.
     */
    public void testMasterFailoverDuringIndexingWithMappingChanges() throws Throwable {
        logger.info("--> start 4 nodes, 3 master, 1 data");

        final Settings sharedSettings = Settings.builder()
                .put(FaultDetection.PING_TIMEOUT_SETTING.getKey(), "1s") // for hitting simulated network failures quickly
                .put(FaultDetection.PING_RETRIES_SETTING.getKey(), "1") // for hitting simulated network failures quickly
                .put("discovery.zen.join_timeout", "10s")  // still long to induce failures but to long so test won't time out
                .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), "1s") // <-- for hitting simulated network failures quickly
                .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 2)
                .build();

        internalCluster().startMasterOnlyNodesAsync(3, sharedSettings).get();

        String dataNode = internalCluster().startDataOnlyNode(sharedSettings);

        logger.info("--> wait for all nodes to join the cluster");
        ensureStableCluster(4);

        // We index data with mapping changes into cluster and have master failover at same time
        client().admin().indices().prepareCreate("myindex")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .get();
        ensureGreen("myindex");

        final CyclicBarrier barrier = new CyclicBarrier(2);

        Thread indexingThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    barrier.await();
                } catch (InterruptedException e) {
                    logger.warn("Barrier interrupted", e);
                    return;
                } catch (BrokenBarrierException e) {
                    logger.warn("Broken barrier", e);
                    return;
                }
                for (int i = 0; i < 10; i++) {
                    // index data with mapping changes
                    IndexResponse response = client(dataNode).prepareIndex("myindex", "mytype").setSource("field_" + i, "val").get();
                    assertThat(response.isCreated(), equalTo(true));
                }
            }
        });
        indexingThread.setName("indexingThread");
        indexingThread.start();

        barrier.await();

        // interrupt communication between master and other nodes in cluster
        String master = internalCluster().getMasterName();
        Set<String> otherNodes = new HashSet<>(Arrays.asList(internalCluster().getNodeNames()));
        otherNodes.remove(master);

        NetworkPartition partition = new NetworkDisconnectPartition(Collections.singleton(master), otherNodes, random());
        internalCluster().setDisruptionScheme(partition);

        logger.info("--> disrupting network");
        partition.startDisrupting();

        logger.info("--> waiting for new master to be elected");
        ensureStableCluster(3, dataNode);

        partition.stopDisrupting();
        logger.info("--> waiting to heal");
        ensureStableCluster(4);

        indexingThread.join();

        ensureGreen("myindex");
        refresh();
        assertThat(client().prepareSearch("myindex").get().getHits().getTotalHits(), equalTo(10L));
    }
}
