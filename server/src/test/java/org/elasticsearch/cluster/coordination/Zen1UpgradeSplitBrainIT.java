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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkUnresponsive;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService.TestPlugin;

import java.util.Collection;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoMinMasterNodes = false)
@TestLogging("org.elasticsearch.cluster.coordination:TRACE,org.elasticsearch.discovery.zen:TRACE,org.elasticsearch.cluster.service:TRACE")
public class Zen1UpgradeSplitBrainIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return singletonList(TestPlugin.class);
    }

    private void ensureHealthyMasterVia(String clientNode) {
        ClusterHealthResponse clusterHealthResponse = client(clientNode).admin().cluster().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes(">=1")
            .setTimeout(TimeValue.timeValueSeconds(30)).get();
        assertFalse(clientNode, clusterHealthResponse.isTimedOut());
    }

    public void testOneNodeMigration() throws Exception {
        final String zen1Node = internalCluster().startNodes(Coordinator.addZen1Attribute(true, Settings.builder()
            .put(TestZenDiscovery.USE_ZEN2.getKey(), false)
            .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false)) // Zen2 does not know about mock pings
            .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 1)
            .build()).get(0);

        final String zen2Node = internalCluster().startNodes(Settings.builder()
            .put(TestZenDiscovery.USE_ZEN2.getKey(), true)
            // give up on old leader quickly after partition
            .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            // do the auto-upgrade quickly
            .put(ElectMasterService.DISCOVERY_ZEN_MINIMUM_MASTER_NODES_SETTING.getKey(), 1)
            .put(DiscoveryUpgradeService.BWC_PING_TIMEOUT_SETTING.getKey(), "1s")
            // finish becoming master quickly
            .put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "1s")
            .build()).get(0);

        assertThat(internalCluster().getMasterName(), is(zen1Node));

        final NetworkDisruption networkDisruption = new NetworkDisruption(new TwoPartitions(zen1Node, zen2Node), new NetworkUnresponsive());
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        logger.info("--> waiting for discovery upgrade service to bootstrap the new node");
        assertBusy(() -> {
            final DiscoveryNode masterNode = client(zen2Node).admin().cluster().prepareState().clear().setNodes(true).setLocal(true)
                .get().getState().nodes().getMasterNode();
            assertThat(masterNode, not(nullValue()));
            assertThat(masterNode.getName(), equalTo(zen2Node));
        });
        ensureHealthyMasterVia(zen2Node);

        logger.info("--> bootstrapped and new master elected, removing partition");
        networkDisruption.stopDisrupting();
        ensureFullyConnectedCluster();

        logger.info("--> publishing cluster state update from old node");

        try {
            client(zen1Node).admin().indices().prepareCreate("test").setSettings(Settings.builder()
                .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 2)
                .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)).get();
        } catch (ResourceAlreadyExistsException e) {
            // this is ok, we sent it to the wrong master and then retried
        }

        ensureHealthyMasterVia(zen1Node);
        ensureHealthyMasterVia(zen2Node);

        assertThat(internalCluster().getMasterName(zen1Node), is(zen2Node));
        assertThat(internalCluster().getMasterName(zen2Node), is(zen2Node));
    }
}
