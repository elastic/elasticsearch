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
package org.elasticsearch.discovery;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.coordination.LeaderChecker;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkLinkDisruptionType;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkUnresponsive;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService.TestPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests relating to the loss of the master, but which work with the default fault detection settings which are rather lenient and will
 * not detect a master failure too quickly.
 */
@TestLogging("_root:DEBUG,org.elasticsearch.cluster.service:TRACE")
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, transportClientRatio = 0)
public class StableMasterDisruptionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(TestPlugin.class);
    }

    /**
     * Test that no split brain occurs under partial network partition. See https://github.com/elastic/elasticsearch/issues/2488
     */
    public void testFailWithMinimumMasterNodesConfigured() throws Exception {
        List<String> nodes = internalCluster().startNodes(3);
        ensureStableCluster(3);

        // Figure out what is the elected master node
        final String masterNode = internalCluster().getMasterName();
        logger.info("---> legit elected master node={}", masterNode);

        // Pick a node that isn't the elected master.
        Set<String> nonMasters = new HashSet<>(nodes);
        nonMasters.remove(masterNode);
        final String unluckyNode = randomFrom(nonMasters.toArray(Strings.EMPTY_ARRAY));

        // Simulate a network issue between the unlucky node and elected master node in both directions.

        NetworkDisruption networkDisconnect = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(masterNode, unluckyNode),
            new NetworkDisruption.NetworkDisconnect());
        setDisruptionScheme(networkDisconnect);
        networkDisconnect.startDisrupting();

        // Wait until elected master has removed that the unlucky node...
        ensureStableCluster(2, masterNode);

        // The unlucky node must report *no* master node, since it can't connect to master and in fact it should
        // continuously ping until network failures have been resolved. However
        // It may a take a bit before the node detects it has been cut off from the elected master
        ensureNoMaster(unluckyNode);

        networkDisconnect.stopDisrupting();

        // Wait until the master node sees all 3 nodes again.
        ensureStableCluster(3);

        // The elected master shouldn't have changed, since the unlucky node never could have elected itself as master
        assertThat(internalCluster().getMasterName(), equalTo(masterNode));
    }

    private void ensureNoMaster(String node) throws Exception {
        assertBusy(() -> assertNull(client(node).admin().cluster().state(
            new ClusterStateRequest().local(true)).get().getState().nodes().getMasterNode()));
    }

    /**
     * Verify that nodes fault detection detects a disconnected node after master reelection
     */
    public void testFollowerCheckerDetectsDisconnectedNodeAfterMasterReelection() throws Exception {
        testFollowerCheckerAfterMasterReelection(new NetworkDisconnect(), Settings.EMPTY);
    }

    /**
     * Verify that nodes fault detection detects an unresponsive node after master reelection
     */
    public void testFollowerCheckerDetectsUnresponsiveNodeAfterMasterReelection() throws Exception {
        testFollowerCheckerAfterMasterReelection(new NetworkUnresponsive(), Settings.builder()
            .put(LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), "10")
            .put(FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING.getKey(), "1s")
            .put(FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey(), 1).build());
    }

    private void testFollowerCheckerAfterMasterReelection(NetworkLinkDisruptionType networkLinkDisruptionType,
                                                          Settings settings) throws Exception {
        internalCluster().startNodes(4, settings);
        ensureStableCluster(4);

        logger.info("--> stopping current master");
        internalCluster().stopCurrentMasterNode();

        ensureStableCluster(3);

        final String master = internalCluster().getMasterName();
        final List<String> nonMasters = Arrays.stream(internalCluster().getNodeNames()).filter(n -> master.equals(n) == false)
            .collect(Collectors.toList());
        final String isolatedNode = randomFrom(nonMasters);
        final String otherNode = nonMasters.get(nonMasters.get(0).equals(isolatedNode) ? 1 : 0);

        logger.info("--> isolating [{}]", isolatedNode);

        final NetworkDisruption networkDisruption = new NetworkDisruption(new TwoPartitions(
            singleton(isolatedNode), Sets.newHashSet(master, otherNode)), networkLinkDisruptionType);
        setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();

        logger.info("--> waiting for master to remove it");
        ensureStableCluster(2, master);
        ensureNoMaster(isolatedNode);

        networkDisruption.stopDisrupting();
        ensureStableCluster(3);
    }
}
