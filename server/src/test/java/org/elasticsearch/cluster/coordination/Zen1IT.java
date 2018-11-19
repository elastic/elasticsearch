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

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class Zen1IT extends ESIntegTestCase {

    private static Settings ZEN1_SETTINGS = Coordinator.addZen1Attribute(Settings.builder()
        .put(TestZenDiscovery.USE_ZEN2.getKey(), false)
        .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false)) // Zen2 does not know about mock pings
        .build();

    private static Settings ZEN2_SETTINGS = Settings.builder()
        .put(TestZenDiscovery.USE_ZEN2.getKey(), true)
        .build();

    public void testZen2NodesJoiningZen1Cluster() {
        internalCluster().startNodes(randomIntBetween(1, 3), ZEN1_SETTINGS);
        internalCluster().startNodes(randomIntBetween(1, 3), ZEN2_SETTINGS);
        createIndex("test");
    }

    public void testZen1NodesJoiningZen2Cluster() {
        internalCluster().startNodes(randomIntBetween(1, 3), ZEN2_SETTINGS);
        internalCluster().startNodes(randomIntBetween(1, 3), ZEN1_SETTINGS);
        createIndex("test");
    }

    @TestLogging("org.elasticsearch.discovery.zen:TRACE,org.elasticsearch.cluster.coordination:TRACE")
    public void testUpgradingFromZen1ToZen2ClusterWithThreeNodes() throws IOException {
        final List<String> zen1Nodes = internalCluster().startNodes(3, ZEN1_SETTINGS);

        createIndex("test",
            Settings.builder()
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.ZERO) // Assign shards
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3) // causes rebalancing
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                .build());
        ensureGreen("test");

        for (final String zen1Node : zen1Nodes) {
            logger.info("--> shutting down {}", zen1Node);
            internalCluster().stopRandomNode(s -> NODE_NAME_SETTING.get(s).equals(zen1Node));
            ensureGreen("test");
            logger.info("--> starting replacement for {}", zen1Node);
            final String newNode = internalCluster().startNode(ZEN2_SETTINGS);
            ensureGreen("test");
            logger.info("--> successfully replaced {} with {}", zen1Node, newNode);
        }

        assertThat(internalCluster().size(), equalTo(3));
    }
}
