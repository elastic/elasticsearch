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

package org.elasticsearch.discovery.zen;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.discovery.zen.ping.ZenPing;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;

public class ZenPingTests extends ESTestCase {
    public void testPingCollection() {
        DiscoveryNode[] nodes = new DiscoveryNode[randomIntBetween(1, 30)];
        long maxIdPerNode[] = new long[nodes.length];
        DiscoveryNode masterPerNode[] = new DiscoveryNode[nodes.length];
        boolean hasJoinedOncePerNode[] = new boolean[nodes.length];
        ArrayList<ZenPing.PingResponse> pings = new ArrayList<>();
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new DiscoveryNode("" + i, DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        }

        for (int pingCount = scaledRandomIntBetween(10, nodes.length * 10); pingCount > 0; pingCount--) {
            int node = randomInt(nodes.length - 1);
            DiscoveryNode masterNode = null;
            if (randomBoolean()) {
                masterNode = nodes[randomInt(nodes.length - 1)];
            }
            boolean hasJoinedOnce = randomBoolean();
            ZenPing.PingResponse ping = new ZenPing.PingResponse(nodes[node], masterNode, ClusterName.CLUSTER_NAME_SETTING.
                getDefault(Settings.EMPTY), hasJoinedOnce);
            if (rarely()) {
                // ignore some pings
                continue;
            }
            // update max ping info
            maxIdPerNode[node] = ping.id();
            masterPerNode[node] = masterNode;
            hasJoinedOncePerNode[node] = hasJoinedOnce;
            pings.add(ping);
        }

        // shuffle
        Collections.shuffle(pings, random());

        ZenPing.PingCollection collection = new ZenPing.PingCollection();
        collection.addPings(pings.toArray(new ZenPing.PingResponse[pings.size()]));

        ZenPing.PingResponse[] aggregate = collection.toArray();

        for (ZenPing.PingResponse ping : aggregate) {
            int nodeId = Integer.parseInt(ping.node().getId());
            assertThat(maxIdPerNode[nodeId], equalTo(ping.id()));
            assertThat(masterPerNode[nodeId], equalTo(ping.master()));
            assertThat(hasJoinedOncePerNode[nodeId], equalTo(ping.hasJoinedOnce()));

            maxIdPerNode[nodeId] = -1; // mark as seen
        }

        for (int i = 0; i < maxIdPerNode.length; i++) {
            assertTrue("node " + i + " had pings but it was not found in collection", maxIdPerNode[i] <= 0);
        }


    }
}
