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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.discovery.zen.elect.ElectMasterService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ElectMasterServiceTests extends ESTestCase {

    ElectMasterService electMasterService() {
        return new ElectMasterService(Settings.EMPTY, Version.CURRENT);
    }

    List<DiscoveryNode> generateRandomNodes() {
        int count = scaledRandomIntBetween(1, 100);
        ArrayList<DiscoveryNode> nodes = new ArrayList<>(count);



        for (int i = 0; i < count; i++) {
            Set<DiscoveryNode.Role> roles = new HashSet<>();
            if (randomBoolean()) {
                roles.add(DiscoveryNode.Role.MASTER);
            }
            DiscoveryNode node = new DiscoveryNode("n_" + i, "n_" + i, DummyTransportAddress.INSTANCE, Collections.emptyMap(),
                    roles, Version.CURRENT);
            nodes.add(node);
        }

        Collections.shuffle(nodes, random());
        return nodes;
    }

    public void testSortByMasterLikelihood() {
        List<DiscoveryNode> nodes = generateRandomNodes();
        List<DiscoveryNode> sortedNodes = electMasterService().sortByMasterLikelihood(nodes);
        assertEquals(nodes.size(), sortedNodes.size());
        DiscoveryNode prevNode = sortedNodes.get(0);
        for (int i = 1; i < sortedNodes.size(); i++) {
            DiscoveryNode node = sortedNodes.get(i);
            if (!prevNode.isMasterNode()) {
                assertFalse(node.isMasterNode());
            } else if (node.isMasterNode()) {
                assertTrue(prevNode.getId().compareTo(node.getId()) < 0);
            }
            prevNode = node;
        }

    }

    public void testElectMaster() {
        List<DiscoveryNode> nodes = generateRandomNodes();
        ElectMasterService service = electMasterService();
        int min_master_nodes = randomIntBetween(0, nodes.size());
        service.minimumMasterNodes(min_master_nodes);

        int master_nodes = 0;
        for (DiscoveryNode node : nodes) {
            if (node.isMasterNode()) {
                master_nodes++;
            }
        }
        DiscoveryNode master = null;
        if (service.hasEnoughMasterNodes(nodes)) {
            master = service.electMaster(nodes);
        }

        if (master_nodes == 0) {
            assertNull(master);
        } else if (min_master_nodes > 0 && master_nodes < min_master_nodes) {
            assertNull(master);
        } else {
            assertNotNull(master);
            for (DiscoveryNode node : nodes) {
                if (node.isMasterNode()) {
                    assertTrue(master.getId().compareTo(node.getId()) <= 0);
                }
            }
        }
    }
}
