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
import org.elasticsearch.discovery.zen.ElectMasterService.MasterCandidate;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class ElectMasterServiceTests extends ESTestCase {

    ElectMasterService electMasterService() {
        return new ElectMasterService(Settings.EMPTY);
    }

    List<DiscoveryNode> generateRandomNodes() {
        int count = scaledRandomIntBetween(1, 100);
        ArrayList<DiscoveryNode> nodes = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Set<DiscoveryNode.Role> roles = new HashSet<>();
            if (randomBoolean()) {
                roles.add(DiscoveryNode.Role.MASTER);
            }
            DiscoveryNode node = new DiscoveryNode("n_" + i, "n_" + i, buildNewFakeTransportAddress(), Collections.emptyMap(),
                    roles, Version.CURRENT);
            nodes.add(node);
        }

        Collections.shuffle(nodes, random());
        return nodes;
    }

    List<MasterCandidate> generateRandomCandidates() {
        int count = scaledRandomIntBetween(1, 100);
        ArrayList<MasterCandidate> candidates = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Set<DiscoveryNode.Role> roles = new HashSet<>();
            roles.add(DiscoveryNode.Role.MASTER);
            DiscoveryNode node = new DiscoveryNode("n_" + i, "n_" + i, buildNewFakeTransportAddress(), Collections.emptyMap(),
                roles, Version.CURRENT);
            candidates.add(
                new MasterCandidate(node, randomBoolean() ? MasterCandidate.UNRECOVERED_CLUSTER_VERSION : randomNonNegativeLong()));
        }

        Collections.shuffle(candidates, random());
        return candidates;
    }


    public void testSortByMasterLikelihood() {
        List<DiscoveryNode> nodes = generateRandomNodes();
        List<DiscoveryNode> sortedNodes = ElectMasterService.sortByMasterLikelihood(nodes);
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

    public void testTieBreakActiveMasters() {
        List<DiscoveryNode> nodes = generateRandomCandidates().stream().map(MasterCandidate::getNode).collect(Collectors.toList());
        DiscoveryNode bestMaster = electMasterService().tieBreakActiveMasters(nodes);
        for (DiscoveryNode node: nodes) {
            if (node.equals(bestMaster) == false) {
                assertTrue(bestMaster.getId().compareTo(node.getId()) < 0);
            }
        }
    }

    public void testHasEnoughNodes() {
        List<DiscoveryNode> nodes = rarely() ? Collections.emptyList() : generateRandomNodes();
        ElectMasterService service = electMasterService();
        int masterNodes = (int) nodes.stream().filter(DiscoveryNode::isMasterNode).count();
        service.minimumMasterNodes(randomIntBetween(-1, masterNodes));
        assertThat(service.hasEnoughMasterNodes(nodes), equalTo(masterNodes > 0));
        service.minimumMasterNodes(masterNodes + 1 + randomIntBetween(0, nodes.size()));
        assertFalse(service.hasEnoughMasterNodes(nodes));
    }

    public void testHasEnoughCandidates() {
        List<MasterCandidate> candidates = rarely() ? Collections.emptyList() : generateRandomCandidates();
        ElectMasterService service = electMasterService();
        service.minimumMasterNodes(randomIntBetween(-1, candidates.size()));
        assertThat(service.hasEnoughCandidates(candidates), equalTo(candidates.size() > 0));
        service.minimumMasterNodes(candidates.size() + 1 + randomIntBetween(0, candidates.size()));
        assertFalse(service.hasEnoughCandidates(candidates));
    }

    public void testElectMaster() {
        List<MasterCandidate> candidates = generateRandomCandidates();
        ElectMasterService service = electMasterService();
        int minMasterNodes = randomIntBetween(0, candidates.size());
        service.minimumMasterNodes(minMasterNodes);
        MasterCandidate master = service.electMaster(candidates);
        assertNotNull(master);
        for (MasterCandidate candidate : candidates) {
            if (candidate.getNode().equals(master.getNode())) {
                // nothing much to test here
            } else if (candidate.getClusterStateVersion() == master.getClusterStateVersion()) {
                assertThat("candidate " + candidate + " has a lower or equal id than master " + master, candidate.getNode().getId(),
                    greaterThan(master.getNode().getId()));
            } else {
                assertThat("candidate " + master + " has a higher cluster state version than candidate " + candidate,
                    master.getClusterStateVersion(), greaterThan(candidate.getClusterStateVersion()));
            }
        }
    }

    public void testCountMasterNodes() {
        List<DiscoveryNode> nodes = generateRandomNodes();
        ElectMasterService service = electMasterService();

        int masterNodes = 0;

        for (DiscoveryNode node : nodes) {
            if (node.isMasterNode()) {
                masterNodes++;
            }
        }

        assertEquals(masterNodes, service.countMasterNodes(nodes));
    }
}
