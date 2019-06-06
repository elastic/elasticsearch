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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.coordination.PeersResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.EqualsHashCodeTestUtils.CopyFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class PeerFinderMessagesTests extends ESTestCase {
    private DiscoveryNode createNode(String id) {
        return new DiscoveryNode(id, buildNewFakeTransportAddress(), Version.CURRENT);
    }

    public void testPeersRequestEqualsHashCodeSerialization() {
        final PeersRequest initialPeersRequest = new PeersRequest(createNode(randomAlphaOfLength(10)),
            Arrays.stream(generateRandomStringArray(10, 10, false)).map(this::createNode).collect(Collectors.toList()));

        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialPeersRequest,
                (CopyFunction<PeersRequest>) publishRequest -> copyWriteable(publishRequest, writableRegistry(), PeersRequest::new),
            in -> {
                final List<DiscoveryNode> discoveryNodes = new ArrayList<>(in.getKnownPeers());
                if (randomBoolean()) {
                    return new PeersRequest(createNode(randomAlphaOfLength(10)), discoveryNodes);
                } else {
                    return new PeersRequest(in.getSourceNode(), modifyDiscoveryNodesList(in.getKnownPeers(), true));
                }
            });
    }

    public void testPeersResponseEqualsHashCodeSerialization() {
        final long initialTerm = randomNonNegativeLong();
        final PeersResponse initialPeersResponse;

        if (randomBoolean()) {
            initialPeersResponse = new PeersResponse(Optional.of(createNode(randomAlphaOfLength(10))), emptyList(), initialTerm);
        } else {
            initialPeersResponse = new PeersResponse(Optional.empty(),
                Arrays.stream(generateRandomStringArray(10, 10, false, false)).map(this::createNode).collect(Collectors.toList()),
                initialTerm);
        }

        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(initialPeersResponse,
                (CopyFunction<PeersResponse>) publishResponse -> copyWriteable(publishResponse, writableRegistry(), PeersResponse::new),
            in -> {
                final long term = in.getTerm();
                if (randomBoolean()) {
                    return new PeersResponse(in.getMasterNode(), in.getKnownPeers(),
                        randomValueOtherThan(term, ESTestCase::randomNonNegativeLong));
                } else {
                    if (in.getMasterNode().isPresent()) {
                        if (randomBoolean()) {
                            return new PeersResponse(Optional.of(createNode(randomAlphaOfLength(10))), in.getKnownPeers(), term);
                        } else {
                            return new PeersResponse(Optional.empty(), singletonList(createNode(randomAlphaOfLength(10))), term);
                        }
                    } else {
                        if (randomBoolean()) {
                            return new PeersResponse(Optional.of(createNode(randomAlphaOfLength(10))), emptyList(), term);
                        } else {
                            return new PeersResponse(in.getMasterNode(), modifyDiscoveryNodesList(in.getKnownPeers(), false), term);
                        }
                    }
                }
            });
    }


    private List<DiscoveryNode> modifyDiscoveryNodesList(Collection<DiscoveryNode> originalNodes, boolean allowEmpty) {
        final List<DiscoveryNode> discoveryNodes = new ArrayList<>(originalNodes);
        if (discoveryNodes.isEmpty() == false && randomBoolean() && (allowEmpty || discoveryNodes.size() > 1)) {
            discoveryNodes.remove(randomIntBetween(0, discoveryNodes.size() - 1));
        } else if (discoveryNodes.isEmpty() == false && randomBoolean()) {
            discoveryNodes.set(randomIntBetween(0, discoveryNodes.size() - 1), createNode(randomAlphaOfLength(10)));
        } else {
            discoveryNodes.add(createNode(randomAlphaOfLength(10)));
        }
        return discoveryNodes;
    }
}
