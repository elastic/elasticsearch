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
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;

public class GetDiscoveredNodesResponseTests extends ESTestCase {
    public void testSerialization() throws IOException {
        final GetDiscoveredNodesResponse original = new GetDiscoveredNodesResponse(randomDiscoveryNodeSet());
        final GetDiscoveredNodesResponse deserialized = copyWriteable(original, writableRegistry(), GetDiscoveredNodesResponse::new);
        assertThat(deserialized.getNodes(), equalTo(original.getNodes()));
    }

    private Set<DiscoveryNode> randomDiscoveryNodeSet() {
        final int size = randomIntBetween(1, 10);
        final Set<DiscoveryNode> nodes = new HashSet<>(size);
        while (nodes.size() < size) {
            assertTrue(nodes.add(new DiscoveryNode(randomAlphaOfLength(10), randomAlphaOfLength(10),
                UUIDs.randomBase64UUID(random()), randomAlphaOfLength(10), randomAlphaOfLength(10), buildNewFakeTransportAddress(),
                emptyMap(), singleton(Role.MASTER), Version.CURRENT)));
        }
        return nodes;
    }

    public void testConversionToBootstrapConfiguration() {
        final Set<DiscoveryNode> nodes = randomDiscoveryNodeSet();
        assertThat(new GetDiscoveredNodesResponse(nodes).getBootstrapConfiguration().resolve(nodes).getNodeIds(),
            equalTo(nodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet())));
    }
}
