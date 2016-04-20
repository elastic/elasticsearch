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
package org.elasticsearch.cluster.node;

import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.VersionUtils.randomVersion;

public class DiscoveryNodeTests extends ESTestCase {

    static AtomicInteger idGenerator = new AtomicInteger();

    /**
     * guarantees to generate a node that is unique for all aspects but for it's roles and version (which are chosen randomly)
     */
    DiscoveryNode randomNode() {
        Map<String, String> attributes = new HashMap<>();
        for (int attr = (idGenerator.get() > 0 ? 1 : 0) + randomInt(3);
             attr > 0;
             attr--) {
            attributes.put("attr_" + attr, "v_" + idGenerator.incrementAndGet());
        }
        final DiscoveryNode node = new DiscoveryNode(
            "name_" + idGenerator.incrementAndGet(),
            "id_" + idGenerator.incrementAndGet(),
            LocalTransportAddress.buildUnique(),
            attributes, new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))),
            randomVersion(random()));
        return node;
    }

    public void testEquality() {
        DiscoveryNode node1 = randomNode();
        DiscoveryNode node2 = randomNode();
        logger.info("node1: {}", node1);
        logger.info("node2: {}", node2);

        assertFalse(node1.equals(node2));

        DiscoveryNode sameIdDifferentMeta;

        sameIdDifferentMeta = new DiscoveryNode(node2.getName(), node1.getId(), node2.getAddress(), node2.getAttributes(),
            node2.getRoles(), node2.getVersion());
        assertFalse(node1.equals(sameIdDifferentMeta));

        sameIdDifferentMeta = new DiscoveryNode(node2.getName(), node1.getId(), node1.getAddress(), node1.getAttributes(),
            (randomBoolean() ? node1 : node2).getRoles(), (randomBoolean() ? node1 : node2).getVersion());
        assertFalse(node1.equals(sameIdDifferentMeta));

        sameIdDifferentMeta = new DiscoveryNode(node1.getName(), node1.getId(), node2.getAddress(), node1.getAttributes(),
            (randomBoolean() ? node1 : node2).getRoles(), (randomBoolean() ? node1 : node2).getVersion());
        assertFalse(node1.equals(sameIdDifferentMeta));

        sameIdDifferentMeta = new DiscoveryNode(node1.getName(), node1.getId(), node1.getAddress(), node2.getAttributes(),
            (randomBoolean() ? node1 : node2).getRoles(), (randomBoolean() ? node1 : node2).getVersion());
        assertFalse(node1.equals(sameIdDifferentMeta));
    }
}
