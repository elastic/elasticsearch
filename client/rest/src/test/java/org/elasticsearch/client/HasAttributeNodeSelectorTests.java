/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node.Roles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

public class HasAttributeNodeSelectorTests extends RestClientTestCase {
    public void testHasAttribute() {
        Node hasAttributeValue = dummyNode(singletonMap("attr", singletonList("val")));
        Node hasAttributeButNotValue = dummyNode(singletonMap("attr", singletonList("notval")));
        Node hasAttributeValueInList = dummyNode(singletonMap("attr", Arrays.asList("val", "notval")));
        Node notHasAttribute = dummyNode(singletonMap("notattr", singletonList("val")));
        List<Node> nodes = new ArrayList<>();
        nodes.add(hasAttributeValue);
        nodes.add(hasAttributeButNotValue);
        nodes.add(hasAttributeValueInList);
        nodes.add(notHasAttribute);
        List<Node> expected = new ArrayList<>();
        expected.add(hasAttributeValue);
        expected.add(hasAttributeValueInList);
        new HasAttributeNodeSelector("attr", "val").select(nodes);
        assertEquals(expected, nodes);
    }

    private static Node dummyNode(Map<String, List<String>> attributes) {
        final Set<String> roles = new TreeSet<>();
        if (randomBoolean()) {
            roles.add("master");
        }
        if (randomBoolean()) {
            roles.add("data");
        }
        if (randomBoolean()) {
            roles.add("ingest");
        }
        return new Node(new HttpHost("dummy"), Collections.<HttpHost>emptySet(),
                randomAsciiAlphanumOfLength(5), randomAsciiAlphanumOfLength(5),
                new Roles(roles),
                attributes);
    }
}
