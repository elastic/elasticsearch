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

package org.elasticsearch.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node.Roles;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class NodeSelectorTests extends RestClientTestCase {
    public void testAny() {
        List<Node> nodes = new ArrayList<>();
        int size = between(2, 5);
        for (int i = 0; i < size; i++) {
            nodes.add(dummyNode(randomBoolean(), randomBoolean(), randomBoolean()));
        }
        assertEquals(nodes, NodeSelector.ANY.select(nodes));
    }

    public void testNotMasterOnly() {
        Node masterOnly = dummyNode(true, false, randomBoolean());
        Node masterAndData = dummyNode(true, true, randomBoolean());
        Node client = dummyNode(false, false, randomBoolean());
        Node data = dummyNode(false, true, randomBoolean());
        List<Node> nodes = Arrays.asList(masterOnly, masterAndData, client, data);
        Collections.shuffle(nodes, getRandom());
        List<Node> expected = new ArrayList<>(nodes);
        expected.remove(masterOnly);
        assertEquals(expected, NodeSelector.NOT_MASTER_ONLY.select(nodes));
    }

    private Node dummyNode(boolean master, boolean data, boolean ingest) {
        return new Node(new HttpHost("dummy"), Collections.<HttpHost>emptyList(),
                randomAsciiAlphanumOfLength(5), new Roles(master, data, ingest));
    }
}
