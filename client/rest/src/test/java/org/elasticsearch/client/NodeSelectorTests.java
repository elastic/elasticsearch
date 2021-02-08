/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node.Roles;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class NodeSelectorTests extends RestClientTestCase {
    public void testAny() {
        List<Node> nodes = new ArrayList<>();
        int size = between(2, 5);
        for (int i = 0; i < size; i++) {
            nodes.add(dummyNode(randomBoolean(), randomBoolean(), randomBoolean()));
        }
        List<Node> expected = new ArrayList<>(nodes);
        NodeSelector.ANY.select(nodes);
        assertEquals(expected, nodes);
    }

    public void testNotMasterOnly() {
        Node masterOnly = dummyNode(true, false, false);
        Node all = dummyNode(true, true, true);
        Node masterAndData = dummyNode(true, true, false);
        Node masterAndIngest = dummyNode(true, false, true);
        Node coordinatingOnly = dummyNode(false, false, false);
        Node ingestOnly = dummyNode(false, false, true);
        Node data = dummyNode(false, true, randomBoolean());
        List<Node> nodes = new ArrayList<>();
        nodes.add(masterOnly);
        nodes.add(all);
        nodes.add(masterAndData);
        nodes.add(masterAndIngest);
        nodes.add(coordinatingOnly);
        nodes.add(ingestOnly);
        nodes.add(data);
        Collections.shuffle(nodes, getRandom());
        List<Node> expected = new ArrayList<>(nodes);
        expected.remove(masterOnly);
        NodeSelector.SKIP_DEDICATED_MASTERS.select(nodes);
        assertEquals(expected, nodes);
    }

    private static Node dummyNode(boolean master, boolean data, boolean ingest) {
        final Set<String> roles = new TreeSet<>();
        if (master) {
            roles.add("master");
        }
        if (data) {
            roles.add("data");
        }
        if (ingest) {
            roles.add("ingest");
        }
        return new Node(new HttpHost("dummy"), Collections.<HttpHost>emptySet(),
                randomAsciiAlphanumOfLength(5), randomAsciiAlphanumOfLength(5),
                new Roles(roles),
                Collections.<String, List<String>>emptyMap());
    }
}
