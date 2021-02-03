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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeTests extends RestClientTestCase {
    public void testToString() {
        Map<String, List<String>> attributes = new HashMap<>();
        attributes.put("foo", singletonList("bar"));
        attributes.put("baz", Arrays.asList("bort", "zoom"));
        assertEquals("[host=http://1]", new Node(new HttpHost("1")).toString());
        assertEquals("[host=http://1, attributes={foo=[bar], baz=[bort, zoom]}]",
                new Node(new HttpHost("1"), null, null, null, null, attributes).toString());
        assertEquals("[host=http://1, roles=data,ingest,master]", new Node(new HttpHost("1"),
                null, null, null, new Roles(new TreeSet<>(Arrays.asList("master", "data", "ingest"))), null).toString());
        assertEquals("[host=http://1, version=ver]", new Node(new HttpHost("1"),
                null, null, "ver", null, null).toString());
        assertEquals("[host=http://1, name=nam]", new Node(new HttpHost("1"),
                null, "nam", null, null, null).toString());
        assertEquals("[host=http://1, bound=[http://1, http://2]]", new Node(new HttpHost("1"),
                new HashSet<>(Arrays.asList(new HttpHost("1"), new HttpHost("2"))), null, null, null, null).toString());
        assertEquals(
                "[host=http://1, bound=[http://1, http://2], "
                    + "name=nam, version=ver, roles=master, attributes={foo=[bar], baz=[bort, zoom]}]",
                new Node(new HttpHost("1"), new HashSet<>(Arrays.asList(new HttpHost("1"), new HttpHost("2"))),
                    "nam", "ver", new Roles(Collections.singleton("master")), attributes).toString());
    }

    public void testEqualsAndHashCode() {
        HttpHost host = new HttpHost(randomAsciiAlphanumOfLength(5));
        Node node = new Node(host,
                randomBoolean() ? null : singleton(host),
                randomBoolean() ? null : randomAsciiAlphanumOfLength(5),
                randomBoolean() ? null : randomAsciiAlphanumOfLength(5),
                randomBoolean() ? null : new Roles(new TreeSet<>(Arrays.asList("master", "data", "ingest"))),
                randomBoolean() ? null : singletonMap("foo", singletonList("bar")));
        assertFalse(node.equals(null));
        assertTrue(node.equals(node));
        assertEquals(node.hashCode(), node.hashCode());
        Node copy = new Node(host, node.getBoundHosts(), node.getName(), node.getVersion(),
                node.getRoles(), node.getAttributes());
        assertTrue(node.equals(copy));
        assertEquals(node.hashCode(), copy.hashCode());
        assertFalse(node.equals(new Node(new HttpHost(host.toHostString() + "changed"), node.getBoundHosts(),
                node.getName(), node.getVersion(), node.getRoles(), node.getAttributes())));
        assertFalse(node.equals(new Node(host, new HashSet<>(Arrays.asList(host, new HttpHost(host.toHostString() + "changed"))),
                node.getName(), node.getVersion(), node.getRoles(), node.getAttributes())));
        assertFalse(node.equals(new Node(host, node.getBoundHosts(), node.getName() + "changed",
                node.getVersion(), node.getRoles(), node.getAttributes())));
        assertFalse(node.equals(new Node(host, node.getBoundHosts(), node.getName(),
                node.getVersion() + "changed", node.getRoles(), node.getAttributes())));
        assertFalse(node.equals(new Node(host, node.getBoundHosts(), node.getName(),
                node.getVersion(), new Roles(Collections.emptySet()), node.getAttributes())));
                assertFalse(node.equals(new Node(host, node.getBoundHosts(), node.getName(),
                node.getVersion(), node.getRoles(), singletonMap("bort", singletonList("bing")))));
    }
}
