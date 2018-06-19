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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

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
        assertEquals("[host=http://1, roles=mdi]", new Node(new HttpHost("1"),
                null, null, null, new Roles(true, true, true), null).toString());
        assertEquals("[host=http://1, version=ver]", new Node(new HttpHost("1"),
                null, null, "ver", null, null).toString());
        assertEquals("[host=http://1, name=nam]", new Node(new HttpHost("1"),
                null, "nam", null, null, null).toString());
        assertEquals("[host=http://1, bound=[http://1, http://2]]", new Node(new HttpHost("1"),
                new HashSet<>(Arrays.asList(new HttpHost("1"), new HttpHost("2"))), null, null, null, null).toString());
        assertEquals(
                "[host=http://1, bound=[http://1, http://2], name=nam, version=ver, roles=m, attributes={foo=[bar], baz=[bort, zoom]}]",
                new Node(new HttpHost("1"), new HashSet<>(Arrays.asList(new HttpHost("1"), new HttpHost("2"))),
                    "nam", "ver", new Roles(true, false, false), attributes).toString());

    }

    public void testEqualsAndHashCode() {
        HttpHost host = new HttpHost(randomAsciiAlphanumOfLength(5));
        Node node = new Node(host,
                randomBoolean() ? null : singleton(host),
                randomBoolean() ? null : randomAsciiAlphanumOfLength(5),
                randomBoolean() ? null : randomAsciiAlphanumOfLength(5),
                randomBoolean() ? null : new Roles(true, true, true),
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
                node.getVersion(), new Roles(false, false, false), node.getAttributes())));
                assertFalse(node.equals(new Node(host, node.getBoundHosts(), node.getName(),
                node.getVersion(), node.getRoles(), singletonMap("bort", singletonList("bing")))));
    }
}
