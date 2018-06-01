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
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

public class NodeTests extends RestClientTestCase {
    public void testWithHost() {
        HttpHost h1 = new HttpHost("1");
        HttpHost h2 = new HttpHost("2");
        HttpHost h3 = new HttpHost("3");

        Node n = new Node(h1, new HashSet<>(Arrays.asList(h1, h2)),
                randomAsciiAlphanumOfLength(5), randomAsciiAlphanumOfLength(5),
                new Roles(randomBoolean(), randomBoolean(), randomBoolean()));

        // Host is in the bound hosts list
        assertEquals(h2, n.withHost(h2).getHost());
        assertEquals(n.getBoundHosts(), n.withHost(h2).getBoundHosts());

        // Host not in the bound hosts list
        assertEquals(h3, n.withHost(h3).getHost());
        assertEquals(new HashSet<>(Arrays.asList(h1, h2, h3)), n.withHost(h3).getBoundHosts());
    }

    public void testToString() {
        assertEquals("[host=http://1]", new Node(new HttpHost("1")).toString());
        assertEquals("[host=http://1, roles=mdi]", new Node(new HttpHost("1"),
                null, null, null, new Roles(true, true, true)).toString());
        assertEquals("[host=http://1, version=ver]", new Node(new HttpHost("1"),
                null, null, "ver", null).toString());
        assertEquals("[host=http://1, name=nam]", new Node(new HttpHost("1"),
                null, "nam", null, null).toString());
        assertEquals("[host=http://1, bound=[http://1, http://2]]", new Node(new HttpHost("1"),
                new HashSet<>(Arrays.asList(new HttpHost("1"), new HttpHost("2"))), null, null, null).toString());
        assertEquals("[host=http://1, bound=[http://1, http://2], name=nam, version=ver, roles=m]",
                new Node(new HttpHost("1"), new HashSet<>(Arrays.asList(new HttpHost("1"), new HttpHost("2"))),
                    "nam", "ver", new Roles(true, false, false)).toString());

    }
}
