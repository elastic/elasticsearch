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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class NodeTests extends RestClientTestCase {
    public void testWithBoundHostAsHost() {
        HttpHost h1 = new HttpHost("1");
        HttpHost h2 = new HttpHost("2");
        HttpHost h3 = new HttpHost("3");

        Node n = new Node(h1, Arrays.asList(h1, h2, h3), randomAsciiAlphanumOfLength(5),
            new Roles(randomBoolean(), randomBoolean(), randomBoolean()));
        assertEquals(h2, n.withBoundHostAsHost(h2).getHost());
        assertEquals(n.getBoundHosts(), n.withBoundHostAsHost(h2).getBoundHosts());

        try {
            n.withBoundHostAsHost(new HttpHost("4"));
            fail("expected failure");
        } catch (IllegalArgumentException e) {
            assertEquals("http://4 must be a bound host but wasn't in [http://1, http://2, http://3]",
                    e.getMessage());
        }
    }

    public void testToString() {
        assertEquals("[host=http://1]", new Node(new HttpHost("1")).toString());
        assertEquals("[host=http://1, roles=mdi]", new Node(new HttpHost("1"),
                null, null, new Roles(true, true, true)).toString());
        assertEquals("[host=http://1, version=ver]", new Node(new HttpHost("1"),
                null, "ver", null).toString());
        assertEquals("[host=http://1, bound=[http://1, http://2]]", new Node(new HttpHost("1"),
                Arrays.asList(new HttpHost("1"), new HttpHost("2")), null, null).toString());
    }

    // TODO tests for equals and hashcode probably
}
