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

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.http.HttpHost;
import org.apache.lucene.util.LuceneTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.sameInstance;

public class NodeTests extends LuceneTestCase {

    public void testSingleArgumentConstructor() {
        HttpHost httpHost = new HttpHost(randomHost(), randomPort(), randomScheme());
        Node node = new Node(httpHost);
        assertThat(node.getHttpHost(), sameInstance(httpHost));
        assertThat(node.getAttributes(), notNullValue());
        assertThat(node.getAttributes().size(), equalTo(0));
        assertThat(node.getRoles(), notNullValue());
        assertThat(node.getRoles(), equalTo(new HashSet<>(Arrays.asList(Node.Role.values()))));

        try {
            new Node(null);
            fail("node construction should have failed");
        } catch(NullPointerException e) {
            assertThat(e.getMessage(), equalTo("host cannot be null"));
        }

    }

    public void testThreeArgumentsConstructor() {
        HttpHost httpHost = new HttpHost(randomHost(), randomPort(), randomScheme());
        Set<Node.Role> roles = randomRoles();
        Map<String, String> attributes = randomAttributes();
        Node node = new Node(httpHost, roles, attributes);
        assertThat(node.getHttpHost(), sameInstance(httpHost));
        assertThat(node.getAttributes(), equalTo(attributes));
        assertThat(node.getRoles(), equalTo(roles));

        try {
            new Node(null, roles, attributes);
            fail("node construction should have failed");
        } catch(NullPointerException e) {
            assertThat(e.getMessage(), equalTo("host cannot be null"));
        }

        try {
            new Node(httpHost, null, attributes);
            fail("node construction should have failed");
        } catch(NullPointerException e) {
            assertThat(e.getMessage(), equalTo("roles cannot be null"));
        }

        try {
            new Node(httpHost, roles, null);
            fail("node construction should have failed");
        } catch(NullPointerException e) {
            assertThat(e.getMessage(), equalTo("attributes cannot be null"));
        }
    }

    public void testToString() {
        HttpHost httpHost = new HttpHost(randomHost(), randomPort(), randomScheme());
        Set<Node.Role> roles = randomRoles();
        Map<String, String> attributes = randomAttributes();
        Node node = new Node(httpHost, roles, attributes);
        String expectedString = "Node{" +
                "httpHost=" + httpHost.toString() +
                ", roles=" + roles.toString() +
                ", attributes=" + attributes.toString() +
                '}';
        assertThat(node.toString(), equalTo(expectedString));
    }

    private static String randomHost() {
        return RandomStrings.randomAsciiOfLengthBetween(random(), 5, 10);
    }

    private static int randomPort() {
        return random().nextInt();
    }

    private static String randomScheme() {
        if (rarely()) {
            return null;
        }
        return random().nextBoolean() ? "http" : "https";
    }

    private static Map<String, String> randomAttributes() {
        int numAttributes = RandomInts.randomIntBetween(random(), 0, 5);
        Map<String, String> attributes = new HashMap<>(numAttributes);
        for (int i = 0; i < numAttributes; i++) {
            String key = RandomStrings.randomAsciiOfLengthBetween(random(), 3, 10);
            String value = RandomStrings.randomAsciiOfLengthBetween(random(), 3, 10);
            attributes.put(key, value);
        }
        return attributes;
    }

    private static Set<Node.Role> randomRoles() {
        int numRoles = RandomInts.randomIntBetween(random(), 0, 3);
        Set<Node.Role> roles = new HashSet<>(numRoles);
        for (int j = 0; j < numRoles; j++) {
            roles.add(RandomPicks.randomFrom(random(), Node.Role.values()));
        }
        return roles;
    }
}
