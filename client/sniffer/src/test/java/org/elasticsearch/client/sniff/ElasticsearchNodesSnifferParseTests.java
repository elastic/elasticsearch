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

package org.elasticsearch.client.sniff;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClientTestCase;
import org.elasticsearch.client.Node.Roles;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer.Scheme;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonFactory;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test parsing the response from the {@code /_nodes/http} API from fixed
 * versions of Elasticsearch.
 */
public class ElasticsearchNodesSnifferParseTests extends RestClientTestCase {
    private void checkFile(String file, Node... expected) throws IOException {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
        if (in == null) {
            throw new IllegalArgumentException("Couldn't find [" + file + "]");
        }
        try {
            HttpEntity entity = new InputStreamEntity(in, ContentType.APPLICATION_JSON);
            List<Node> nodes = ElasticsearchNodesSniffer.readHosts(entity, Scheme.HTTP, new JsonFactory());
            /*
             * Use these assertions because the error messages are nicer
             * than hasItems and we know the results are in order because
             * that is how we generated the file.
             */
            assertThat(nodes, hasSize(expected.length));
            for (int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], nodes.get(i));
            }
        } finally {
            in.close();
        }
    }

    public void test2x() throws IOException {
        checkFile("2.0.0_nodes_http.json",
                node(9200, "m1", "2.0.0", true, false, false),
                node(9201, "m2", "2.0.0", true, true, false),
                node(9202, "m3", "2.0.0", true, false, false),
                node(9203, "d1", "2.0.0", false, true, false),
                node(9204, "d2", "2.0.0", false, true, false),
                node(9205, "d3", "2.0.0", false, true, false),
                node(9206, "c1", "2.0.0", false, false, false),
                node(9207, "c2", "2.0.0", false, false, false));
    }

    public void test5x() throws IOException {
        checkFile("5.0.0_nodes_http.json",
                node(9200, "m1", "5.0.0", true, false, true),
                node(9201, "m2", "5.0.0", true, true, true),
                node(9202, "m3", "5.0.0", true, false, true),
                node(9203, "d1", "5.0.0", false, true, true),
                node(9204, "d2", "5.0.0", false, true, true),
                node(9205, "d3", "5.0.0", false, true, true),
                node(9206, "c1", "5.0.0", false, false, true),
                node(9207, "c2", "5.0.0", false, false, true));
    }

    public void test6x() throws IOException {
        checkFile("6.0.0_nodes_http.json",
                node(9200, "m1", "6.0.0", true, false, true),
                node(9201, "m2", "6.0.0", true, true, true),
                node(9202, "m3", "6.0.0", true, false, true),
                node(9203, "d1", "6.0.0", false, true, true),
                node(9204, "d2", "6.0.0", false, true, true),
                node(9205, "d3", "6.0.0", false, true, true),
                node(9206, "c1", "6.0.0", false, false, true),
                node(9207, "c2", "6.0.0", false, false, true));
    }

    public void testParsingPublishAddressWithPreES7Format() throws IOException {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("es6_nodes_publication_address_format.json");

        HttpEntity entity = new InputStreamEntity(in, ContentType.APPLICATION_JSON);
        List<Node> nodes = ElasticsearchNodesSniffer.readHosts(entity, Scheme.HTTP, new JsonFactory());

        assertEquals("127.0.0.1", nodes.get(0).getHost().getHostName());
        assertEquals(9200, nodes.get(0).getHost().getPort());
        assertEquals("http", nodes.get(0).getHost().getSchemeName());
    }

    public void testParsingPublishAddressWithES7Format() throws IOException {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("es7_nodes_publication_address_format.json");

        HttpEntity entity = new InputStreamEntity(in, ContentType.APPLICATION_JSON);
        List<Node> nodes = ElasticsearchNodesSniffer.readHosts(entity, Scheme.HTTP, new JsonFactory());

        assertEquals("elastic.test", nodes.get(0).getHost().getHostName());
        assertEquals(9200, nodes.get(0).getHost().getPort());
        assertEquals("http", nodes.get(0).getHost().getSchemeName());
    }

    private Node node(int port, String name, String version, boolean master, boolean data, boolean ingest) {
        HttpHost host = new HttpHost("127.0.0.1", port);
        Set<HttpHost> boundHosts = new HashSet<>(2);
        boundHosts.add(host);
        boundHosts.add(new HttpHost("[::1]", port));
        Map<String, List<String>> attributes = new HashMap<>();
        attributes.put("dummy", singletonList("everyone_has_me"));
        attributes.put("number", singletonList(name.substring(1)));
        attributes.put("array", Arrays.asList(name.substring(0, 1), name.substring(1)));
        return new Node(host, boundHosts, name, version, new Roles(master, data, ingest), attributes);
    }
}
