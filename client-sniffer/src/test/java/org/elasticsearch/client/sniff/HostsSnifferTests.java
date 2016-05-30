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

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.Consts;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpGet;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.client.ElasticsearchResponse;
import org.elasticsearch.client.ElasticsearchResponseException;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class HostsSnifferTests extends LuceneTestCase {

    private int sniffRequestTimeout;
    private String scheme;
    private SniffResponse sniffResponse;
    private HttpServer httpServer;

    @Before
    public void startHttpServer() throws IOException {
        this.sniffRequestTimeout = RandomInts.randomIntBetween(random(), 1000, 10000);
        this.scheme = RandomPicks.randomFrom(random(), Arrays.asList("http", "https"));
        if (rarely()) {
            this.sniffResponse = SniffResponse.buildFailure();
        } else {
            this.sniffResponse = buildSniffResponse(scheme);
        }
        this.httpServer = createHttpServer(sniffResponse, sniffRequestTimeout);
        this.httpServer.start();
    }

    @After
    public void stopHttpServer() throws IOException {
        httpServer.stop(0);
    }

    public void testSniffNodes() throws IOException, URISyntaxException {
        HttpHost httpHost = new HttpHost(httpServer.getAddress().getHostName(), httpServer.getAddress().getPort());
        try (RestClient restClient = RestClient.builder().setHosts(httpHost).build()) {
            HostsSniffer sniffer = new HostsSniffer(restClient, sniffRequestTimeout, scheme);
            try {
                List<HttpHost> sniffedHosts = sniffer.sniffHosts();
                if (sniffResponse.isFailure) {
                    fail("sniffNodes should have failed");
                }
                assertThat(sniffedHosts.size(), equalTo(sniffResponse.hosts.size()));
                Iterator<HttpHost> responseHostsIterator = sniffResponse.hosts.iterator();
                for (HttpHost sniffedHost : sniffedHosts) {
                    assertEquals(sniffedHost, responseHostsIterator.next());
                }
            } catch(ElasticsearchResponseException e) {
                ElasticsearchResponse response = e.getElasticsearchResponse();
                if (sniffResponse.isFailure) {
                    assertThat(e.getMessage(), containsString("GET http://localhost:" + httpServer.getAddress().getPort() +
                            "/_nodes/http?timeout=" + sniffRequestTimeout));
                    assertThat(e.getMessage(), containsString(Integer.toString(sniffResponse.nodesInfoResponseCode)));
                    assertThat(response.getHost(), equalTo(httpHost));
                    assertThat(response.getStatusLine().getStatusCode(), equalTo(sniffResponse.nodesInfoResponseCode));
                    assertThat(response.getRequestLine().toString(),
                            equalTo("GET /_nodes/http?timeout=" + sniffRequestTimeout + "ms HTTP/1.1"));
                } else {
                    fail("sniffNodes should have succeeded: " + response.getStatusLine());
                }
            }
        }
    }

    private static HttpServer createHttpServer(final SniffResponse sniffResponse, final int sniffTimeout) throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        httpServer.createContext("/_nodes/http", new HttpHandler() {
            @Override
            public void handle(HttpExchange httpExchange) throws IOException {
                if (httpExchange.getRequestMethod().equals(HttpGet.METHOD_NAME)) {
                    if (httpExchange.getRequestURI().getRawQuery().equals("timeout=" + sniffTimeout + "ms")) {
                        String nodesInfoBody = sniffResponse.nodesInfoBody;
                        httpExchange.sendResponseHeaders(sniffResponse.nodesInfoResponseCode, nodesInfoBody.length());
                        try (OutputStream out = httpExchange.getResponseBody()) {
                            out.write(nodesInfoBody.getBytes(Consts.UTF_8));
                            return;
                        }
                    }
                }
                httpExchange.sendResponseHeaders(404, 0);
                httpExchange.close();
            }
        });
        return httpServer;
    }

    private static SniffResponse buildSniffResponse(String scheme) throws IOException {
        int numNodes = RandomInts.randomIntBetween(random(), 1, 5);
        List<HttpHost> hosts = new ArrayList<>(numNodes);
        JsonFactory jsonFactory = new JsonFactory();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = jsonFactory.createGenerator(writer);
        generator.writeStartObject();
        if (random().nextBoolean()) {
            generator.writeStringField("cluster_name", "elasticsearch");
        }
        if (random().nextBoolean()) {
            generator.writeObjectFieldStart("bogus_object");
            generator.writeEndObject();
        }
        generator.writeObjectFieldStart("nodes");
        for (int i = 0; i < numNodes; i++) {
            String nodeId = RandomStrings.randomAsciiOfLengthBetween(random(), 5, 10);
            generator.writeObjectFieldStart(nodeId);
            if (random().nextBoolean()) {
                generator.writeObjectFieldStart("bogus_object");
                generator.writeEndObject();
            }
            if (random().nextBoolean()) {
                generator.writeArrayFieldStart("bogus_array");
                generator.writeStartObject();
                generator.writeEndObject();
                generator.writeEndArray();
            }
            boolean isHttpEnabled = rarely() == false;
            if (isHttpEnabled) {
                String host = "host" + i;
                int port = RandomInts.randomIntBetween(random(), 9200, 9299);
                HttpHost httpHost = new HttpHost(host, port, scheme);
                hosts.add(httpHost);
                generator.writeObjectFieldStart("http");
                if (random().nextBoolean()) {
                    generator.writeArrayFieldStart("bound_address");
                    generator.writeString("[fe80::1]:" + port);
                    generator.writeString("[::1]:" + port);
                    generator.writeString("127.0.0.1:" + port);
                    generator.writeEndArray();
                }
                if (random().nextBoolean()) {
                    generator.writeObjectFieldStart("bogus_object");
                    generator.writeEndObject();
                }
                generator.writeStringField("publish_address", httpHost.toHostString());
                if (random().nextBoolean()) {
                    generator.writeNumberField("max_content_length_in_bytes", 104857600);
                }
                generator.writeEndObject();
            }
            String[] roles = {"master", "data", "ingest"};
            int numRoles = RandomInts.randomIntBetween(random(), 0, 3);
            Set<String> nodeRoles = new HashSet<>(numRoles);
            for (int j = 0; j < numRoles; j++) {
                String role;
                do {
                    role = RandomPicks.randomFrom(random(), roles);
                } while(nodeRoles.add(role) == false);
            }
            generator.writeArrayFieldStart("roles");
            for (String nodeRole : nodeRoles) {
                generator.writeString(nodeRole);
            }
            generator.writeEndArray();
            int numAttributes = RandomInts.randomIntBetween(random(), 0, 3);
            Map<String, String> attributes = new HashMap<>(numAttributes);
            for (int j = 0; j < numAttributes; j++) {
                attributes.put("attr" + j, "value" + j);
            }
            if (numAttributes > 0) {
                generator.writeObjectFieldStart("attributes");
            }
            for (Map.Entry<String, String> entry : attributes.entrySet()) {
                generator.writeStringField(entry.getKey(), entry.getValue());
            }
            if (numAttributes > 0) {
                generator.writeEndObject();
            }
            generator.writeEndObject();
        }
        generator.writeEndObject();
        generator.writeEndObject();
        generator.close();
        return SniffResponse.buildResponse(writer.toString(), hosts);
    }

    private static class SniffResponse {
        private final String nodesInfoBody;
        private final int nodesInfoResponseCode;
        private final List<HttpHost> hosts;
        private final boolean isFailure;

        SniffResponse(String nodesInfoBody, List<HttpHost> hosts, boolean isFailure) {
            this.nodesInfoBody = nodesInfoBody;
            this.hosts = hosts;
            this.isFailure = isFailure;
            if (isFailure) {
                this.nodesInfoResponseCode = randomErrorResponseCode();
            } else {
                this.nodesInfoResponseCode = 200;
            }
        }

        static SniffResponse buildFailure() {
            return new SniffResponse("", Collections.<HttpHost>emptyList(), true);
        }

        static SniffResponse buildResponse(String nodesInfoBody, List<HttpHost> hosts) {
            return new SniffResponse(nodesInfoBody, hosts, false);
        }
    }

    private static int randomErrorResponseCode() {
        return RandomInts.randomIntBetween(random(), 400, 599);
    }
}
