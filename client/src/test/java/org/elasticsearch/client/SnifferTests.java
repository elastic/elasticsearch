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
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.LogManager;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class SnifferTests extends LuceneTestCase {

    static {
        //prevent MockWebServer from logging to stdout and stderr
        LogManager.getLogManager().reset();
    }

    private int sniffRequestTimeout;
    private SniffingConnectionPool.Scheme scheme;
    private SniffResponse sniffResponse;
    private MockWebServer server;

    @Before
    public void startMockWebServer() throws IOException {
        this.sniffRequestTimeout = RandomInts.randomIntBetween(random(), 1000, 10000);
        this.scheme = RandomPicks.randomFrom(random(), SniffingConnectionPool.Scheme.values());
        if (rarely()) {
            this.sniffResponse = SniffResponse.buildFailure();
        } else {
            this.sniffResponse = buildSniffResponse(scheme);
        }
        this.server = buildMockWebServer(sniffResponse, sniffRequestTimeout);
        this.server.start();
    }

    @After
    public void stopMockWebServer() throws IOException {
        server.shutdown();
    }

    public void testSniffNodes() throws IOException, URISyntaxException {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        Sniffer sniffer = new Sniffer(client, RequestConfig.DEFAULT, sniffRequestTimeout, scheme.toString());
        HttpHost httpHost = new HttpHost(server.getHostName(), server.getPort());
        try {
            List<HttpHost> sniffedHosts = sniffer.sniffNodes(httpHost);
            if (sniffResponse.isFailure) {
                fail("sniffNodes should have failed");
            }
            assertThat(sniffedHosts.size(), equalTo(sniffResponse.hosts.size()));
            Iterator<HttpHost> responseHostsIterator = sniffResponse.hosts.iterator();
            for (HttpHost sniffedHost : sniffedHosts) {
                assertEquals(sniffedHost, responseHostsIterator.next());
            }
        } catch(ElasticsearchResponseException e) {
            if (sniffResponse.isFailure) {
                assertThat(e.getMessage(), containsString("GET http://localhost:" + server.getPort() +
                        "/_nodes/http?timeout=" + sniffRequestTimeout));
                assertThat(e.getMessage(), containsString(Integer.toString(sniffResponse.nodesInfoResponseCode)));
                assertThat(e.getHost(), equalTo(httpHost));
                assertThat(e.getStatusLine().getStatusCode(), equalTo(sniffResponse.nodesInfoResponseCode));
                assertThat(e.getRequestLine().toString(), equalTo("GET /_nodes/http?timeout=" + sniffRequestTimeout + "ms HTTP/1.1"));
            } else {
                fail("sniffNodes should have succeeded: " + e.getStatusLine());
            }
        }
    }

    private static MockWebServer buildMockWebServer(SniffResponse sniffResponse, int sniffTimeout) throws UnsupportedEncodingException {
        MockWebServer server = new MockWebServer();
        final Dispatcher dispatcher = new Dispatcher() {
            @Override
            public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
                String decodedUrl;
                try {
                    decodedUrl = URLDecoder.decode(request.getPath(), StandardCharsets.UTF_8.name());
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
                String sniffUrl = "/_nodes/http?timeout=" + sniffTimeout + "ms";
                if (sniffUrl.equals(decodedUrl)) {
                    return new MockResponse().setBody(sniffResponse.nodesInfoBody).setResponseCode(sniffResponse.nodesInfoResponseCode);
                } else {
                    return new MockResponse().setResponseCode(404);
                }
            }
        };
        server.setDispatcher(dispatcher);
        return server;
    }

    private static SniffResponse buildSniffResponse(SniffingConnectionPool.Scheme scheme) throws IOException {
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
                HttpHost httpHost = new HttpHost(host, port, scheme.toString());
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
            return new SniffResponse("", Collections.emptyList(), true);
        }

        static SniffResponse buildResponse(String nodesInfoBody, List<HttpHost> hosts) {
            return new SniffResponse(nodesInfoBody, hosts, false);
        }
    }

    private static int randomErrorResponseCode() {
        return RandomInts.randomIntBetween(random(), 400, 599);
    }
}
