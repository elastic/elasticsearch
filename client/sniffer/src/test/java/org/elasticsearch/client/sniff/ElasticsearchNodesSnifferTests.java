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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
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
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientTestCase;
import org.elasticsearch.mocksocket.MockHttpServer;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ElasticsearchNodesSnifferTests extends RestClientTestCase {

    private int sniffRequestTimeout;
    private ElasticsearchNodesSniffer.Scheme scheme;
    private SniffResponse sniffResponse;
    private HttpServer httpServer;

    @Before
    public void startHttpServer() throws IOException {
        this.sniffRequestTimeout = RandomNumbers.randomIntBetween(getRandom(), 1000, 10000);
        this.scheme = RandomPicks.randomFrom(getRandom(), ElasticsearchNodesSniffer.Scheme.values());
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

    public void testConstructorValidation() throws IOException {
        try {
            new ElasticsearchNodesSniffer(null, 1, ElasticsearchNodesSniffer.Scheme.HTTP);
            fail("should have failed");
        } catch(NullPointerException e) {
            assertEquals("restClient cannot be null", e.getMessage());
        }
        HttpHost httpHost = new HttpHost(httpServer.getAddress().getHostString(), httpServer.getAddress().getPort());
        try (RestClient restClient = RestClient.builder(httpHost).build()) {
            try {
                new ElasticsearchNodesSniffer(restClient, 1, null);
                fail("should have failed");
            } catch (NullPointerException e) {
                assertEquals(e.getMessage(), "scheme cannot be null");
            }
            try {
                new ElasticsearchNodesSniffer(restClient, RandomNumbers.randomIntBetween(getRandom(), Integer.MIN_VALUE, 0),
                        ElasticsearchNodesSniffer.Scheme.HTTP);
                fail("should have failed");
            } catch (IllegalArgumentException e) {
                assertEquals(e.getMessage(), "sniffRequestTimeoutMillis must be greater than 0");
            }
        }
    }

    public void testSniffNodes() throws IOException {
        HttpHost httpHost = new HttpHost(httpServer.getAddress().getHostString(), httpServer.getAddress().getPort());
        try (RestClient restClient = RestClient.builder(httpHost).build()) {
            ElasticsearchNodesSniffer sniffer = new ElasticsearchNodesSniffer(restClient, sniffRequestTimeout, scheme);
            try {
                List<Node> sniffedNodes = sniffer.sniff();
                if (sniffResponse.isFailure) {
                    fail("sniffNodes should have failed");
                }
                assertEquals(sniffResponse.result, sniffedNodes);
            } catch(ResponseException e) {
                Response response = e.getResponse();
                if (sniffResponse.isFailure) {
                    final String errorPrefix = "method [GET], host [" + httpHost + "], URI [/_nodes/http?timeout=" + sniffRequestTimeout
                        + "ms], status line [HTTP/1.1";
                    assertThat(e.getMessage(), startsWith(errorPrefix));
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

    private static HttpServer createHttpServer(final SniffResponse sniffResponse, final int sniffTimeoutMillis) throws IOException {
        HttpServer httpServer = MockHttpServer.createHttp(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        httpServer.createContext("/_nodes/http", new ResponseHandler(sniffTimeoutMillis, sniffResponse));
        return httpServer;
    }

    private static class ResponseHandler implements HttpHandler {
        private final int sniffTimeoutMillis;
        private final SniffResponse sniffResponse;

        ResponseHandler(int sniffTimeoutMillis, SniffResponse sniffResponse) {
            this.sniffTimeoutMillis = sniffTimeoutMillis;
            this.sniffResponse = sniffResponse;
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            if (httpExchange.getRequestMethod().equals(HttpGet.METHOD_NAME)) {
                if (httpExchange.getRequestURI().getRawQuery().equals("timeout=" + sniffTimeoutMillis + "ms")) {
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
    }

    private static SniffResponse buildSniffResponse(ElasticsearchNodesSniffer.Scheme scheme) throws IOException {
        int numNodes = RandomNumbers.randomIntBetween(getRandom(), 1, 5);
        List<Node> nodes = new ArrayList<>(numNodes);
        JsonFactory jsonFactory = new JsonFactory();
        StringWriter writer = new StringWriter();
        JsonGenerator generator = jsonFactory.createGenerator(writer);
        generator.writeStartObject();
        if (getRandom().nextBoolean()) {
            generator.writeStringField("cluster_name", "elasticsearch");
        }
        if (getRandom().nextBoolean()) {
            generator.writeObjectFieldStart("bogus_object");
            generator.writeEndObject();
        }
        generator.writeObjectFieldStart("nodes");
        for (int i = 0; i < numNodes; i++) {
            String nodeId = RandomStrings.randomAsciiOfLengthBetween(getRandom(), 5, 10);
            String host = "host" + i;
            int port = RandomNumbers.randomIntBetween(getRandom(), 9200, 9299);
            HttpHost publishHost = new HttpHost(host, port, scheme.toString());
            Set<HttpHost> boundHosts = new HashSet<>();
            boundHosts.add(publishHost);

            if (randomBoolean()) {
                int bound = between(1, 5);
                for (int b = 0; b < bound; b++) {
                    boundHosts.add(new HttpHost(host + b, port, scheme.toString()));
                }
            }

            int numAttributes = between(0, 5);
            Map<String, List<String>> attributes = new HashMap<>(numAttributes);
            for (int j = 0; j < numAttributes; j++) {
                int numValues = frequently() ? 1 : between(2, 5);
                List<String> values = new ArrayList<>();
                for (int v = 0; v < numValues; v++) {
                    values.add(j + "value" + v);
                }
                attributes.put("attr" + j, values);
            }

            Node node = new Node(publishHost, boundHosts, randomAsciiAlphanumOfLength(5),
                    randomAsciiAlphanumOfLength(5),
                    new Node.Roles(randomBoolean(), randomBoolean(), randomBoolean()),
                    attributes);

            generator.writeObjectFieldStart(nodeId);
            if (getRandom().nextBoolean()) {
                generator.writeObjectFieldStart("bogus_object");
                generator.writeEndObject();
            }
            if (getRandom().nextBoolean()) {
                generator.writeArrayFieldStart("bogus_array");
                generator.writeStartObject();
                generator.writeEndObject();
                generator.writeEndArray();
            }
            boolean isHttpEnabled = rarely() == false;
            if (isHttpEnabled) {
                nodes.add(node);
                generator.writeObjectFieldStart("http");
                generator.writeArrayFieldStart("bound_address");
                for (HttpHost bound : boundHosts) {
                    generator.writeString(bound.toHostString());
                }
                generator.writeEndArray();
                if (getRandom().nextBoolean()) {
                    generator.writeObjectFieldStart("bogus_object");
                    generator.writeEndObject();
                }
                generator.writeStringField("publish_address", publishHost.toHostString());
                if (getRandom().nextBoolean()) {
                    generator.writeNumberField("max_content_length_in_bytes", 104857600);
                }
                generator.writeEndObject();
            }

            List<String> roles = Arrays.asList(new String[] {"master", "data", "ingest"});
            Collections.shuffle(roles, getRandom());
            generator.writeArrayFieldStart("roles");
            for (String role : roles) {
                if ("master".equals(role) && node.getRoles().isMasterEligible()) {
                    generator.writeString("master");
                }
                if ("data".equals(role) && node.getRoles().isData()) {
                    generator.writeString("data");
                }
                if ("ingest".equals(role) && node.getRoles().isIngest()) {
                    generator.writeString("ingest");
                }
            }
            generator.writeEndArray();

            generator.writeFieldName("version");
            generator.writeString(node.getVersion());
            generator.writeFieldName("name");
            generator.writeString(node.getName());

            if (numAttributes > 0) {
                generator.writeObjectFieldStart("attributes");
                for (Map.Entry<String, List<String>> entry : attributes.entrySet()) {
                    if (entry.getValue().size() == 1) {
                        generator.writeStringField(entry.getKey(), entry.getValue().get(0));
                    } else {
                        for (int v = 0; v < entry.getValue().size(); v++) {
                            generator.writeStringField(entry.getKey() + "." + v, entry.getValue().get(v));
                        }
                    }
                }
                generator.writeEndObject();
            }
            generator.writeEndObject();
        }
        generator.writeEndObject();
        generator.writeEndObject();
        generator.close();
        return SniffResponse.buildResponse(writer.toString(), nodes);
    }

    private static class SniffResponse {
        private final String nodesInfoBody;
        private final int nodesInfoResponseCode;
        private final List<Node> result;
        private final boolean isFailure;

        SniffResponse(String nodesInfoBody, List<Node> result, boolean isFailure) {
            this.nodesInfoBody = nodesInfoBody;
            this.result = result;
            this.isFailure = isFailure;
            if (isFailure) {
                this.nodesInfoResponseCode = randomErrorResponseCode();
            } else {
                this.nodesInfoResponseCode = 200;
            }
        }

        static SniffResponse buildFailure() {
            return new SniffResponse("", Collections.<Node>emptyList(), true);
        }

        static SniffResponse buildResponse(String nodesInfoBody, List<Node> nodes) {
            return new SniffResponse(nodesInfoBody, nodes, false);
        }
    }

    private static int randomErrorResponseCode() {
        return RandomNumbers.randomIntBetween(getRandom(), 400, 599);
    }
}
