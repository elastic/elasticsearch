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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
public class RestClientTests extends LuceneTestCase {
    //TODO this should be refactored into a base test!!
    HttpServer server;
    protected String clusterName = "elasticsearch";
    protected List<String> additionalNodes = Collections.emptyList();


    public void setUp() throws Exception {
        super.setUp();
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.setExecutor(null); // creates a default executor
        server.start();
        server.createContext("/", (t) -> {
            handle("/", t);
        });
        server.createContext("/_cat/nodes", (t) -> {
            handle("/_cat/nodes", t);
        });
        server.createContext("/_cat/health", (t) -> {
            handle("/_cat/health", t);
        });
    }

    protected void handle(String path, HttpExchange t) throws IOException {
        final String response;
        switch (path) {
            case "/":
                response = "{}";
                break;
            case "/_cat/nodes":
                StringBuilder builder = new StringBuilder( "127.0.0.1:" + server.getAddress().getPort()  + " " + "d\n");
                for (String host : additionalNodes) {
                    builder.append(host).append("\n");
                }
                response = builder.toString();
                break;
            case "/_cat/health":
                response = clusterName;
                break;
            default:
                throw new IllegalArgumentException("no such handler " + path);
        }
        t.sendResponseHeaders(200, response.length());
        OutputStream os = t.getResponseBody();
        os.write(response.getBytes());
        os.close();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        server.stop(0);
    }

    public void testGetClustername() throws IOException {
        HttpHost httpHost = new HttpHost("127.0.0.1", server.getAddress().getPort(), "http");
        try(RestClient client = new RestClient(httpHost)) {
            assertEquals(clusterName, client.getClusterName(httpHost));
        }
    }

    public void testFetchNodes() throws IOException {
        additionalNodes = Arrays.asList("127.0.0.2:9200 c", "127.0.0.3:9200 d");
        HttpHost httpHost = new HttpHost("127.0.0.1", server.getAddress().getPort(), "http");
        try(RestClient client = new RestClient(httpHost)) {
            assertEquals(3, client.fetchNodes(httpHost, true, true, false).size());
            assertTrue(client.fetchNodes(httpHost, true, true, false).toString(), client.fetchNodes(httpHost, true, true, false).contains(new HttpHost("127.0.0.2", 9200, "http")));
            assertTrue(client.fetchNodes(httpHost, true, true, false).contains(new HttpHost("127.0.0.3", 9200, "http")));
            assertTrue(client.fetchNodes(httpHost, true, true, false).contains(httpHost));
            assertEquals(1, client.fetchNodes(httpHost, true, true, true).size());
        }
    }

    public void testSimpleRetry() throws IOException{
        additionalNodes = Arrays.asList("127.0.0.2:9200 c", "127.0.0.3:9200 d");
        HttpHost httpHost = new HttpHost("127.0.0.1", server.getAddress().getPort(), "http");
        try(RestClient client = new RestClient(httpHost)) {
            client.setNodes(client.fetchNodes(httpHost, true, true, false));
            HttpResponse httpResponse = client.httpGet("/_cat/health", Collections.emptyMap());
            assertEquals(httpResponse.getStatusLine().getStatusCode(), 200);
            server.stop(0);
            try {
                client.httpGet("/_cat/health", Collections.emptyMap());
                fail();
            } catch (IOException ex) {
                assertTrue(ex.getMessage(), ex.getMessage().endsWith("failed: connect timed out") || ex.getMessage().endsWith("failed: Connection refused"));
            }
        }
    }

    public void testBlacklist() throws IOException{
        additionalNodes = Arrays.asList("127.0.0.2:9200 c", "127.0.0.3:9200 d");
        HttpHost httpHost = new HttpHost("127.0.0.1", server.getAddress().getPort(), "http");
        try(RestClient client = new RestClient(httpHost)) {
            client.setNodes(client.fetchNodes(httpHost, true, true, false));
            assertEquals(3, client.getNumHosts());
            assertEquals(0, client.getNumBlacklistedHosts());
            server.stop(0);
            try {
                client.httpGet("/_cat/health", Collections.emptyMap());
                fail();
            } catch (IOException ex) {
                assertTrue(ex.getMessage(), ex.getMessage().endsWith("failed: connect timed out") || ex.getMessage().endsWith("failed: Connection refused"));
            }
            assertEquals(3, client.getNumHosts());
            assertEquals(3, client.getNumBlacklistedHosts());
            int num = 0;
            for (HttpHost host : client.getHostIterator(false)) {
                num++; // nothing here
            }
            assertEquals(0, num);
            for (HttpHost host : client.getHostIterator(true)) {
                num++; // all there - we have to retry now
            }
            assertEquals(3, num);
        }
    }


}
