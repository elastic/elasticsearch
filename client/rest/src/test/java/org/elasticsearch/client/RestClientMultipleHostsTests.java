/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import static org.elasticsearch.client.NodePriorityStrategy.NO_PRIORITY;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.elasticsearch.client.RestClientTestUtil.randomErrorNoRetryStatusCode;
import static org.elasticsearch.client.RestClientTestUtil.randomErrorRetryStatusCode;
import static org.elasticsearch.client.RestClientTestUtil.randomHttpMethod;
import static org.elasticsearch.client.RestClientTestUtil.randomOkStatusCode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RestClient} behaviour against multiple hosts: fail-over, blacklisting etc.
 * Relies on a mock http client to intercept requests and return desired responses based on request path.
 */
public class RestClientMultipleHostsTests extends RestClientTestCase {

    private ExecutorService exec = Executors.newFixedThreadPool(1);
    private List<Node> nodes;
    private HostsTrackingFailureListener failureListener;

    public RestClient createRestClient(NodeSelector nodeSelector) {
        return createRestClient(nodeSelector, NO_PRIORITY);
    }

    public RestClient createRestClient(NodeSelector nodeSelector, NodePriorityStrategy nodePriorityStrategy) {
        CloseableHttpAsyncClient httpClient = RestClientSingleHostTests.mockHttpClient(exec);
        int numNodes = RandomNumbers.randomIntBetween(getRandom(), 2, 5);
        nodes = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; i++) {
            nodes.add(new Node(new HttpHost("localhost", 9200 + i)));
        }
        nodes = Collections.unmodifiableList(nodes);
        failureListener = new HostsTrackingFailureListener();
        return new RestClient(httpClient, new Header[0], nodes, null, failureListener, nodeSelector, nodePriorityStrategy, false, false);
    }

    /**
     * Shutdown the executor so we don't leak threads into other test runs.
     */
    @After
    public void shutdownExec() {
        exec.shutdown();
    }

    public void testRoundRobinOkStatusCodes() throws Exception {
        RestClient restClient = createRestClient(NodeSelector.ANY);
        int numIters = RandomNumbers.randomIntBetween(getRandom(), 1, 5);
        for (int i = 0; i < numIters; i++) {
            Set<HttpHost> hostsSet = hostsSet();
            for (int j = 0; j < nodes.size(); j++) {
                int statusCode = randomOkStatusCode(getRandom());
                Response response = RestClientSingleHostTests.performRequestSyncOrAsync(restClient,
                    new Request(randomHttpMethod(getRandom()), "/" + statusCode));
                assertEquals(statusCode, response.getStatusLine().getStatusCode());
                assertTrue("host not found: " + response.getHost(), hostsSet.remove(response.getHost()));
            }
            assertEquals("every host should have been used but some weren't: " + hostsSet, 0, hostsSet.size());
        }
        failureListener.assertNotCalled();
    }

    public void testRoundRobinNoRetryErrors() throws Exception {
        RestClient restClient = createRestClient(NodeSelector.ANY);
        int numIters = RandomNumbers.randomIntBetween(getRandom(), 1, 5);
        for (int i = 0; i < numIters; i++) {
            Set<HttpHost> hostsSet = hostsSet();
            for (int j = 0; j < nodes.size(); j++) {
                String method = randomHttpMethod(getRandom());
                int statusCode = randomErrorNoRetryStatusCode(getRandom());
                try {
                    Response response = RestClientSingleHostTests.performRequestSyncOrAsync(restClient,
                        new Request(method, "/" + statusCode));
                    if (method.equals("HEAD") && statusCode == 404) {
                        //no exception gets thrown although we got a 404
                        assertEquals(404, response.getStatusLine().getStatusCode());
                        assertEquals(statusCode, response.getStatusLine().getStatusCode());
                        assertTrue("host not found: " + response.getHost(), hostsSet.remove(response.getHost()));
                    } else {
                        fail("request should have failed");
                    }
                } catch (ResponseException e) {
                    if (method.equals("HEAD") && statusCode == 404) {
                        throw e;
                    }
                    Response response = e.getResponse();
                    assertEquals(statusCode, response.getStatusLine().getStatusCode());
                    assertTrue("host not found: " + response.getHost(), hostsSet.remove(response.getHost()));
                    assertEquals(0, e.getSuppressed().length);
                }
            }
            assertEquals("every host should have been used but some weren't: " + hostsSet, 0, hostsSet.size());
        }
        failureListener.assertNotCalled();
    }

    public void testRoundRobinRetryErrors() throws Exception {
        RestClient restClient = createRestClient(NodeSelector.ANY);
        String retryEndpoint = randomErrorRetryEndpoint();
        try  {
            RestClientSingleHostTests.performRequestSyncOrAsync(restClient, new Request(randomHttpMethod(getRandom()), retryEndpoint));
            fail("request should have failed");
        } catch (ResponseException e) {
            Set<HttpHost> hostsSet = hostsSet();
            //first request causes all the hosts to be blacklisted, the returned exception holds one suppressed exception each
            failureListener.assertCalled(nodes);
            do {
                Response response = e.getResponse();
                assertEquals(Integer.parseInt(retryEndpoint.substring(1)), response.getStatusLine().getStatusCode());
                assertTrue("host [" + response.getHost() + "] not found, most likely used multiple times",
                        hostsSet.remove(response.getHost()));
                if (e.getSuppressed().length > 0) {
                    assertEquals(1, e.getSuppressed().length);
                    Throwable suppressed = e.getSuppressed()[0];
                    assertThat(suppressed, instanceOf(ResponseException.class));
                    e = (ResponseException)suppressed;
                } else {
                    e = null;
                }
            } while(e != null);
            assertEquals("every host should have been used but some weren't: " + hostsSet, 0, hostsSet.size());
        } catch (IOException e) {
            Set<HttpHost> hostsSet = hostsSet();
            //first request causes all the hosts to be blacklisted, the returned exception holds one suppressed exception each
            failureListener.assertCalled(nodes);
            do {
                HttpHost httpHost = HttpHost.create(e.getMessage());
                assertTrue("host [" + httpHost + "] not found, most likely used multiple times", hostsSet.remove(httpHost));
                if (e.getSuppressed().length > 0) {
                    assertEquals(1, e.getSuppressed().length);
                    Throwable suppressed = e.getSuppressed()[0];
                    assertThat(suppressed, instanceOf(IOException.class));
                    e = (IOException) suppressed;
                } else {
                    e = null;
                }
            } while(e != null);
            assertEquals("every host should have been used but some weren't: " + hostsSet, 0, hostsSet.size());
        }

        int numIters = RandomNumbers.randomIntBetween(getRandom(), 2, 5);
        for (int i = 1; i <= numIters; i++) {
            //check that one different host is resurrected at each new attempt
            Set<HttpHost> hostsSet = hostsSet();
            for (int j = 0; j < nodes.size(); j++) {
                retryEndpoint = randomErrorRetryEndpoint();
                try  {
                    RestClientSingleHostTests.performRequestSyncOrAsync(restClient,
                        new Request(randomHttpMethod(getRandom()), retryEndpoint));
                    fail("request should have failed");
                } catch (ResponseException e) {
                    Response response = e.getResponse();
                    assertThat(response.getStatusLine().getStatusCode(), equalTo(Integer.parseInt(retryEndpoint.substring(1))));
                    assertTrue("host [" + response.getHost() + "] not found, most likely used multiple times",
                            hostsSet.remove(response.getHost()));
                    //after the first request, all hosts are blacklisted, a single one gets resurrected each time
                    failureListener.assertCalled(response.getHost());
                    assertEquals(0, e.getSuppressed().length);
                } catch (IOException e) {
                    HttpHost httpHost = HttpHost.create(e.getMessage());
                    assertTrue("host [" + httpHost + "] not found, most likely used multiple times", hostsSet.remove(httpHost));
                    //after the first request, all hosts are blacklisted, a single one gets resurrected each time
                    failureListener.assertCalled(httpHost);
                    assertEquals(0, e.getSuppressed().length);
                }
            }
            assertEquals("every host should have been used but some weren't: " + hostsSet, 0, hostsSet.size());
            if (getRandom().nextBoolean()) {
                //mark one host back alive through a successful request and check that all requests after that are sent to it
                HttpHost selectedHost = null;
                int iters = RandomNumbers.randomIntBetween(getRandom(), 2, 10);
                for (int y = 0; y < iters; y++) {
                    int statusCode = randomErrorNoRetryStatusCode(getRandom());
                    Response response;
                    try {
                        response = RestClientSingleHostTests.performRequestSyncOrAsync(restClient,
                            new Request(randomHttpMethod(getRandom()), "/" + statusCode));
                    } catch (ResponseException e) {
                        response = e.getResponse();
                    }
                    assertThat(response.getStatusLine().getStatusCode(), equalTo(statusCode));
                    if (selectedHost == null) {
                        selectedHost = response.getHost();
                    } else {
                        assertThat(response.getHost(), equalTo(selectedHost));
                    }
                }
                failureListener.assertNotCalled();
                //let the selected host catch up on number of failures, it gets selected a consecutive number of times as it's the one
                //selected to be retried earlier (due to lower number of failures) till all the hosts have the same number of failures
                for (int y = 0; y < i + 1; y++) {
                    retryEndpoint = randomErrorRetryEndpoint();
                    try {
                        RestClientSingleHostTests.performRequestSyncOrAsync(restClient,
                            new Request(randomHttpMethod(getRandom()), retryEndpoint));
                        fail("request should have failed");
                    } catch (ResponseException e) {
                        Response response = e.getResponse();
                        assertThat(response.getStatusLine().getStatusCode(), equalTo(Integer.parseInt(retryEndpoint.substring(1))));
                        assertThat(response.getHost(), equalTo(selectedHost));
                        failureListener.assertCalled(selectedHost);
                    } catch(IOException e) {
                        HttpHost httpHost = HttpHost.create(e.getMessage());
                        assertThat(httpHost, equalTo(selectedHost));
                        failureListener.assertCalled(selectedHost);
                    }
                }
            }
        }
    }

    public void testNodeSelector() throws Exception {
        NodeSelector firstPositionOnly = restClientNodes -> {
            boolean found = false;
            for (Iterator<Node> itr = restClientNodes.iterator(); itr.hasNext();) {
                if (nodes.get(0) == itr.next()) {
                    found = true;
                } else {
                    itr.remove();
                }
            }
            assertTrue(found);
        };
        RestClient restClient = createRestClient(firstPositionOnly);
        int rounds = between(1, 10);
        for (int i = 0; i < rounds; i++) {
            /*
             * Run the request more than once to verify that the
             * NodeSelector overrides the round robin behavior.
             */
            Request request = new Request("GET", "/200");
            Response response = RestClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(nodes.get(0).getHost(), response.getHost());
        }
    }

    public void testNodePriorityStrategy() throws Exception {
        NodePriorityStrategy firstPositionPriority = restClientNodes -> {
            List<Node> highPriority = new ArrayList<>();
            highPriority.add(restClientNodes.get(0));
            List<Node> lowPriority = new ArrayList<>(restClientNodes.subList(1, restClientNodes.size() - 1));
            List<List<Node>> groups = new ArrayList<>();
            groups.add(highPriority);
            groups.add(lowPriority);
            return groups;
        };
        RestClient restClient = createRestClient(NodeSelector.ANY, firstPositionPriority);
        int rounds = between(1, 10);
        for (int i = 0; i < rounds; i++) {
            /*
             * Run the request more than once to verify that the
             * NodeSelector overrides the round robin behavior.
             */
            Request request = new Request("GET", "/200");
            Response response = RestClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(nodes.get(0).getHost(), response.getHost());
        }
    }

    public void testDefaultNodePriorityStrategy() throws Exception {
        RestClient restClient = createRestClient(NodeSelector.ANY, NodePriorityStrategy.NO_PRIORITY);
        int rounds = between(1, 10);
        int distance = 0;

        for (int i = 0; i < rounds; i++) {
            /*
             * Run the request more than once to verify that the
             * default NodePriorityStrategy uses the round robin behavior.
             */
            Request request = new Request("GET", "/200");
            Response response = RestClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            List<Node> rotatedNodes = new ArrayList<>(nodes);
            Collections.rotate(rotatedNodes, distance++);
            assertEquals(rotatedNodes.get(0).getHost(), response.getHost());
        }
    }

    public void testSetNodes() throws Exception {
        RestClient restClient = createRestClient(NodeSelector.SKIP_DEDICATED_MASTERS);
        List<Node> newNodes = new ArrayList<>(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            Node.Roles roles = i == 0 ?
                new Node.Roles(new TreeSet<>(Arrays.asList("data", "ingest"))) :
                new Node.Roles(new TreeSet<>(Arrays.asList("master")));
            newNodes.add(new Node(nodes.get(i).getHost(), null, null, null, roles, null));
        }
        restClient.setNodes(newNodes);
        int rounds = between(1, 10);
        for (int i = 0; i < rounds; i++) {
            /*
             * Run the request more than once to verify that the
             * NodeSelector overrides the round robin behavior.
             */
            Request request = new Request("GET", "/200");
            Response response = RestClientSingleHostTests.performRequestSyncOrAsync(restClient, request);
            assertEquals(newNodes.get(0).getHost(), response.getHost());
        }
    }

    private static String randomErrorRetryEndpoint() {
        switch(RandomNumbers.randomIntBetween(getRandom(), 0, 3)) {
            case 0:
                return "/" + randomErrorRetryStatusCode(getRandom());
            case 1:
                return "/coe";
            case 2:
                return "/soe";
            case 3:
                return "/ioe";
        }
        throw new UnsupportedOperationException();
    }

    /**
     * Build a mutable {@link Set} containing all the {@link Node#getHost() hosts}
     * in use by the test.
     */
    private Set<HttpHost> hostsSet() {
        Set<HttpHost> hosts = new HashSet<>();
        for (Node node : nodes) {
            hosts.add(node.getHost());
        }
        return hosts;
    }
}
