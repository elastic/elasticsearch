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

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.elasticsearch.client.RestClient.NodeTuple;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.elasticsearch.client.RestClientTestUtil.getHttpMethods;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RestClientTests extends RestClientTestCase {

    public void testCloseIsIdempotent() throws IOException {
        Node[] nodes = new Node[] {new Node(new HttpHost("localhost", 9200))};
        CloseableHttpAsyncClient closeableHttpAsyncClient = mock(CloseableHttpAsyncClient.class);
        RestClient restClient =  new RestClient(closeableHttpAsyncClient, 1_000, new Header[0], nodes, null, null);
        restClient.close();
        verify(closeableHttpAsyncClient, times(1)).close();
        restClient.close();
        verify(closeableHttpAsyncClient, times(2)).close();
        restClient.close();
        verify(closeableHttpAsyncClient, times(3)).close();
    }

    public void testPerformAsyncWithUnsupportedMethod() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync(new Request("unsupported", randomAsciiLettersOfLength(5)), new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    throw new UnsupportedOperationException("onSuccess cannot be called when using a mocked http client");
                }

                @Override
                public void onFailure(Exception exception) {
                    try {
                        assertThat(exception, instanceOf(UnsupportedOperationException.class));
                        assertEquals("http method not supported: unsupported", exception.getMessage());
                    } finally {
                        latch.countDown();
                    }
                }
            });
            assertTrue("time out waiting for request to return", latch.await(1000, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * @deprecated will remove method in 7.0 but needs tests until then. Replaced by {@link #testPerformAsyncWithUnsupportedMethod()}.
     */
    @Deprecated
    public void testPerformAsyncOldStyleWithUnsupportedMethod() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync("unsupported", randomAsciiLettersOfLength(5), new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    throw new UnsupportedOperationException("onSuccess cannot be called when using a mocked http client");
                }

                @Override
                public void onFailure(Exception exception) {
                    try {
                        assertThat(exception, instanceOf(UnsupportedOperationException.class));
                        assertEquals("http method not supported: unsupported", exception.getMessage());
                    } finally {
                        latch.countDown();
                    }
                }
            });
            assertTrue("time out waiting for request to return", latch.await(1000, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * @deprecated will remove method in 7.0 but needs tests until then. Replaced by {@link RequestTests#testAddParameters()}.
     */
    @Deprecated
    public void testPerformOldStyleAsyncWithNullParams() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync(randomAsciiLettersOfLength(5), randomAsciiLettersOfLength(5), null, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    throw new UnsupportedOperationException("onSuccess cannot be called when using a mocked http client");
                }

                @Override
                public void onFailure(Exception exception) {
                    try {
                        assertThat(exception, instanceOf(NullPointerException.class));
                        assertEquals("parameters cannot be null", exception.getMessage());
                    } finally {
                        latch.countDown();
                    }
                }
            });
            assertTrue("time out waiting for request to return", latch.await(1000, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * @deprecated will remove method in 7.0 but needs tests until then. Replaced by {@link RequestTests#testAddHeader()}.
     */
    @Deprecated
    public void testPerformOldStyleAsyncWithNullHeaders() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (RestClient restClient = createRestClient()) {
            ResponseListener listener = new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    throw new UnsupportedOperationException("onSuccess cannot be called when using a mocked http client");
                }

                @Override
                public void onFailure(Exception exception) {
                    try {
                        assertThat(exception, instanceOf(NullPointerException.class));
                        assertEquals("header cannot be null", exception.getMessage());
                    } finally {
                        latch.countDown();
                    }
                }
            };
            restClient.performRequestAsync("GET", randomAsciiLettersOfLength(5), listener, (Header) null);
            assertTrue("time out waiting for request to return", latch.await(1000, TimeUnit.MILLISECONDS));
        }
    }

    public void testPerformAsyncWithWrongEndpoint() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync(new Request("GET", "::http:///"), new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    throw new UnsupportedOperationException("onSuccess cannot be called when using a mocked http client");
                }

                @Override
                public void onFailure(Exception exception) {
                    try {
                        assertThat(exception, instanceOf(IllegalArgumentException.class));
                        assertEquals("Expected scheme name at index 0: ::http:///", exception.getMessage());
                    } finally {
                        latch.countDown();
                    }
                }
            });
            assertTrue("time out waiting for request to return", latch.await(1000, TimeUnit.MILLISECONDS));
        }
    }

    /**
     * @deprecated will remove method in 7.0 but needs tests until then. Replaced by {@link #testPerformAsyncWithWrongEndpoint()}.
     */
    @Deprecated
    public void testPerformAsyncOldStyleWithWrongEndpoint() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync("GET", "::http:///", new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    throw new UnsupportedOperationException("onSuccess cannot be called when using a mocked http client");
                }

                @Override
                public void onFailure(Exception exception) {
                    try {
                        assertThat(exception, instanceOf(IllegalArgumentException.class));
                        assertEquals("Expected scheme name at index 0: ::http:///", exception.getMessage());
                    } finally {
                        latch.countDown();
                    }
                }
            });
            assertTrue("time out waiting for request to return", latch.await(1000, TimeUnit.MILLISECONDS));
        }
    }

    public void testBuildUriLeavesPathUntouched() {
        {
            URI uri = RestClient.buildUri("/foo$bar", "/index/type/id", Collections.<String, String>emptyMap());
            assertEquals("/foo$bar/index/type/id", uri.getPath());
        }
        {
            URI uri = RestClient.buildUri(null, "/foo$bar/ty/pe/i/d", Collections.<String, String>emptyMap());
            assertEquals("/foo$bar/ty/pe/i/d", uri.getPath());
        }
        {
            URI uri = RestClient.buildUri(null, "/index/type/id", Collections.singletonMap("foo$bar", "x/y/z"));
            assertEquals("/index/type/id", uri.getPath());
            assertEquals("foo$bar=x/y/z", uri.getQuery());
        }
    }

    public void testSetNodesWrongArguments() throws IOException {
        try (RestClient restClient = createRestClient()) {
            restClient.setNodes((Node[]) null);
            fail("setNodes should have failed");
        } catch (IllegalArgumentException e) {
            assertEquals("nodes must not be null or empty", e.getMessage());
        }
        try (RestClient restClient = createRestClient()) {
            restClient.setNodes();
            fail("setNodes should have failed");
        } catch (IllegalArgumentException e) {
            assertEquals("nodes must not be null or empty", e.getMessage());
        }
        try (RestClient restClient = createRestClient()) {
            restClient.setNodes((Node) null);
            fail("setNodes should have failed");
        } catch (NullPointerException e) {
            assertEquals("node cannot be null", e.getMessage());
        }
        try (RestClient restClient = createRestClient()) {
            restClient.setNodes(new Node(new HttpHost("localhost", 9200)), null, new Node(new HttpHost("localhost", 9201)));
            fail("setNodes should have failed");
        } catch (NullPointerException e) {
            assertEquals("node cannot be null", e.getMessage());
        }
    }

    public void testSetHostsPreservesOrdering() throws Exception {
        try (RestClient restClient = createRestClient()) {
            Node[] hosts = randomNodes();
            restClient.setNodes(hosts);
            assertEquals(Arrays.asList(hosts), restClient.getNodes());
        }
    }

    private static Node[] randomNodes() {
        int numNodes = randomIntBetween(1, 10);
        Node[] nodes = new Node[numNodes];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new Node(new HttpHost("host-" + i, 9200));
        }
        return nodes;
    }

    public void testSetNodesDuplicatedHosts() throws Exception {
        try (RestClient restClient = createRestClient()) {
            int numNodes = randomIntBetween(1, 10);
            Node[] nodes = new Node[numNodes];
            Node node = new Node(new HttpHost("host", 9200));
            for (int i = 0; i < nodes.length; i++) {
                nodes[i] = node;
            }
            restClient.setNodes(nodes);
            assertEquals(1, restClient.getNodes().size());
            assertEquals(node, restClient.getNodes().get(0));
        }
    }

    /**
     * @deprecated will remove method in 7.0 but needs tests until then. Replaced by {@link RequestTests#testConstructor()}.
     */
    @Deprecated
    public void testNullPath() throws IOException {
        try (RestClient restClient = createRestClient()) {
            for (String method : getHttpMethods()) {
                try {
                    restClient.performRequest(method, null);
                    fail("path set to null should fail!");
                } catch (NullPointerException e) {
                    assertEquals("endpoint cannot be null", e.getMessage());
                }
            }
        }
    }

    public void testSelectHosts() throws IOException {
        int iterations = 1000;
        Node n1 = new Node(new HttpHost("1"), null, null, "1", null);
        Node n2 = new Node(new HttpHost("2"), null, null, "2", null);
        Node n3 = new Node(new HttpHost("3"), null, null, "3", null);
        List<Node> nodes = Arrays.asList(n1, n2, n3);

        NodeSelector not1 = new NodeSelector() {
            @Override
            public List<Node> select(List<Node> nodes) {
                List<Node> result = new ArrayList<>();
                for (Node node : nodes) {
                    if (false == "1".equals(node.getVersion())) {
                        result.add(node);
                    }
                }
                return result;
            }

            @Override
            public String toString() {
                return "NOT 1";
            }
        };
        NodeSelector noNodes = new NodeSelector() {
            @Override
            public List<Node> select(List<Node> nodes) {
                return Collections.<Node>emptyList();
            }

            @Override
            public String toString() {
                return "NONE";
            }
        };

        NodeTuple<List<Node>> nodeTuple = new NodeTuple<>(nodes, null);
        Map<HttpHost, DeadHostState> blacklist = new HashMap<>();
        AtomicInteger lastNodeIndex = new AtomicInteger(0);
        long now = 0;

        // Normal case
        List<Node> expectedNodes = Arrays.asList(n1, n2, n3);
        assertEquals(expectedNodes, RestClient.selectHosts(nodeTuple, blacklist,
                lastNodeIndex, now, NodeSelector.ANY));
        // Calling it again rotates the set of results
        for (int i = 0; i < iterations; i++) {
            Collections.rotate(expectedNodes, 1);
            assertEquals(expectedNodes, RestClient.selectHosts(nodeTuple, blacklist,
                    lastNodeIndex, now, NodeSelector.ANY));
        }

        // Exclude some node
        lastNodeIndex.set(0);
        // h1 excluded
        assertEquals(Arrays.asList(n2, n3), RestClient.selectHosts(nodeTuple, blacklist,
                lastNodeIndex, now, not1));
        // Calling it again rotates the set of results
        assertEquals(Arrays.asList(n3, n2), RestClient.selectHosts(nodeTuple, blacklist,
                lastNodeIndex, now, not1));
        // And again, same
        assertEquals(Arrays.asList(n2, n3), RestClient.selectHosts(nodeTuple, blacklist,
                lastNodeIndex, now, not1));
        /*
         * But this time it doesn't because the list being filtered changes
         * from (h1, h2, h3) to (h2, h3, h1) which both look the same when
         * you filter out h1.
         */
        assertEquals(Arrays.asList(n2, n3), RestClient.selectHosts(nodeTuple, blacklist,
                lastNodeIndex, now, not1));

        /*
         * Try a NodeSelector that excludes all nodes. This should
         * throw an exception
         */
        lastNodeIndex.set(0);
        try {
            RestClient.selectHosts(nodeTuple, blacklist, lastNodeIndex, now, noNodes);
            fail("expected selectHosts to fail");
        } catch (IOException e) {
            String message = "NodeSelector [NONE] rejected all nodes, living ["
                    + "[host=http://1, version=1], [host=http://2, version=2], "
                    + "[host=http://3, version=3]] and dead []";
            assertEquals(message, e.getMessage());
        }

        /*
         * Mark all nodes as dead and look up at a time *after* the
         * revival time. This should return all nodes.
         */
        blacklist.put(n1.getHost(), new DeadHostState(1));
        blacklist.put(n2.getHost(), new DeadHostState(new DeadHostState(1), 1));
        blacklist.put(n3.getHost(), new DeadHostState(new DeadHostState(new DeadHostState(1), 1), 1));
        lastNodeIndex.set(0);
        now = DeadHostState.MAX_CONNECTION_TIMEOUT_NANOS + 1;
        expectedNodes = Arrays.asList(n1, n2, n3);
        assertEquals(expectedNodes, RestClient.selectHosts(nodeTuple, blacklist, lastNodeIndex,
                now, NodeSelector.ANY));
        // Calling it again rotates the set of results
        for (int i = 0; i < iterations; i++) {
            Collections.rotate(expectedNodes, 1);
            assertEquals(expectedNodes, RestClient.selectHosts(nodeTuple, blacklist,
                    lastNodeIndex, now, NodeSelector.ANY));
        }

        /*
         * Now try with the nodes dead and *not* past their dead time.
         * Only the node closest to revival should come back.
         */
        now = 0;
        assertEquals(singletonList(n1), RestClient.selectHosts(nodeTuple, blacklist, lastNodeIndex,
                now, NodeSelector.ANY));

        /*
         * Now try with the nodes dead and *not* past their dead time
         * *and* a node selector that removes the node that is closest
         * to being revived. The second closest node should come back.
         */
        assertEquals(singletonList(n2), RestClient.selectHosts(nodeTuple, blacklist,
                lastNodeIndex, now, not1));

        /*
         * Try a NodeSelector that excludes all nodes. This should
         * return a failure, but a different failure than normal
         * because it'll block revival rather than outright reject
         * healthy nodes.
         */
        lastNodeIndex.set(0);
        try {
            RestClient.selectHosts(nodeTuple, blacklist, lastNodeIndex, now, noNodes);
            fail("expected selectHosts to fail");
        } catch (IOException e) {
            String message = "NodeSelector [NONE] rejected all nodes, living [] and dead ["
                    + "[host=http://1, version=1], [host=http://2, version=2], "
                    + "[host=http://3, version=3]]";
            assertEquals(message, e.getMessage());
        }
    }

    private static RestClient createRestClient() {
        Node[] nodes = new Node[] {new Node(new HttpHost("localhost", 9200))};
        return new RestClient(mock(CloseableHttpAsyncClient.class), randomLongBetween(1_000, 30_000),
                new Header[] {}, nodes, null, null);
    }


}
