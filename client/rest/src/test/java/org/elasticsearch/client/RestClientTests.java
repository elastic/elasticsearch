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
import org.elasticsearch.client.HostMetadata.HostMetadataResolver;
import org.elasticsearch.client.RestClient.HostTuple;
import org.elasticsearch.client.RestClient.NextHostsResult;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RestClientTests extends RestClientTestCase {

    public void testCloseIsIdempotent() throws IOException {
        HttpHost[] hosts = new HttpHost[]{new HttpHost("localhost", 9200)};
        CloseableHttpAsyncClient closeableHttpAsyncClient = mock(CloseableHttpAsyncClient.class);
        RestClient restClient = new RestClient(closeableHttpAsyncClient, 1_000, new Header[0],
                hosts, HostMetadata.EMPTY_RESOLVER, null, null);
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
            restClient.performRequestAsync("unsupported", randomAsciiLettersOfLength(5), new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    fail("should have failed because of unsupported method");
                }

                @Override
                public void onFailure(Exception exception) {
                    assertThat(exception, instanceOf(UnsupportedOperationException.class));
                    assertEquals("http method not supported: unsupported", exception.getMessage());
                    latch.countDown();
                }
            });
            latch.await();
        }
    }

    public void testPerformAsyncWithNullParams() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync(randomAsciiLettersOfLength(5), randomAsciiLettersOfLength(5), null, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    fail("should have failed because of null parameters");
                }

                @Override
                public void onFailure(Exception exception) {
                    assertThat(exception, instanceOf(NullPointerException.class));
                    assertEquals("params must not be null", exception.getMessage());
                    latch.countDown();
                }
            });
            latch.await();
        }
    }

    public void testPerformAsyncWithNullHeaders() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (RestClient restClient = createRestClient()) {
            ResponseListener listener = new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    fail("should have failed because of null headers");
                }

                @Override
                public void onFailure(Exception exception) {
                    assertThat(exception, instanceOf(NullPointerException.class));
                    assertEquals("request header must not be null", exception.getMessage());
                    latch.countDown();
                }
            };
            restClient.performRequestAsync("GET", randomAsciiLettersOfLength(5), listener, (Header) null);
            latch.await();
        }
    }

    public void testPerformAsyncWithWrongEndpoint() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        try (RestClient restClient = createRestClient()) {
            restClient.performRequestAsync("GET", "::http:///", new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    fail("should have failed because of wrong endpoint");
                }

                @Override
                public void onFailure(Exception exception) {
                    assertThat(exception, instanceOf(IllegalArgumentException.class));
                    assertEquals("Expected scheme name at index 0: ::http:///", exception.getMessage());
                    latch.countDown();
                }
            });
            latch.await();
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

    public void testNextHostsOneTime() {
        int iterations = 1000;
        HttpHost h1 = new HttpHost("1");
        HttpHost h2 = new HttpHost("2");
        HttpHost h3 = new HttpHost("3");
        Set<HttpHost> hosts = new HashSet<>();
        hosts.add(h1);
        hosts.add(h2);
        hosts.add(h3);

        HostMetadataResolver versionIsName = new HostMetadataResolver() {
            @Override
            public HostMetadata resolveMetadata(HttpHost host) {
                return new HostMetadata(host.toHostString(), new HostMetadata.Roles(true, true, true));
            }
        };
        HostSelector not1 = new HostSelector() {
            @Override
            public boolean select(HttpHost host, HostMetadata meta) {
                return false == "1".equals(meta.version());
            }
        };
        HostSelector noHosts = new HostSelector() {
            @Override
            public boolean select(HttpHost host, HostMetadata meta) {
                return false;
            }
        };

        HostTuple<Set<HttpHost>> hostTuple = new HostTuple<>(hosts, null, versionIsName);
        Map<HttpHost, DeadHostState> blacklist = new HashMap<>();
        AtomicInteger lastHostIndex = new AtomicInteger(0);
        long now = 0;

        // Normal case
        NextHostsResult result = RestClient.nextHostsOneTime(hostTuple, blacklist,
                lastHostIndex, now, HostSelector.ANY);
        assertThat(result.hosts, containsInAnyOrder(h1, h2, h3));
        List<HttpHost> expectedHosts = new ArrayList<>(result.hosts);
        // Calling it again rotates the set of results
        for (int i = 0; i < iterations; i++) {
            Collections.rotate(expectedHosts, 1);
            assertEquals(expectedHosts, RestClient.nextHostsOneTime(hostTuple, blacklist,
                    lastHostIndex, now, HostSelector.ANY).hosts);
        }

        // Exclude some host
        lastHostIndex.set(0);
        result = RestClient.nextHostsOneTime(hostTuple, blacklist, lastHostIndex, now, not1);
        assertThat(result.hosts, containsInAnyOrder(h2, h3)); // h1 excluded
        assertEquals(0, result.blacklisted);
        assertEquals(1, result.selectorRejected);
        assertEquals(0, result.selectorBlockedRevival);
        expectedHosts = new ArrayList<>(result.hosts);
        // Calling it again rotates the set of results
        for (int i = 0; i < iterations; i++) {
            Collections.rotate(expectedHosts, 1);
            assertEquals(expectedHosts, RestClient.nextHostsOneTime(hostTuple, blacklist,
                    lastHostIndex, now, not1).hosts);
        }

        /*
         * Try a HostSelector that excludes all hosts. This should
         * return a failure.
         */
        result = RestClient.nextHostsOneTime(hostTuple, blacklist, lastHostIndex, now, noHosts);
        assertNull(result.hosts);
        assertEquals(0, result.blacklisted);
        assertEquals(3, result.selectorRejected);
        assertEquals(0, result.selectorBlockedRevival);

        /*
         * Mark all hosts as dead and look up at a time *after* the
         * revival time. This should return all hosts.
         */
        blacklist.put(h1, new DeadHostState(1, 1));
        blacklist.put(h2, new DeadHostState(1, 2));
        blacklist.put(h3, new DeadHostState(1, 3));
        lastHostIndex.set(0);
        now = 1000;
        result = RestClient.nextHostsOneTime(hostTuple, blacklist, lastHostIndex,
                now, HostSelector.ANY);
        assertThat(result.hosts, containsInAnyOrder(h1, h2, h3));
        assertEquals(0, result.blacklisted);
        assertEquals(0, result.selectorRejected);
        assertEquals(0, result.selectorBlockedRevival);
        expectedHosts = new ArrayList<>(result.hosts);
        // Calling it again rotates the set of results
        for (int i = 0; i < iterations; i++) {
            Collections.rotate(expectedHosts, 1);
            assertEquals(expectedHosts, RestClient.nextHostsOneTime(hostTuple, blacklist,
                    lastHostIndex, now, HostSelector.ANY).hosts);
        }

        /*
         * Now try with the hosts dead and *not* past their dead time.
         * Only the host closest to revival should come back.
         */
        now = 0;
        result = RestClient.nextHostsOneTime(hostTuple, blacklist, lastHostIndex,
                now, HostSelector.ANY);
        assertEquals(Collections.singleton(h1), result.hosts);
        assertEquals(3, result.blacklisted);
        assertEquals(0, result.selectorRejected);
        assertEquals(0, result.selectorBlockedRevival);

        /*
         * Now try with the hosts dead and *not* past their dead time
         * *and* a host selector that removes the host that is closest
         * to being revived. The second closest host should come back.
         */
        result = RestClient.nextHostsOneTime(hostTuple, blacklist, lastHostIndex,
                now, not1);
        assertEquals(Collections.singleton(h2), result.hosts);
        assertEquals(3, result.blacklisted);
        assertEquals(0, result.selectorRejected);
        assertEquals(1, result.selectorBlockedRevival);

        /*
         * Try a HostSelector that excludes all hosts. This should
         * return a failure, but a different failure than normal
         * because it'll block revival rather than outright reject
         * healthy hosts.
         */
        result = RestClient.nextHostsOneTime(hostTuple, blacklist, lastHostIndex, now, noHosts);
        assertNull(result.hosts);
        assertEquals(3, result.blacklisted);
        assertEquals(0, result.selectorRejected);
        assertEquals(3, result.selectorBlockedRevival);
    }

    private static RestClient createRestClient() {
        HttpHost[] hosts = new HttpHost[]{new HttpHost("localhost", 9200)};
        return new RestClient(mock(CloseableHttpAsyncClient.class), randomLongBetween(1_000, 30_000),
                new Header[] {}, hosts, HostMetadata.EMPTY_RESOLVER, null, null);
    }
}
