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
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Before;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.client.RestClientTestUtil.randomErrorNoRetryStatusCode;
import static org.elasticsearch.client.RestClientTestUtil.randomErrorRetryStatusCode;
import static org.elasticsearch.client.RestClientTestUtil.randomHttpMethod;
import static org.elasticsearch.client.RestClientTestUtil.randomOkStatusCode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link RestClient} behaviour against multiple hosts: fail-over, blacklisting etc.
 * Relies on a mock http client to intercept requests and return desired responses based on request path.
 */
public class RestClientMultipleHostsTests extends LuceneTestCase {

    private RestClient restClient;
    private HttpHost[] httpHosts;
    private TrackingFailureListener failureListener;

    @Before
    public void createRestClient() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        when(httpClient.execute(any(HttpHost.class), any(HttpRequest.class))).thenAnswer(new Answer<CloseableHttpResponse>() {
            @Override
            public CloseableHttpResponse answer(InvocationOnMock invocationOnMock) throws Throwable {
                HttpHost httpHost = (HttpHost) invocationOnMock.getArguments()[0];
                HttpUriRequest request = (HttpUriRequest) invocationOnMock.getArguments()[1];
                //return the desired status code or exception depending on the path
                if (request.getURI().getPath().equals("/soe")) {
                    throw new SocketTimeoutException(httpHost.toString());
                } else if (request.getURI().getPath().equals("/coe")) {
                    throw new ConnectTimeoutException(httpHost.toString());
                } else if (request.getURI().getPath().equals("/ioe")) {
                    throw new IOException(httpHost.toString());
                }
                int statusCode = Integer.parseInt(request.getURI().getPath().substring(1));
                StatusLine statusLine = new BasicStatusLine(new ProtocolVersion("http", 1, 1), statusCode, "");
                return new CloseableBasicHttpResponse(statusLine);
            }
        });

        int numHosts = RandomInts.randomIntBetween(random(), 2, 5);
        httpHosts = new HttpHost[numHosts];
        for (int i = 0; i < numHosts; i++) {
            httpHosts[i] = new HttpHost("localhost", 9200 + i);
        }
        failureListener = new TrackingFailureListener();
        restClient = RestClient.builder(httpHosts).setHttpClient(httpClient).setFailureListener(failureListener).build();
    }

    public void testRoundRobinOkStatusCodes() throws Exception {
        int numIters = RandomInts.randomIntBetween(random(), 1, 5);
        for (int i = 0; i < numIters; i++) {
            Set<HttpHost> hostsSet = new HashSet<>();
            Collections.addAll(hostsSet, httpHosts);
            for (int j = 0; j < httpHosts.length; j++) {
                int statusCode = randomOkStatusCode(random());
                try (Response response = restClient.performRequest(randomHttpMethod(random()), "/" + statusCode,
                        Collections.<String, String>emptyMap(), null)) {
                    assertThat(response.getStatusLine().getStatusCode(), equalTo(statusCode));
                    assertTrue("host not found: " + response.getHost(), hostsSet.remove(response.getHost()));
                }
            }
            assertEquals("every host should have been used but some weren't: " + hostsSet, 0, hostsSet.size());
        }
        failureListener.assertNotCalled();
    }

    public void testRoundRobinNoRetryErrors() throws Exception {
        int numIters = RandomInts.randomIntBetween(random(), 1, 5);
        for (int i = 0; i < numIters; i++) {
            Set<HttpHost> hostsSet = new HashSet<>();
            Collections.addAll(hostsSet, httpHosts);
            for (int j = 0; j < httpHosts.length; j++) {
                String method = randomHttpMethod(random());
                int statusCode = randomErrorNoRetryStatusCode(random());
                try (Response response = restClient.performRequest(method, "/" + statusCode,
                        Collections.<String, String>emptyMap(), null)) {
                    if (method.equals("HEAD") && statusCode == 404) {
                        //no exception gets thrown although we got a 404
                        assertThat(response.getStatusLine().getStatusCode(), equalTo(404));
                        assertThat(response.getStatusLine().getStatusCode(), equalTo(statusCode));
                        assertTrue("host not found: " + response.getHost(), hostsSet.remove(response.getHost()));
                    } else {
                        fail("request should have failed");
                    }
                } catch(ResponseException e) {
                    if (method.equals("HEAD") && statusCode == 404) {
                        throw e;
                    }
                    Response response = e.getResponse();
                    assertThat(response.getStatusLine().getStatusCode(), equalTo(statusCode));
                    assertTrue("host not found: " + response.getHost(), hostsSet.remove(response.getHost()));
                    assertEquals(0, e.getSuppressed().length);
                }
            }
            assertEquals("every host should have been used but some weren't: " + hostsSet, 0, hostsSet.size());
        }
        failureListener.assertNotCalled();
    }

    public void testRoundRobinRetryErrors() throws Exception {
        String retryEndpoint = randomErrorRetryEndpoint();
        try  {
            restClient.performRequest(randomHttpMethod(random()), retryEndpoint, Collections.<String, String>emptyMap(), null);
            fail("request should have failed");
        } catch(ResponseException e) {
            Set<HttpHost> hostsSet = new HashSet<>();
            Collections.addAll(hostsSet, httpHosts);
            //first request causes all the hosts to be blacklisted, the returned exception holds one suppressed exception each
            failureListener.assertCalled(httpHosts);
            do {
                Response response = e.getResponse();
                assertThat(response.getStatusLine().getStatusCode(), equalTo(Integer.parseInt(retryEndpoint.substring(1))));
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
        } catch(IOException e) {
            Set<HttpHost> hostsSet = new HashSet<>();
            Collections.addAll(hostsSet, httpHosts);
            //first request causes all the hosts to be blacklisted, the returned exception holds one suppressed exception each
            failureListener.assertCalled(httpHosts);
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

        int numIters = RandomInts.randomIntBetween(random(), 2, 5);
        for (int i = 1; i <= numIters; i++) {
            //check that one different host is resurrected at each new attempt
            Set<HttpHost> hostsSet = new HashSet<>();
            Collections.addAll(hostsSet, httpHosts);
            for (int j = 0; j < httpHosts.length; j++) {
                retryEndpoint = randomErrorRetryEndpoint();
                try  {
                    restClient.performRequest(randomHttpMethod(random()), retryEndpoint, Collections.<String, String>emptyMap(), null);
                    fail("request should have failed");
                } catch(ResponseException e) {
                    Response response = e.getResponse();
                    assertThat(response.getStatusLine().getStatusCode(), equalTo(Integer.parseInt(retryEndpoint.substring(1))));
                    assertTrue("host [" + response.getHost() + "] not found, most likely used multiple times",
                            hostsSet.remove(response.getHost()));
                    //after the first request, all hosts are blacklisted, a single one gets resurrected each time
                    failureListener.assertCalled(response.getHost());
                    assertEquals(0, e.getSuppressed().length);
                } catch(IOException e) {
                    HttpHost httpHost = HttpHost.create(e.getMessage());
                    assertTrue("host [" + httpHost + "] not found, most likely used multiple times", hostsSet.remove(httpHost));
                    //after the first request, all hosts are blacklisted, a single one gets resurrected each time
                    failureListener.assertCalled(httpHost);
                    assertEquals(0, e.getSuppressed().length);
                }
            }
            assertEquals("every host should have been used but some weren't: " + hostsSet, 0, hostsSet.size());
            if (random().nextBoolean()) {
                //mark one host back alive through a successful request and check that all requests after that are sent to it
                HttpHost selectedHost = null;
                int iters = RandomInts.randomIntBetween(random(), 2, 10);
                for (int y = 0; y < iters; y++) {
                    int statusCode = randomErrorNoRetryStatusCode(random());
                    Response response;
                    try (Response esResponse = restClient.performRequest(randomHttpMethod(random()), "/" + statusCode,
                            Collections.<String, String>emptyMap(), null)) {
                        response = esResponse;
                    }
                    catch(ResponseException e) {
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
                        restClient.performRequest(randomHttpMethod(random()), retryEndpoint,
                                Collections.<String, String>emptyMap(), null);
                        fail("request should have failed");
                    } catch(ResponseException e) {
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

    private static String randomErrorRetryEndpoint() {
        switch(RandomInts.randomIntBetween(random(), 0, 3)) {
            case 0:
                return "/" + randomErrorRetryStatusCode(random());
            case 1:
                return "/coe";
            case 2:
                return "/soe";
            case 3:
                return "/ioe";
        }
        throw new UnsupportedOperationException();
    }
}
