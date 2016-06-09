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
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.client.RestClientTestUtil.getAllErrorStatusCodes;
import static org.elasticsearch.client.RestClientTestUtil.getHttpMethods;
import static org.elasticsearch.client.RestClientTestUtil.getOkStatusCodes;
import static org.elasticsearch.client.RestClientTestUtil.randomHttpMethod;
import static org.elasticsearch.client.RestClientTestUtil.randomStatusCode;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for basic functionality of {@link RestClient} against one single host: tests http requests being sent, headers,
 * body, different status codes and corresponding responses/exceptions.
 * Relies on a mock http client to intercept requests and return desired responses based on request path.
 */
public class RestClientSingleHostTests extends LuceneTestCase {

    private RestClient restClient;
    private Header[] defaultHeaders;
    private HttpHost httpHost;
    private CloseableHttpClient httpClient;
    private TrackingFailureListener failureListener;

    @Before
    public void createRestClient() throws IOException {
        httpClient = mock(CloseableHttpClient.class);
        when(httpClient.execute(any(HttpHost.class), any(HttpRequest.class))).thenAnswer(new Answer<CloseableHttpResponse>() {
            @Override
            public CloseableHttpResponse answer(InvocationOnMock invocationOnMock) throws Throwable {
                HttpUriRequest request = (HttpUriRequest) invocationOnMock.getArguments()[1];
                //return the desired status code or exception depending on the path
                if (request.getURI().getPath().equals("/soe")) {
                    throw new SocketTimeoutException();
                } else if (request.getURI().getPath().equals("/coe")) {
                    throw new ConnectTimeoutException();
                }
                int statusCode = Integer.parseInt(request.getURI().getPath().substring(1));
                StatusLine statusLine = new BasicStatusLine(new ProtocolVersion("http", 1, 1), statusCode, "");

                CloseableHttpResponse httpResponse = new CloseableBasicHttpResponse(statusLine);
                //return the same body that was sent
                if (request instanceof HttpEntityEnclosingRequest) {
                    HttpEntity entity = ((HttpEntityEnclosingRequest) request).getEntity();
                    if (entity != null) {
                        assertTrue("the entity is not repeatable, cannot set it to the response directly", entity.isRepeatable());
                        httpResponse.setEntity(entity);
                    }
                }
                //return the same headers that were sent
                httpResponse.setHeaders(request.getAllHeaders());
                return httpResponse;
            }
        });
        int numHeaders = RandomInts.randomIntBetween(random(), 0, 3);
        defaultHeaders = new Header[numHeaders];
        for (int i = 0; i < numHeaders; i++) {
            String headerName = "Header-default" + (random().nextBoolean() ? i : "");
            String headerValue = RandomStrings.randomAsciiOfLengthBetween(random(), 3, 10);
            defaultHeaders[i] = new BasicHeader(headerName, headerValue);
        }
        httpHost = new HttpHost("localhost", 9200);
        restClient = RestClient.builder(httpHost).setHttpClient(httpClient).setDefaultHeaders(defaultHeaders).build();
        failureListener = new TrackingFailureListener();
        restClient.setFailureListener(failureListener);
    }

    /**
     * Verifies the content of the {@link HttpRequest} that's internally created and passed through to the http client
     */
    public void testInternalHttpRequest() throws Exception {
        ArgumentCaptor<HttpUriRequest> requestArgumentCaptor = ArgumentCaptor.forClass(HttpUriRequest.class);
        int times = 0;
        for (String httpMethod : getHttpMethods()) {
            HttpUriRequest expectedRequest = performRandomRequest(httpMethod);
            verify(httpClient, times(++times)).execute(any(HttpHost.class), requestArgumentCaptor.capture());
            HttpUriRequest actualRequest = requestArgumentCaptor.getValue();
            assertEquals(expectedRequest.getURI(), actualRequest.getURI());
            assertEquals(expectedRequest.getClass(), actualRequest.getClass());
            assertArrayEquals(expectedRequest.getAllHeaders(), actualRequest.getAllHeaders());
            if (expectedRequest instanceof HttpEntityEnclosingRequest) {
                HttpEntity expectedEntity = ((HttpEntityEnclosingRequest) expectedRequest).getEntity();
                if (expectedEntity != null) {
                    HttpEntity actualEntity = ((HttpEntityEnclosingRequest) actualRequest).getEntity();
                    assertEquals(EntityUtils.toString(expectedEntity), EntityUtils.toString(actualEntity));
                }
            }
        }
    }

    public void testSetNodes() throws IOException {
        try {
            restClient.setHosts((HttpHost[]) null);
            fail("setHosts should have failed");
        } catch (IllegalArgumentException e) {
            assertEquals("hosts must not be null nor empty", e.getMessage());
        }
        try {
            restClient.setHosts();
            fail("setHosts should have failed");
        } catch (IllegalArgumentException e) {
            assertEquals("hosts must not be null nor empty", e.getMessage());
        }
        try {
            restClient.setHosts((HttpHost) null);
            fail("setHosts should have failed");
        } catch (NullPointerException e) {
            assertEquals("host cannot be null", e.getMessage());
        }
        try {
            restClient.setHosts(new HttpHost("localhost", 9200), null, new HttpHost("localhost", 9201));
            fail("setHosts should have failed");
        } catch (NullPointerException e) {
            assertEquals("host cannot be null", e.getMessage());
        }
    }

    /**
     * End to end test for ok status codes
     */
    public void testOkStatusCodes() throws Exception {
        for (String method : getHttpMethods()) {
            for (int okStatusCode : getOkStatusCodes()) {
                Response response = restClient.performRequest(method, "/" + okStatusCode,
                        Collections.<String, String>emptyMap(), null);
                assertThat(response.getStatusLine().getStatusCode(), equalTo(okStatusCode));
            }
        }
        failureListener.assertNotCalled();
    }

    /**
     * End to end test for error status codes: they should cause an exception to be thrown, apart from 404 with HEAD requests
     */
    public void testErrorStatusCodes() throws Exception {
        for (String method : getHttpMethods()) {
            //error status codes should cause an exception to be thrown
            for (int errorStatusCode : getAllErrorStatusCodes()) {
                try (Response response = restClient.performRequest(method, "/" + errorStatusCode,
                        Collections.<String, String>emptyMap(), null)) {
                    if (method.equals("HEAD") && errorStatusCode == 404) {
                        //no exception gets thrown although we got a 404
                        assertThat(response.getStatusLine().getStatusCode(), equalTo(errorStatusCode));
                    } else {
                        fail("request should have failed");
                    }
                } catch(ResponseException e) {
                    if (method.equals("HEAD") && errorStatusCode == 404) {
                        throw e;
                    }
                    assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(errorStatusCode));
                }
                if (errorStatusCode <= 500) {
                    failureListener.assertNotCalled();
                } else {
                    failureListener.assertCalled(httpHost);
                }
            }
        }
    }

    public void testIOExceptions() throws IOException {
        for (String method : getHttpMethods()) {
            //IOExceptions should be let bubble up
            try {
                restClient.performRequest(method, "/coe", Collections.<String, String>emptyMap(), null);
                fail("request should have failed");
            } catch(IOException e) {
                assertThat(e, instanceOf(ConnectTimeoutException.class));
            }
            failureListener.assertCalled(httpHost);
            try {
                restClient.performRequest(method, "/soe", Collections.<String, String>emptyMap(), null);
                fail("request should have failed");
            } catch(IOException e) {
                assertThat(e, instanceOf(SocketTimeoutException.class));
            }
            failureListener.assertCalled(httpHost);
        }
    }

    /**
     * End to end test for request and response body. Exercises the mock http client ability to send back
     * whatever body it has received.
     */
    public void testBody() throws Exception {
        String body = "{ \"field\": \"value\" }";
        StringEntity entity = new StringEntity(body);
        for (String method : Arrays.asList("DELETE", "GET", "PATCH", "POST", "PUT")) {
            for (int okStatusCode : getOkStatusCodes()) {
                try (Response response = restClient.performRequest(method, "/" + okStatusCode,
                        Collections.<String, String>emptyMap(), entity)) {
                    assertThat(response.getStatusLine().getStatusCode(), equalTo(okStatusCode));
                    assertThat(EntityUtils.toString(response.getEntity()), equalTo(body));
                }
            }
            for (int errorStatusCode : getAllErrorStatusCodes()) {
                try {
                    restClient.performRequest(method, "/" + errorStatusCode, Collections.<String, String>emptyMap(), entity);
                    fail("request should have failed");
                } catch(ResponseException e) {
                    Response response = e.getResponse();
                    assertThat(response.getStatusLine().getStatusCode(), equalTo(errorStatusCode));
                    assertThat(EntityUtils.toString(response.getEntity()), equalTo(body));
                }
            }
        }
        for (String method : Arrays.asList("HEAD", "OPTIONS", "TRACE")) {
            try {
                restClient.performRequest(method, "/" + randomStatusCode(random()),
                        Collections.<String, String>emptyMap(), entity);
                fail("request should have failed");
            } catch(UnsupportedOperationException e) {
                assertThat(e.getMessage(), equalTo(method + " with body is not supported"));
            }
        }
    }

    public void testNullHeaders() throws Exception {
        String method = randomHttpMethod(random());
        int statusCode = randomStatusCode(random());
        try {
            restClient.performRequest(method, "/" + statusCode, Collections.<String, String>emptyMap(), null, (Header[])null);
            fail("request should have failed");
        } catch(NullPointerException e) {
            assertEquals("request headers must not be null", e.getMessage());
        }
        try {
            restClient.performRequest(method, "/" + statusCode, Collections.<String, String>emptyMap(), null, (Header)null);
            fail("request should have failed");
        } catch(NullPointerException e) {
            assertEquals("request header must not be null", e.getMessage());
        }
    }

    public void testNullParams() throws Exception {
        String method = randomHttpMethod(random());
        int statusCode = randomStatusCode(random());
        try {
            restClient.performRequest(method, "/" + statusCode, null, null);
            fail("request should have failed");
        } catch(NullPointerException e) {
            assertEquals("params must not be null", e.getMessage());
        }
    }

    /**
     * End to end test for request and response headers. Exercises the mock http client ability to send back
     * whatever headers it has received.
     */
    public void testHeaders() throws Exception {
        for (String method : getHttpMethods()) {
            Map<String, String> expectedHeaders = new HashMap<>();
            for (Header defaultHeader : defaultHeaders) {
                expectedHeaders.put(defaultHeader.getName(), defaultHeader.getValue());
            }
            int numHeaders = RandomInts.randomIntBetween(random(), 1, 5);
            Header[] headers = new Header[numHeaders];
            for (int i = 0; i < numHeaders; i++) {
                String headerName = "Header" + (random().nextBoolean() ? i : "");
                String headerValue = RandomStrings.randomAsciiOfLengthBetween(random(), 3, 10);
                headers[i] = new BasicHeader(headerName, headerValue);
                expectedHeaders.put(headerName, headerValue);
            }

            int statusCode = randomStatusCode(random());
            Response esResponse;
            try (Response response = restClient.performRequest(method, "/" + statusCode,
                    Collections.<String, String>emptyMap(), null, headers)) {
                esResponse = response;
            } catch(ResponseException e) {
                esResponse = e.getResponse();
            }
            assertThat(esResponse.getStatusLine().getStatusCode(), equalTo(statusCode));
            for (Header responseHeader : esResponse.getHeaders()) {
                String headerValue = expectedHeaders.remove(responseHeader.getName());
                assertNotNull("found response header [" + responseHeader.getName() + "] that wasn't originally sent", headerValue);
            }
            assertEquals("some headers that were sent weren't returned " + expectedHeaders, 0, expectedHeaders.size());
        }
    }

    private HttpUriRequest performRandomRequest(String method) throws IOException, URISyntaxException {
        String uriAsString = "/" + randomStatusCode(random());
        URIBuilder uriBuilder = new URIBuilder(uriAsString);
        Map<String, String> params = Collections.emptyMap();
        if (random().nextBoolean()) {
            int numParams = RandomInts.randomIntBetween(random(), 1, 3);
            params = new HashMap<>(numParams);
            for (int i = 0; i < numParams; i++) {
                String paramKey = "param-" + i;
                String paramValue = RandomStrings.randomAsciiOfLengthBetween(random(), 3, 10);
                params.put(paramKey, paramValue);
                uriBuilder.addParameter(paramKey, paramValue);
            }
        }
        URI uri = uriBuilder.build();

        HttpUriRequest request;
        switch(method) {
            case "DELETE":
                request = new HttpDeleteWithEntity(uri);
                break;
            case "GET":
                request = new HttpGetWithEntity(uri);
                break;
            case "HEAD":
                request = new HttpHead(uri);
                break;
            case "OPTIONS":
                request = new HttpOptions(uri);
                break;
            case "PATCH":
                request = new HttpPatch(uri);
                break;
            case "POST":
                request = new HttpPost(uri);
                break;
            case "PUT":
                request = new HttpPut(uri);
                break;
            case "TRACE":
                request = new HttpTrace(uri);
                break;
            default:
                throw new UnsupportedOperationException("method not supported: " + method);
        }

        HttpEntity entity = null;
        if (request instanceof HttpEntityEnclosingRequest && random().nextBoolean()) {
            entity = new StringEntity(RandomStrings.randomAsciiOfLengthBetween(random(), 10, 100));
            ((HttpEntityEnclosingRequest) request).setEntity(entity);
        }

        Header[] headers = new Header[0];
        for (Header defaultHeader : defaultHeaders) {
            //default headers are expected but not sent for each request
            request.setHeader(defaultHeader);
        }
        if (random().nextBoolean()) {
            int numHeaders = RandomInts.randomIntBetween(random(), 1, 5);
            headers = new Header[numHeaders];
            for (int i = 0; i < numHeaders; i++) {
                String headerName = "Header" + (random().nextBoolean() ? i : "");
                String headerValue = RandomStrings.randomAsciiOfLengthBetween(random(), 3, 10);
                BasicHeader basicHeader = new BasicHeader(headerName, headerValue);
                headers[i] = basicHeader;
                request.setHeader(basicHeader);
            }
        }

        try {
            restClient.performRequest(method, uriAsString, params, entity, headers);
        } catch(ResponseException e) {
            //all good
        }
        return request;
    }
}
