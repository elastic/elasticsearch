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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
public class RestClientSingleHostTests extends RestClientTestCase {

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
        int numHeaders = RandomInts.randomIntBetween(getRandom(), 0, 3);
        defaultHeaders = new Header[numHeaders];
        for (int i = 0; i < numHeaders; i++) {
            String headerName = "Header-default" + (getRandom().nextBoolean() ? i : "");
            String headerValue = RandomStrings.randomAsciiOfLengthBetween(getRandom(), 3, 10);
            defaultHeaders[i] = new BasicHeader(headerName, headerValue);
        }
        httpHost = new HttpHost("localhost", 9200);
        failureListener = new TrackingFailureListener();
        restClient = new RestClient(httpClient, 10000, defaultHeaders, new HttpHost[]{httpHost}, failureListener);
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

    public void testSetHosts() throws IOException {
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
                Response response = performRequest(method, "/" + okStatusCode);
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
                try (Response response = performRequest(method, "/" + errorStatusCode)) {
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
                performRequest(method, "/coe");
                fail("request should have failed");
            } catch(IOException e) {
                assertThat(e, instanceOf(ConnectTimeoutException.class));
            }
            failureListener.assertCalled(httpHost);
            try {
                performRequest(method, "/soe");
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
                restClient.performRequest(method, "/" + randomStatusCode(getRandom()), Collections.<String, String>emptyMap(), entity);
                fail("request should have failed");
            } catch(UnsupportedOperationException e) {
                assertThat(e.getMessage(), equalTo(method + " with body is not supported"));
            }
        }
    }

    public void testNullHeaders() throws Exception {
        String method = randomHttpMethod(getRandom());
        int statusCode = randomStatusCode(getRandom());
        try {
            performRequest(method, "/" + statusCode, (Header[])null);
            fail("request should have failed");
        } catch(NullPointerException e) {
            assertEquals("request headers must not be null", e.getMessage());
        }
        try {
            performRequest(method, "/" + statusCode, (Header)null);
            fail("request should have failed");
        } catch(NullPointerException e) {
            assertEquals("request header must not be null", e.getMessage());
        }
    }

    public void testNullParams() throws Exception {
        String method = randomHttpMethod(getRandom());
        int statusCode = randomStatusCode(getRandom());
        try {
            restClient.performRequest(method, "/" + statusCode, (Map<String, String>)null);
            fail("request should have failed");
        } catch(NullPointerException e) {
            assertEquals("params must not be null", e.getMessage());
        }
        try {
            restClient.performRequest(method, "/" + statusCode, null, (HttpEntity)null);
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
            int numHeaders = RandomInts.randomIntBetween(getRandom(), 1, 5);
            Header[] headers = new Header[numHeaders];
            for (int i = 0; i < numHeaders; i++) {
                String headerName = "Header" + (getRandom().nextBoolean() ? i : "");
                String headerValue = RandomStrings.randomAsciiOfLengthBetween(getRandom(), 3, 10);
                headers[i] = new BasicHeader(headerName, headerValue);
                expectedHeaders.put(headerName, headerValue);
            }

            int statusCode = randomStatusCode(getRandom());
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
        String uriAsString = "/" + randomStatusCode(getRandom());
        URIBuilder uriBuilder = new URIBuilder(uriAsString);
        Map<String, String> params = Collections.emptyMap();
        boolean hasParams = randomBoolean();
        if (hasParams) {
            int numParams = RandomInts.randomIntBetween(getRandom(), 1, 3);
            params = new HashMap<>(numParams);
            for (int i = 0; i < numParams; i++) {
                String paramKey = "param-" + i;
                String paramValue = RandomStrings.randomAsciiOfLengthBetween(getRandom(), 3, 10);
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
        boolean hasBody = request instanceof HttpEntityEnclosingRequest && getRandom().nextBoolean();
        if (hasBody) {
            entity = new StringEntity(RandomStrings.randomAsciiOfLengthBetween(getRandom(), 10, 100));
            ((HttpEntityEnclosingRequest) request).setEntity(entity);
        }

        Header[] headers = new Header[0];
        for (Header defaultHeader : defaultHeaders) {
            //default headers are expected but not sent for each request
            request.setHeader(defaultHeader);
        }
        if (getRandom().nextBoolean()) {
            int numHeaders = RandomInts.randomIntBetween(getRandom(), 1, 5);
            headers = new Header[numHeaders];
            for (int i = 0; i < numHeaders; i++) {
                String headerName = "Header" + (getRandom().nextBoolean() ? i : "");
                String headerValue = RandomStrings.randomAsciiOfLengthBetween(getRandom(), 3, 10);
                BasicHeader basicHeader = new BasicHeader(headerName, headerValue);
                headers[i] = basicHeader;
                request.setHeader(basicHeader);
            }
        }

        try {
            if (hasParams == false && hasBody == false && randomBoolean()) {
                restClient.performRequest(method, uriAsString, headers);
            } else if (hasBody == false && randomBoolean()) {
                restClient.performRequest(method, uriAsString, params, headers);
            } else {
                restClient.performRequest(method, uriAsString, params, entity, headers);
            }
        } catch(ResponseException e) {
            //all good
        }
        return request;
    }

    private Response performRequest(String method, String endpoint, Header... headers) throws IOException {
        switch(randomIntBetween(0, 2)) {
            case 0:
                return restClient.performRequest(method, endpoint, headers);
            case 1:
                return restClient.performRequest(method, endpoint, Collections.<String, String>emptyMap(), headers);
            case 2:
                return restClient.performRequest(method, endpoint, Collections.<String, String>emptyMap(), null, headers);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
