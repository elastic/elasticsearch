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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.ProtocolVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.internal.matchers.ArrayEquals;
import org.mockito.internal.matchers.VarargMatcher;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestHighLevelClientTests extends ESTestCase {

    private RestClient restClient;
    private RestHighLevelClient restHighLevelClient;

    @Before
    public void initClient() throws IOException {
        restClient = mock(RestClient.class);
        restHighLevelClient = new RestHighLevelClient(restClient);
    }

    public void testPingSuccessful() throws IOException {
        Header[] headers = RestClientTestUtil.randomHeaders(random(), "Header");
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(200));
        when(restClient.performRequest(anyString(), anyString(), anyMapOf(String.class, String.class),
                anyObject(), anyVararg())).thenReturn(response);
        assertTrue(restHighLevelClient.ping(headers));
        verify(restClient).performRequest(eq("HEAD"), eq("/"), eq(Collections.emptyMap()),
                Matchers.isNull(HttpEntity.class), argThat(new HeadersVarargMatcher(headers)));
    }

    public void testPing404NotFound() throws IOException {
        Header[] headers = RestClientTestUtil.randomHeaders(random(), "Header");
        Response response = mock(Response.class);
        when(response.getStatusLine()).thenReturn(newStatusLine(404));
        when(restClient.performRequest(anyString(), anyString(), anyMapOf(String.class, String.class),
                anyObject(), anyVararg())).thenReturn(response);
        assertFalse(restHighLevelClient.ping(headers));
        verify(restClient).performRequest(eq("HEAD"), eq("/"), eq(Collections.emptyMap()),
                Matchers.isNull(HttpEntity.class), argThat(new HeadersVarargMatcher(headers)));
    }

    public void testPingSocketTimeout() throws IOException {
        Header[] headers = RestClientTestUtil.randomHeaders(random(), "Header");
        when(restClient.performRequest(anyString(), anyString(), anyMapOf(String.class, String.class),
                anyObject(), anyVararg())).thenThrow(new SocketTimeoutException());
        expectThrows(SocketTimeoutException.class, () -> restHighLevelClient.ping(headers));
        verify(restClient).performRequest(eq("HEAD"), eq("/"), eq(Collections.emptyMap()),
                Matchers.isNull(HttpEntity.class), argThat(new HeadersVarargMatcher(headers)));
    }

    public void testRequestValidation() throws IOException {
        ActionRequestValidationException validationException = new ActionRequestValidationException();
        validationException.addValidationError("validation error");
        ActionRequest request = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return validationException;
            }
        };

        {
            ActionRequestValidationException actualException = expectThrows(ActionRequestValidationException.class,
                    () -> restHighLevelClient.performRequest(request, null, null));
            assertSame(validationException, actualException);
        }
        {
            TrackingActionListener trackingActionListener = new TrackingActionListener();
            restHighLevelClient.performRequestAsync(request, null, null, trackingActionListener);
            assertSame(validationException, trackingActionListener.exception.get());
        }
    }

    public void testParseEntity() throws IOException {
        {
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> RestHighLevelClient.parseEntity(null, null));
            assertEquals("Response body expected but not returned", ise.getMessage());
        }
        {
            IllegalStateException ise = expectThrows(IllegalStateException.class,
                    () -> RestHighLevelClient.parseEntity(new BasicHttpEntity(), null));
            assertEquals("Elasticsearch didn't return the [Content-Type] header, unable to parse response body", ise.getMessage());
        }
        {
            StringEntity entity = new StringEntity("", ContentType.APPLICATION_SVG_XML);
            IllegalStateException ise = expectThrows(IllegalStateException.class, () -> RestHighLevelClient.parseEntity(entity, null));
            assertEquals("Unsupported Content-Type: " + entity.getContentType().getValue(), ise.getMessage());
        }
        {
            StringEntity entity = new StringEntity("{\"field\":\"value\"}", ContentType.APPLICATION_JSON);
            assertEquals("value", RestHighLevelClient.parseEntity(entity, parser -> {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertTrue(parser.nextToken().isValue());
                String value = parser.text();
                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                assertNull(parser.nextToken());
                return value;
            }));
        }
    }

    public void testConvertExistsResponse() {
        ProtocolVersion protocolVersion = new ProtocolVersion("http", 1, 1);
        BasicRequestLine requestLine = new BasicRequestLine("GET", "/", protocolVersion);
        RestStatus restStatus = randomBoolean() ? RestStatus.OK : randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocolVersion,
                restStatus.getStatus(), restStatus.name()));
        Response response = new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
        boolean result = RestHighLevelClient.convertExistsResponse(response);
        assertEquals(restStatus == RestStatus.OK, result);
    }

    public void testParseResponseException() throws IOException {
        ProtocolVersion protocolVersion = new ProtocolVersion("http", 1, 1);
        BasicRequestLine requestLine = new BasicRequestLine("GET", "/", protocolVersion);
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocolVersion,
                    restStatus.getStatus(), restStatus.name()));
            Response response = new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            ElasticsearchException elasticsearchException = RestHighLevelClient.parseResponseException(responseException);
            assertEquals(responseException.getMessage(), elasticsearchException.getMessage());
            assertEquals(restStatus, elasticsearchException.status());
            assertEquals(1, elasticsearchException.getSuppressed().length);
            assertSame(responseException, elasticsearchException.getSuppressed()[0]);
        }
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocolVersion,
                    restStatus.getStatus(), restStatus.name()));
            httpResponse.setEntity(new StringEntity("{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}",
                    ContentType.APPLICATION_JSON));
            Response response = new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            ElasticsearchException elasticsearchException = RestHighLevelClient.parseResponseException(responseException);
            assertEquals("Elasticsearch exception [type=exception, reason=test error message]", elasticsearchException.getMessage());
            assertEquals(restStatus, elasticsearchException.status());
            assertEquals(1, elasticsearchException.getSuppressed().length);
            assertSame(responseException, elasticsearchException.getSuppressed()[0]);
        }
        {
            RestStatus restStatus = randomFrom(RestStatus.values());
            HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocolVersion,
                    restStatus.getStatus(), restStatus.name()));
            httpResponse.setEntity(new StringEntity("{\"error\":",
                    ContentType.APPLICATION_JSON));
            Response response = new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
            ResponseException responseException = new ResponseException(response);
            ElasticsearchException elasticsearchException = RestHighLevelClient.parseResponseException(responseException);
            assertEquals("unable to parse response body", elasticsearchException.getMessage());
            assertThat(elasticsearchException.getCause(), instanceOf(IOException.class));
            assertEquals(restStatus, elasticsearchException.status());
            assertEquals(1, elasticsearchException.getSuppressed().length);
            assertSame(responseException, elasticsearchException.getSuppressed()[0]);
        }
    }

    public void testWrapResponseListenerOnSuccess() throws IOException {
        ProtocolVersion protocolVersion = new ProtocolVersion("http", 1, 1);
        BasicRequestLine requestLine = new BasicRequestLine("GET", "/", protocolVersion);
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = RestHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(), trackingActionListener);
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocolVersion,
                restStatus.getStatus(), restStatus.name()));
        responseListener.onSuccess(new Response(requestLine, new HttpHost("localhost", 9200), httpResponse));
        assertNull(trackingActionListener.exception.get());
        assertEquals(restStatus.getStatus(), trackingActionListener.statusCode.get());
    }

    public void testWrapResponseListenerOnException() throws IOException {
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = RestHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(), trackingActionListener);
        IllegalStateException exception = new IllegalStateException();
        responseListener.onFailure(exception);
        assertSame(exception, trackingActionListener.exception.get());
    }

    public void testWrapResponseListenerOnResponseExceptionWithoutEntity() throws IOException {
        ProtocolVersion protocolVersion = new ProtocolVersion("http", 1, 1);
        BasicRequestLine requestLine = new BasicRequestLine("GET", "/", protocolVersion);
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = RestHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(), trackingActionListener);
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocolVersion,
                restStatus.getStatus(), restStatus.name()));
        Response response = new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(ElasticsearchException.class));
        ElasticsearchException elasticsearchException = (ElasticsearchException) trackingActionListener.exception.get();
        assertEquals(responseException.getMessage(), elasticsearchException.getMessage());
        assertEquals(restStatus, elasticsearchException.status());
        assertEquals(1, elasticsearchException.getSuppressed().length);
        assertSame(responseException, elasticsearchException.getSuppressed()[0]);
    }

    public void testWrapResponseListenerOnResponseExceptionWithEntity() throws IOException {
        ProtocolVersion protocolVersion = new ProtocolVersion("http", 1, 1);
        BasicRequestLine requestLine = new BasicRequestLine("GET", "/", protocolVersion);
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = RestHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(), trackingActionListener);
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocolVersion,
                restStatus.getStatus(), restStatus.name()));
        httpResponse.setEntity(new StringEntity("{\"error\":\"test error message\",\"status\":" + restStatus.getStatus() + "}",
                ContentType.APPLICATION_JSON));
        Response response = new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(ElasticsearchException.class));
        ElasticsearchException elasticsearchException = (ElasticsearchException)trackingActionListener.exception.get();
        assertEquals("Elasticsearch exception [type=exception, reason=test error message]", elasticsearchException.getMessage());
        assertEquals(restStatus, elasticsearchException.status());
        assertEquals(1, elasticsearchException.getSuppressed().length);
        assertSame(responseException, elasticsearchException.getSuppressed()[0]);
    }

    public void testWrapResponseListenerOnResponseExceptionWithBrokenEntity() throws IOException {
        ProtocolVersion protocolVersion = new ProtocolVersion("http", 1, 1);
        BasicRequestLine requestLine = new BasicRequestLine("GET", "/", protocolVersion);
        TrackingActionListener trackingActionListener = new TrackingActionListener();
        ResponseListener responseListener = RestHighLevelClient.wrapResponseListener(
                response -> response.getStatusLine().getStatusCode(), trackingActionListener);
        RestStatus restStatus = randomFrom(RestStatus.values());
        HttpResponse httpResponse = new BasicHttpResponse(new BasicStatusLine(protocolVersion,
                restStatus.getStatus(), restStatus.name()));
        httpResponse.setEntity(new StringEntity("{\"error\":",
                ContentType.APPLICATION_JSON));
        Response response = new Response(requestLine, new HttpHost("localhost", 9200), httpResponse);
        ResponseException responseException = new ResponseException(response);
        responseListener.onFailure(responseException);
        assertThat(trackingActionListener.exception.get(), instanceOf(ElasticsearchException.class));
        ElasticsearchException elasticsearchException = (ElasticsearchException)trackingActionListener.exception.get();
        assertEquals("unable to parse response body", elasticsearchException.getMessage());
        assertThat(elasticsearchException.getCause(), instanceOf(IOException.class));
        assertEquals(restStatus, elasticsearchException.status());
        assertEquals(1, elasticsearchException.getSuppressed().length);
        assertSame(responseException, elasticsearchException.getSuppressed()[0]);
    }

    private static class TrackingActionListener implements ActionListener<Integer> {
        private final AtomicInteger statusCode = new AtomicInteger(-1);
        private final AtomicReference<Exception> exception = new AtomicReference<>();

        @Override
        public void onResponse(Integer statusCode) {
            assertTrue(this.statusCode.compareAndSet(-1, statusCode));
        }

        @Override
        public void onFailure(Exception e) {
            assertTrue(exception.compareAndSet(null, e));
        }
    }

    private static class HeadersVarargMatcher extends ArgumentMatcher<Header[]> implements VarargMatcher {
        private Header[] expectedHeaders;

        HeadersVarargMatcher(Header... expectedHeaders) {
            this.expectedHeaders = expectedHeaders;
        }

        @Override
        public boolean matches(Object varargArgument) {
            if (varargArgument instanceof Header[]) {
                Header[] actualHeaders = (Header[]) varargArgument;
                return new ArrayEquals(expectedHeaders).matches(actualHeaders);
            }
            return false;
        }
    }

    private static StatusLine newStatusLine(int statusCode) {
        return new BasicStatusLine(new ProtocolVersion("http", 1, 1), statusCode, "");
    }
}
