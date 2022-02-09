/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url.http;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.StringContains.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryingHttpInputStreamTests extends ESTestCase {
    public void testUnderlyingInputStreamIsAbortedAfterAFailureAndRetries() throws Exception {
        final URI blobURI = new URI("blob");
        final int blobSize = randomIntBetween(20, 1024);
        final int firstChunkSize = randomIntBetween(1, blobSize - 1);

        final HttpResponseInputStream firstHttpResponseInputStream = mock(HttpResponseInputStream.class);
        when(firstHttpResponseInputStream.read(any(), anyInt(), anyInt())).thenReturn(firstChunkSize).thenThrow(new IOException());
        final Map<String, String> firstResponseHeaders = Map.of("Content-Length", Integer.toString(blobSize));

        final HttpResponseInputStream secondHttpResponseInputStream = mock(HttpResponseInputStream.class);
        when(secondHttpResponseInputStream.read(any(), anyInt(), anyInt())).thenReturn(blobSize - firstChunkSize).thenReturn(-1);
        final Map<String, String> secondResponseHeaders = Map.of(
            "Content-Range",
            String.format(Locale.ROOT, "bytes %d-%d/%d", firstChunkSize, blobSize - 1, blobSize)
        );

        final List<MockHttpResponse> responses = List.of(
            new MockHttpResponse(firstHttpResponseInputStream, RestStatus.OK.getStatus(), firstResponseHeaders),
            new MockHttpResponse(secondHttpResponseInputStream, RestStatus.PARTIAL_CONTENT.getStatus(), secondResponseHeaders) {
                @Override
                protected void assertExpectedRequestHeaders(Map<String, String> requestHeaders) {
                    assertThat("Expected a Range request but it wasn't", requestHeaders.containsKey("Range"), equalTo(true));
                }
            }
        );

        final Iterator<MockHttpResponse> responsesIterator = responses.iterator();

        final URLHttpClient urlHttpClient = new URLHttpClient(null, null) {
            @Override
            public HttpResponse get(URI uri, Map<String, String> headers) {
                assert responsesIterator.hasNext() : "Expected less calls";

                final MockHttpResponse mockHttpResponse = responsesIterator.next();
                mockHttpResponse.assertExpectedRequestHeaders(headers);
                return mockHttpResponse;
            }
        };

        Streams.readFully(new RetryingHttpInputStream("blob", blobURI, urlHttpClient, 1));

        verify(firstHttpResponseInputStream, times(1)).close();
        verify(firstHttpResponseInputStream, times(1)).abort();

        verify(secondHttpResponseInputStream, times(1)).close();
    }

    public void testClosesTheResponseAndTheInputStreamWhenTheResponseIsUnexpected() throws Exception {
        final URI blobURI = new URI("blob");
        final AtomicInteger closed = new AtomicInteger(0);
        final HttpResponseInputStream httpResponseInputStream = mock(HttpResponseInputStream.class);
        when(httpResponseInputStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException());
        String errorMessage = randomAlphaOfLength(100);
        int statusCode = randomFrom(RestStatus.CREATED.getStatus(), RestStatus.ACCEPTED.getStatus(), RestStatus.NO_CONTENT.getStatus());

        final URLHttpClient urlHttpClient = new URLHttpClient(null, null) {
            @Override
            public HttpResponse get(URI uri, Map<String, String> headers) {
                return new HttpResponse() {
                    @Override
                    public HttpResponseInputStream getInputStream() throws IOException {
                        return httpResponseInputStream;
                    }

                    @Override
                    public int getStatusCode() {
                        return statusCode;
                    }

                    @Override
                    public String getHeader(String headerName) {
                        return null;
                    }

                    @Override
                    public String getBodyAsString(int maxSize) {
                        IOUtils.closeWhileHandlingException(httpResponseInputStream);
                        return errorMessage;
                    }

                    @Override
                    public void close() throws IOException {
                        closed.incrementAndGet();
                    }
                };
            }
        };

        final IOException exception = expectThrows(
            IOException.class,
            () -> Streams.readFully(new RetryingHttpInputStream("blob", blobURI, urlHttpClient, 0))
        );

        assertThat(closed.get(), equalTo(1));
        verify(httpResponseInputStream, times(1)).close();
        assertThat(exception.getMessage(), containsString(errorMessage));
        assertThat(exception.getMessage(), containsString(Integer.toString(statusCode)));
    }

    public void testRetriesTheRequestAfterAFailureUpToMaxRetries() throws Exception {
        final URI blobURI = new URI("blob");
        final int maxRetries = randomIntBetween(0, 5);
        final AtomicInteger attempts = new AtomicInteger(0);

        final URLHttpClient urlHttpClient = new URLHttpClient(null, null) {
            @Override
            public HttpResponse get(URI uri, Map<String, String> headers) throws IOException {
                attempts.incrementAndGet();
                if (randomBoolean()) {
                    final Integer statusCode = randomFrom(
                        RestStatus.INTERNAL_SERVER_ERROR.getStatus(),
                        RestStatus.SERVICE_UNAVAILABLE.getStatus()
                    );
                    throw new URLHttpClientException(statusCode, "Server error");
                } else {
                    throw new URLHttpClientIOException("Unable to execute request", new IOException());
                }
            }
        };

        expectThrows(IOException.class, () -> Streams.readFully(new RetryingHttpInputStream("blob", blobURI, urlHttpClient, maxRetries)));

        assertThat(attempts.get(), equalTo(maxRetries + 1));
    }

    public void testFailsImmediatelyAfterNotFoundResponse() throws Exception {
        final URI blobURI = new URI("blob");
        final int maxRetries = randomIntBetween(0, 5);
        final AtomicInteger attempts = new AtomicInteger(0);

        final URLHttpClient urlHttpClient = new URLHttpClient(null, null) {
            @Override
            public HttpResponse get(URI uri, Map<String, String> headers) {
                attempts.incrementAndGet();
                throw new URLHttpClientException(RestStatus.NOT_FOUND.getStatus(), "Not-Found");
            }
        };

        expectThrows(IOException.class, () -> Streams.readFully(new RetryingHttpInputStream("blob", blobURI, urlHttpClient, maxRetries)));

        assertThat(attempts.get(), equalTo(1));
    }

    static class MockHttpResponse implements URLHttpClient.HttpResponse {
        private final HttpResponseInputStream inputStream;
        private final int statusCode;
        private final Map<String, String> headers;

        MockHttpResponse(HttpResponseInputStream inputStream, int statusCode, Map<String, String> headers) {
            this.inputStream = inputStream;
            this.statusCode = statusCode;
            this.headers = headers;
        }

        @Override
        public HttpResponseInputStream getInputStream() {
            return inputStream;
        }

        @Override
        public int getStatusCode() {
            return statusCode;
        }

        @Override
        public String getHeader(String headerName) {
            return headers.get(headerName);
        }

        @Override
        public void close() {}

        @Override
        public String getBodyAsString(int maxSize) {
            return null;
        }

        protected void assertExpectedRequestHeaders(Map<String, String> requestHeaders) {

        }
    }
}
