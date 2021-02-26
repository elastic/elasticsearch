/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore.url.http;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryingHttpInputStreamTests extends ESTestCase {
    public void testAborts() throws Exception {
        final URI blobURI = new URI("blob");
        final HttpResponseInputStream httpResponseInputStream = mock(HttpResponseInputStream.class);
        when(httpResponseInputStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException());

        final URLHttpClient urlHttpClient = new URLHttpClient(null, null) {
            @Override
            public HttpResponse get(URI uri, Map<String, String> headers) {
                return new URLHttpClient.HttpResponse() {
                    @Override
                    public HttpResponseInputStream getInputStream() {
                        return httpResponseInputStream;
                    }

                    @Override
                    public int getStatusCode() {
                        return 200;
                    }

                    @Override
                    public String getHeader(String headerName) {
                        if (headerName.equals("Content-Length")) {
                            return Integer.toString(randomIntBetween(1, 1000));
                        }
                        return null;
                    }

                    @Override
                    public void close() throws IOException {
                    }
                };
            }
        };

        expectThrows(IOException.class, () -> {
            RetryingHttpInputStream inputStream = new RetryingHttpInputStream("blob", blobURI, urlHttpClient, 0);
            Streams.readFully(inputStream);
        });

        verify(httpResponseInputStream, times(1)).abort();
        verify(httpResponseInputStream, times(1)).close();
    }

    public void testAbortsHalfWay() throws Exception {
        final URI blobURI = new URI("blob");
        final HttpResponseInputStream httpResponseInputStream = mock(HttpResponseInputStream.class);
        when(httpResponseInputStream.read(any(), anyInt(), anyInt())).thenReturn(1).thenThrow(new IOException());

        final URLHttpClient urlHttpClient = new URLHttpClient(null, null) {
            @Override
            public HttpResponse get(URI uri, Map<String, String> headers) {
                return new URLHttpClient.HttpResponse() {
                    @Override
                    public HttpResponseInputStream getInputStream() {
                        return httpResponseInputStream;
                    }

                    @Override
                    public int getStatusCode() {
                        return 200;
                    }

                    @Override
                    public String getHeader(String headerName) {
                        if (headerName.equals("Content-Length")) {
                            return "900";
                        }
                        return null;
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };

        expectThrows(IOException.class, () -> {
            RetryingHttpInputStream inputStream = new RetryingHttpInputStream("blob", blobURI, urlHttpClient, 0);
            Streams.readFully(inputStream);
        });

        verify(httpResponseInputStream, times(1)).abort();
        verify(httpResponseInputStream, times(1)).close();
    }

    public void testRetriesTheRequest() throws Exception {
        final URI blobURI = new URI("blob");
        final int maxRetries = randomIntBetween(0, 5);
        final AtomicInteger attempts = new AtomicInteger(0);

        final URLHttpClient urlHttpClient = new URLHttpClient(null, null) {
            @Override
            public HttpResponse get(URI uri, Map<String, String> headers) {
                attempts.incrementAndGet();
                final Integer statusCode =
                    randomFrom(RestStatus.INTERNAL_SERVER_ERROR.getStatus(), RestStatus.SERVICE_UNAVAILABLE.getStatus());
                throw new URLHttpClientException(statusCode);
            }
        };

        expectThrows(IOException.class, () -> {
            RetryingHttpInputStream inputStream = new RetryingHttpInputStream("blob", blobURI, urlHttpClient, maxRetries);
            Streams.readFully(inputStream);
        });

        assertThat(attempts.get(), equalTo(maxRetries + 1));
    }

    public void testCloses() throws Exception {
        final URI blobURI = new URI("blob");
        final AtomicInteger closed = new AtomicInteger(0);
        final HttpResponseInputStream httpResponseInputStream = mock(HttpResponseInputStream.class);
        when(httpResponseInputStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException());

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
                        return randomFrom(RestStatus.CREATED.getStatus(),
                            RestStatus.ACCEPTED.getStatus(),
                            RestStatus.NO_CONTENT.getStatus());
                    }

                    @Override
                    public String getHeader(String headerName) {
                        return null;
                    }

                    @Override
                    public void close() throws IOException {
                        closed.incrementAndGet();
                    }
                };
            }
        };

        expectThrows(IOException.class, () -> {
            RetryingHttpInputStream inputStream = new RetryingHttpInputStream("blob", blobURI, urlHttpClient, 0);
            Streams.readFully(inputStream);
        });

        assertThat(closed.get(), equalTo(1));
        verify(httpResponseInputStream, times(1)).close();
    }
}
