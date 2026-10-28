/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.azure;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpMethod;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.storage.blob.models.BlobStorageException;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * Verifies that {@link AzureTransientTypingInputStream} re-types a mid-read fault into a
 * {@link ExternalUnavailableException} so the provider-agnostic resume loop engages.
 * <p>
 * The Azure {@code BlobInputStream} never lets a {@link BlobStorageException} escape the stream:
 * {@code dispatchRead} catches it and rethrows it wrapped in a plain {@link IOException} (its cause). These tests
 * feed that real wrapped shape — {@code new IOException(blobStorageException)} — not a raw {@code BlobStorageException},
 * so they exercise the exact shape a live blob read produces.
 */
public class AzureTransientTypingInputStreamTests extends ESTestCase {

    private static final StoragePath PATH = StoragePath.of("https://acct.blob.core.windows.net/c/b");

    public void testMidReadBlobStorageException503RetypedAsThrottling() throws IOException {
        AzureTransientTypingInputStream wrapped = new AzureTransientTypingInputStream(faultingStream(blobStorageException(503)), PATH);
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, wrapped::read);
        assertTrue("a 503 surfaced mid-read is throttling", e.throttling());
    }

    public void testMidReadBlobStorageException429RetypedAsThrottling() throws IOException {
        AzureTransientTypingInputStream wrapped = new AzureTransientTypingInputStream(faultingStream(blobStorageException(429)), PATH);
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, () -> wrapped.read(new byte[8], 0, 8));
        assertTrue("a 429 surfaced mid-read is throttling", e.throttling());
    }

    public void testMidReadBlobStorageException500IsTransientNotThrottling() throws IOException {
        AzureTransientTypingInputStream wrapped = new AzureTransientTypingInputStream(faultingStream(blobStorageException(500)), PATH);
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, wrapped::read);
        assertFalse("500 mid-read is transient but not throttling", e.throttling());
    }

    public void testMidReadPlainTransportIOExceptionIsTransientNotThrottling() throws IOException {
        // A transport drop with no BlobStorageException cause is still re-typed transient so the resume engages.
        AzureTransientTypingInputStream wrapped = new AzureTransientTypingInputStream(faultingStream(null), PATH);
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, wrapped::read);
        assertFalse("a plain transport IOException is transient but not throttling", e.throttling());
    }

    public void testMidReadBlobStorageExceptionNestedDeeperStillFlaggedThrottling() throws IOException {
        // The throttle status must be found even if a future SDK nests the BlobStorageException one level deeper:
        // the wrapper walks the cause chain rather than reading only the immediate cause.
        IOException nested = new IOException("outer", new RuntimeException(blobStorageException(503)));
        AzureTransientTypingInputStream wrapped = new AzureTransientTypingInputStream(throwingStream(nested), PATH);
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, wrapped::read);
        assertTrue("a 503 nested deeper in the cause chain is still throttling", e.throttling());
    }

    /**
     * A stream that throws the real wrapped shape on first read: {@code IOException(BlobStorageException)}, or a bare
     * {@code IOException} when {@code cause} is null.
     */
    private static InputStream faultingStream(BlobStorageException cause) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                throw cause == null ? new IOException("connection reset") : new IOException(cause);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throw cause == null ? new IOException("connection reset") : new IOException(cause);
            }
        };
    }

    /** A stream that throws the given {@link IOException} (with whatever cause chain it carries) on first read. */
    private static InputStream throwingStream(IOException toThrow) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                throw toThrow;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throw toThrow;
            }
        };
    }

    private static BlobStorageException blobStorageException(int status) {
        return new BlobStorageException("status " + status, new StatusOnlyHttpResponse(status), null);
    }

    /** Minimal real {@link HttpResponse} that only carries a status code — enough to build a {@link BlobStorageException}. */
    private static final class StatusOnlyHttpResponse extends HttpResponse {
        private final int status;

        StatusOnlyHttpResponse(int status) {
            super(new HttpRequest(HttpMethod.GET, "https://acct.blob.core.windows.net/c/b"));
            this.status = status;
        }

        @Override
        public int getStatusCode() {
            return status;
        }

        @Override
        public String getHeaderValue(String name) {
            return null;
        }

        @Override
        public HttpHeaders getHeaders() {
            return new HttpHeaders();
        }

        @Override
        public Flux<ByteBuffer> getBody() {
            return Flux.empty();
        }

        @Override
        public Mono<byte[]> getBodyAsByteArray() {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString() {
            return Mono.empty();
        }

        @Override
        public Mono<String> getBodyAsString(Charset charset) {
            return Mono.empty();
        }
    }
}
