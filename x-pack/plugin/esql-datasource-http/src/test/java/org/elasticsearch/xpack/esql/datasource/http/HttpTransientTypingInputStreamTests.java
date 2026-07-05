/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Verifies that {@link HttpTransientTypingInputStream} re-types a mid-read fault into a
 * {@link ExternalUnavailableException} so the provider-agnostic resume loop engages — the JDK
 * {@code HttpClient} body stream throws a <b>plain {@link IOException}</b> on a content-length-short premature
 * EOF (the idle-drop case), which the resume classifier would otherwise treat as a hard error and never resume.
 * Unlike the cloud providers an HTTP mid-body fault carries no status, so it is always transient, never throttling.
 */
public class HttpTransientTypingInputStreamTests extends ESTestCase {

    private static final StoragePath PATH = StoragePath.of("https://host/path/object.ndjson");

    public void testMidReadPlainIOExceptionRetypedTransientNotThrottling() throws IOException {
        HttpTransientTypingInputStream wrapped = new HttpTransientTypingInputStream(
            faultingStream("fixed content-length: 1024, bytes received: 512"),
            PATH
        );
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, wrapped::read);
        assertFalse("an HTTP mid-body drop carries no status, so it is transient but not throttling", e.throttling());
        assertThat(e.getCause(), org.hamcrest.Matchers.instanceOf(IOException.class));
    }

    public void testMidReadPlainIOExceptionViaBulkReadRetyped() throws IOException {
        HttpTransientTypingInputStream wrapped = new HttpTransientTypingInputStream(faultingStream("connection reset"), PATH);
        ExternalUnavailableException e = expectThrows(ExternalUnavailableException.class, () -> wrapped.read(new byte[16], 0, 16));
        assertFalse(e.throttling());
    }

    public void testCleanReadPassesThroughUntyped() throws IOException {
        byte[] payload = "hello-world".getBytes(StandardCharsets.UTF_8);
        try (HttpTransientTypingInputStream wrapped = new HttpTransientTypingInputStream(new ByteArrayInputStream(payload), PATH)) {
            byte[] out = wrapped.readAllBytes();
            assertArrayEquals("a fault-free read must be untouched by the typing wrapper", payload, out);
        }
    }

    /** A stream that throws a plain {@link IOException} on first read — the JDK HttpClient premature-EOF shape. */
    private static InputStream faultingStream(String message) {
        return new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException(message);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                throw new IOException(message);
            }
        };
    }
}
