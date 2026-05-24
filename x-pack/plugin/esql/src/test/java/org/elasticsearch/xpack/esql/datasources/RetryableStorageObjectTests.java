/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RetryableStorageObjectTests extends ESTestCase {

    /**
     * Regression guard: {@code abortStream} must forward directly to the delegate, not fall
     * through to the SPI default (which is a draining {@code stream.close()} on providers like
     * S3). The original bug was a missing override on a decorator silently swallowing the abort.
     */
    public void testAbortStreamDelegates() throws IOException {
        StorageObject delegate = mock(StorageObject.class);
        InputStream stream = new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8));

        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(3, 1, 10));
        obj.abortStream(stream);

        verify(delegate).abortStream(stream);
    }

    /**
     * Regression guard: abort is a best-effort connection-discard, not a retryable I/O. If the
     * delegate's {@code abortStream} throws, the exception must propagate unchanged — wrapping
     * abort in retry logic would defeat the bounded-latency contract the abort path provides.
     */
    public void testAbortStreamDoesNotRetryOnFailure() throws IOException {
        StorageObject delegate = mock(StorageObject.class);
        InputStream stream = new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8));
        doThrow(new IOException("abort failed")).when(delegate).abortStream(any(InputStream.class));

        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(5, 1, 10));
        IOException thrown = expectThrows(IOException.class, () -> obj.abortStream(stream));
        assertEquals("abort failed", thrown.getMessage());

        verify(delegate, times(1)).abortStream(stream);
    }

    /**
     * Regression guard: the wrapper must not unwrap or alter the stream instance — the SPI
     * contract requires the delegate to receive the exact instance it returned from
     * {@code newStream()}, otherwise provider-specific abort dispatch (e.g. the S3
     * {@code Abortable} cast) silently falls back to a draining close.
     */
    public void testAbortStreamForwardsSameInstance() throws IOException {
        StorageObject delegate = mock(StorageObject.class);
        InputStream stream = new ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8));
        InputStream[] captured = new InputStream[1];
        doAnswer(inv -> {
            captured[0] = inv.getArgument(0);
            return null;
        }).when(delegate).abortStream(any(InputStream.class));

        RetryableStorageObject obj = new RetryableStorageObject(delegate, new RetryPolicy(3, 1, 10));
        obj.abortStream(stream);

        assertSame(stream, captured[0]);
    }
}
