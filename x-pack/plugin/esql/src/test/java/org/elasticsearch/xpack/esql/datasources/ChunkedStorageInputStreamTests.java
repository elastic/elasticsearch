/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Request-shape tests for {@link ChunkedStorageInputStream}. What matters here is not only that the bytes come
 * out right, but that they arrive as bounded closed ranges that are read to completion — that property is what
 * lets the provider pool the connection, and it is the whole reason this stream exists.
 */
public class ChunkedStorageInputStreamTests extends ESTestCase {

    public void testBoundaryInsideFirstChunkCostsOneRequest() throws IOException {
        byte[] payload = "line one\nline two\n".getBytes(StandardCharsets.UTF_8);
        RecordingStorageObject object = new RecordingStorageObject(payload);

        try (InputStream stream = new ChunkedStorageInputStream(object, 0, payload.length)) {
            assertEquals('l', stream.read());
        }

        assertEquals("a read inside the first chunk must not fetch a second", 1, object.readBytesCalls.size());
        assertEquals("the probe path must never open a raw stream", 0, object.newStreamCalls.size());
        assertEquals("the probe path must never abort", 0, object.abortCalls.get());
    }

    public void testChunkSizesGrowGeometricallyAndAreCapped() throws IOException {
        byte[] payload = new byte[600];
        RecordingStorageObject object = new RecordingStorageObject(payload);

        // firstChunk 32, cap 128: expect 32, 64, 128, 128, ...
        try (InputStream stream = new ChunkedStorageInputStream(object, 0, payload.length, 32, 128)) {
            assertEquals(payload.length, stream.readAllBytes().length);
        }

        assertThat(object.readBytesCalls.get(0)[1], Matchers.equalTo(32L));
        assertThat(object.readBytesCalls.get(1)[1], Matchers.equalTo(64L));
        assertThat(object.readBytesCalls.get(2)[1], Matchers.equalTo(128L));
        assertThat(object.readBytesCalls.get(3)[1], Matchers.equalTo(128L));
    }

    public void testNoRequestReachesPastTheDeclaredEnd() throws IOException {
        byte[] payload = new byte[1000];
        RecordingStorageObject object = new RecordingStorageObject(payload);
        long end = 300;

        try (InputStream stream = new ChunkedStorageInputStream(object, 100, end, 64, 1024)) {
            assertEquals(200, stream.readAllBytes().length);
        }

        for (long[] call : object.readBytesCalls) {
            assertThat(
                "request [" + call[0] + "," + call[1] + ") must stay within the declared end",
                call[0] + call[1],
                Matchers.lessThanOrEqualTo(end)
            );
        }
    }

    public void testContentMatchesTheUnderlyingRangeAcrossChunkSizes() throws IOException {
        byte[] payload = randomByteArrayOfLength(randomIntBetween(1, 4096));
        int from = randomIntBetween(0, payload.length - 1);
        RecordingStorageObject object = new RecordingStorageObject(payload, randomIntBetween(1, 64));

        byte[] read;
        try (
            InputStream stream = new ChunkedStorageInputStream(
                object,
                from,
                payload.length,
                randomIntBetween(1, 128),
                randomIntBetween(128, 512)
            )
        ) {
            read = stream.readAllBytes();
        }

        byte[] expected = new byte[payload.length - from];
        System.arraycopy(payload, from, expected, 0, expected.length);
        assertArrayEquals("short positional reads must be refilled, not truncated", expected, read);
    }

    public void testSingleByteAndBulkReadsAgree() throws IOException {
        byte[] payload = randomByteArrayOfLength(randomIntBetween(1, 2048));
        RecordingStorageObject object = new RecordingStorageObject(payload);

        byte[] oneAtATime = new byte[payload.length];
        try (InputStream stream = new ChunkedStorageInputStream(object, 0, payload.length, 16, 64)) {
            for (int i = 0; i < payload.length; i++) {
                int b = stream.read();
                assertThat("stream ended early at " + i, b, Matchers.greaterThanOrEqualTo(0));
                oneAtATime[i] = (byte) b;
            }
            assertEquals("stream must be exhausted", -1, stream.read());
        }
        assertArrayEquals(payload, oneAtATime);
    }

    public void testEmptyRangeReadsAsEof() throws IOException {
        RecordingStorageObject object = new RecordingStorageObject(new byte[100]);

        try (InputStream stream = new ChunkedStorageInputStream(object, 50, 50)) {
            assertEquals(-1, stream.read());
            assertEquals(-1, stream.read(new byte[8], 0, 8));
        }

        assertEquals("an empty range must not issue a request", 0, object.readBytesCalls.size());
    }

    public void testEndBeyondObjectStopsAtObjectEnd() throws IOException {
        byte[] payload = new byte[10];
        RecordingStorageObject object = new RecordingStorageObject(payload);

        try (InputStream stream = new ChunkedStorageInputStream(object, 0, 1_000_000)) {
            assertEquals(payload.length, stream.readAllBytes().length);
            assertEquals(-1, stream.read());
        }
    }

    public void testZeroLengthReadReturnsZeroWithoutFetching() throws IOException {
        RecordingStorageObject object = new RecordingStorageObject(new byte[100]);

        try (InputStream stream = new ChunkedStorageInputStream(object, 0, 100)) {
            assertEquals(0, stream.read(new byte[8], 0, 0));
        }

        assertEquals(0, object.readBytesCalls.size());
    }
}
