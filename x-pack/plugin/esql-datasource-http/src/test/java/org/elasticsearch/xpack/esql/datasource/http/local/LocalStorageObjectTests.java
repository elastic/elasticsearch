/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.http.local;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Covers the {@code READ_TO_END} open-ended path, including the hand-rolled unbounded {@code RangeInputStream}
 * that reads to EOF (no byte cap) — the form whole-object resume re-opens.
 */
public class LocalStorageObjectTests extends ESTestCase {

    public void testReadToEndReadsFromPositionToEof() throws IOException {
        byte[] data = "Hello, World! This is local read-to-end data.".getBytes(StandardCharsets.UTF_8);
        Path file = createTempFile();
        Files.write(file, data);
        LocalStorageObject obj = new LocalStorageObject(file);

        try (InputStream stream = obj.newStream(7, StorageObject.READ_TO_END)) {
            byte[] got = stream.readAllBytes();
            assertArrayEquals(Arrays.copyOfRange(data, 7, data.length), got);
        }
    }

    public void testReadToEndFromZeroEqualsWholeFile() throws IOException {
        byte[] data = randomByteArrayOfLength(between(64, 1024));
        Path file = createTempFile();
        Files.write(file, data);
        LocalStorageObject obj = new LocalStorageObject(file);

        try (InputStream stream = obj.newStream(0, StorageObject.READ_TO_END)) {
            assertArrayEquals(data, stream.readAllBytes());
        }
    }

    public void testReadToEndSingleByteReadsMatchBulk() throws IOException {
        byte[] data = randomByteArrayOfLength(between(8, 64));
        Path file = createTempFile();
        Files.write(file, data);
        LocalStorageObject obj = new LocalStorageObject(file);

        // Exercise the unbounded single-byte read() path (no remaining cap) and confirm it terminates at EOF.
        try (InputStream stream = obj.newStream(0, StorageObject.READ_TO_END)) {
            byte[] got = new byte[data.length];
            for (int i = 0; i < data.length; i++) {
                int b = stream.read();
                assertNotEquals("premature EOF at " + i, -1, b);
                got[i] = (byte) b;
            }
            assertEquals("must be exactly at EOF after the last byte", -1, stream.read());
            assertArrayEquals(data, got);
        }
    }

    public void testNegativeLengthOtherThanSentinelThrows() throws IOException {
        Path file = createTempFile();
        Files.write(file, new byte[10]);
        LocalStorageObject obj = new LocalStorageObject(file);
        // -1 is the READ_TO_END sentinel; any other negative length is still rejected.
        expectThrows(IllegalArgumentException.class, () -> obj.newStream(0, -2));
    }
}
