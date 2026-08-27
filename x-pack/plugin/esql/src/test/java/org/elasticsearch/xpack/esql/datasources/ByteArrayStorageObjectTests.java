/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteArrayStorageObjectTests extends ESTestCase {

    public void testNewStreamReadsFullContent() throws IOException {
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
        ByteArrayStorageObject obj = new ByteArrayStorageObject(StoragePath.of("mem://test"), data, 0, data.length);

        try (InputStream stream = obj.newStream()) {
            assertEquals("hello world", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    public void testNewStreamWithOffset() throws IOException {
        byte[] data = "XXXhelloXXX".getBytes(StandardCharsets.UTF_8);
        ByteArrayStorageObject obj = new ByteArrayStorageObject(StoragePath.of("mem://test"), data, 3, 5);

        try (InputStream stream = obj.newStream()) {
            assertEquals("hello", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    public void testNewStreamRange() throws IOException {
        byte[] data = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        ByteArrayStorageObject obj = new ByteArrayStorageObject(StoragePath.of("mem://test"), data, 0, data.length);

        try (InputStream stream = obj.newStream(3, 4)) {
            assertEquals("defg", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    public void testLength() throws IOException {
        byte[] data = new byte[42];
        ByteArrayStorageObject obj = new ByteArrayStorageObject(StoragePath.of("mem://test"), data, 10, 20);
        assertEquals(20, obj.length());
    }

    public void testReadBytesIntoHeapBuffer() throws IOException {
        byte[] data = "abcdef".getBytes(StandardCharsets.UTF_8);
        ByteArrayStorageObject obj = new ByteArrayStorageObject(StoragePath.of("mem://test"), data, 0, data.length);

        ByteBuffer buf = ByteBuffer.allocate(3);
        int read = obj.readBytes(2, buf);
        assertEquals(3, read);
        buf.flip();
        byte[] result = new byte[3];
        buf.get(result);
        assertEquals("cde", new String(result, StandardCharsets.UTF_8));
    }

    public void testReadBytesPastEnd() throws IOException {
        byte[] data = "abc".getBytes(StandardCharsets.UTF_8);
        ByteArrayStorageObject obj = new ByteArrayStorageObject(StoragePath.of("mem://test"), data, 0, data.length);

        assertEquals(-1, obj.readBytes(3, ByteBuffer.allocate(1)));
    }

    public void testExists() throws IOException {
        ByteArrayStorageObject obj = new ByteArrayStorageObject(StoragePath.of("mem://test"), new byte[0], 0, 0);
        assertTrue(obj.exists());
    }

    public void testPath() {
        StoragePath path = StoragePath.of("mem://test/file.ndjson");
        ByteArrayStorageObject obj = new ByteArrayStorageObject(path, new byte[0], 0, 0);
        assertEquals(path, obj.path());
    }

    public void testConstructorRejectsOverflowingOffsetPlusLength() {
        // offset + length silently wrapped to negative before the addExact guard, so the < data.length
        // check passed and a downstream read could touch arbitrary bytes.
        byte[] data = new byte[16];
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new ByteArrayStorageObject(StoragePath.of("mem://test"), data, Integer.MAX_VALUE - 8, Integer.MAX_VALUE - 8)
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("overflows int"));
    }

    public void testNewStreamSubRegionRejectsNegativePosition() {
        byte[] data = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        ByteArrayStorageObject obj = new ByteArrayStorageObject(StoragePath.of("mem://test"), data, 0, data.length);
        expectThrows(IllegalArgumentException.class, () -> obj.newStream(-1, 4));
    }

    public void testNewStreamSubRegionRejectsReadPastLogicalEnd() {
        // Data backing array is 32 bytes but the logical region is the inner [10, 30) window. A caller
        // passing position+len past `length` was allowed pre-fix because ByteArrayInputStream only
        // validated against data.length, exposing adjacent bytes.
        byte[] data = new byte[32];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        ByteArrayStorageObject obj = new ByteArrayStorageObject(StoragePath.of("mem://test"), data, 10, 20);
        expectThrows(IllegalArgumentException.class, () -> obj.newStream(15, 10));
    }
}
