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
}
