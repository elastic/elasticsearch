/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class BigBytesStreamInputTest extends ESTestCase {
    public void testCreation() throws Exception {
        ByteArray byteArray = new BigArrays(null, null, null).newByteArray(1000);
        byte b[] = "Test String".getBytes(StandardCharsets.UTF_8);
        byteArray.set(0, b, 0, b.length);
        BigBytesStreamInput input = new BigBytesStreamInput(byteArray, b.length);
        assertEquals(b.length, input.available());
        byte b2[] = new byte[b.length + 1];
        assertEquals(b.length, input.read(b2, 0, b2.length));
        assertArrayEquals(b, Arrays.copyOf(b2, b.length));
        assertEquals(0, input.available());
        assertEquals(-1, input.read(b2, 0, b2.length));
    }

    public void testReadArray() throws Exception {
        ByteArray byteArray = new BigArrays(null, null, null).newByteArray(1000);
        byte b[] = "Test String".getBytes(StandardCharsets.UTF_8);

        byteArray.set(0, b, 0, b.length);
        BigBytesStreamInput input = new BigBytesStreamInput(byteArray, b.length);
        assertEquals(b.length, input.available());
        byte b2[] = new byte[b.length + 1];
        assertEquals(b.length, input.read(b2));
        assertArrayEquals(b, Arrays.copyOf(b2, b.length));
        assertEquals(0, input.available());
        assertEquals(-1, input.read(b2, 0, b2.length));
    }

    public void testRead() throws Exception {
        ByteArray byteArray = new BigArrays(null, null, null).newByteArray(1000);
        byte b[] = "Test String".getBytes(StandardCharsets.UTF_8);

        byteArray.set(0, b, 0, b.length);
        BigBytesStreamInput input = new BigBytesStreamInput(byteArray, b.length);
        for (int i = 0; i < b.length; i++) {
            assertEquals(b[i], input.read());
        }

        assertEquals(-1, input.read());

        assertEquals(0, input.available());

    }

    public void testReadAllBytes() throws Exception {
        ByteArray byteArray = new BigArrays(null, null, null).newByteArray(1000);
        byte b[] = "Test String".getBytes(StandardCharsets.UTF_8);
        byteArray.set(0, b, 0, b.length);
        BigBytesStreamInput input = new BigBytesStreamInput(byteArray, b.length);
        assertEquals(b.length, input.available());
        byte b2[] = input.readAllBytes();
        assertEquals(b.length, b2.length);
        assertArrayEquals(b, b2);
        assertEquals(0, input.available());
        assertEquals(0, input.readAllBytes().length);
    }

    public void testClose() throws Exception {
        ByteArray byteArray = new BigArrays(null, null, null).newByteArray(1000);
        byte b[] = "Test String".getBytes(StandardCharsets.UTF_8);
        byteArray.set(0, b, 0, b.length);
        BigBytesStreamInput input = new BigBytesStreamInput(byteArray, b.length);
        assertEquals(b.length, input.available());
        input.close();
        assertEquals(0, input.readAllBytes().length);
    }
}
