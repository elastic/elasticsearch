/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TruncatedOutputStreamTests extends ESTestCase {

    public void testWriteSingleBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int maxSize = randomIntBetween(0, 100);
        TruncatedOutputStream truncatedOutputStream = new TruncatedOutputStream(
            byteArrayOutputStream,
            byteArrayOutputStream::size,
            maxSize
        );

        byte[] values = new byte[maxSize];

        // Write enough bytes within the defined maxSize
        for (int i = 0; i < maxSize; i++) {
            byte b = randomByte();
            truncatedOutputStream.write(b);
            values[i] = b;
        }

        // The stream should be truncated now that it is filled
        for (int i = 0; i < randomIntBetween(0, 20); i++) {
            truncatedOutputStream.write(randomByte());
        }

        assertArrayEquals(values, byteArrayOutputStream.toByteArray());
    }

    public void testWriteByteArray() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int maxSize = randomIntBetween(100, 200);
        TruncatedOutputStream truncatedOutputStream = new TruncatedOutputStream(
            byteArrayOutputStream,
            byteArrayOutputStream::size,
            maxSize
        );

        List<Byte> values = new ArrayList<>();
        int bytesWritten = 0;
        // Write beyond the streams capacity
        while (bytesWritten <= maxSize * 2) {
            byte[] bytes = randomByteArrayOfLength(randomIntBetween(0, 20));
            truncatedOutputStream.write(bytes);

            // If there was capacity before writing, then the stream wrote the entire array
            // even if that meant overflowing
            if (bytesWritten < maxSize) {
                for (byte b : bytes) {
                    values.add(b);
                }
            }

            bytesWritten += bytes.length;
        }

        byte[] valuesAsByteArray = new byte[values.size()];
        int i = 0;
        for (byte b : values) {
            valuesAsByteArray[i] = b;
            i++;
        }

        assertArrayEquals(valuesAsByteArray, byteArrayOutputStream.toByteArray());
    }

    public void testWriteByteArrayWithOffsetAndLength() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int maxSize = randomIntBetween(100, 200);
        TruncatedOutputStream truncatedOutputStream = new TruncatedOutputStream(
            byteArrayOutputStream,
            byteArrayOutputStream::size,
            maxSize
        );

        List<Byte> values = new ArrayList<>();
        int bytesWritten = 0;
        // Write beyond the streams capacity
        while (bytesWritten <= maxSize * 2) {
            byte[] bytes = randomByteArrayOfLength(randomIntBetween(0, 20));
            int offset = randomIntBetween(0, bytes.length);
            int length = randomIntBetween(0, bytes.length - offset);
            truncatedOutputStream.write(bytes, offset, length);

            // If there was capacity before writing, then the stream wrote the sub array
            // even if that meant overflowing
            if (bytesWritten < maxSize) {
                for (int i = offset; i < offset + length; i++) {
                    values.add(bytes[i]);
                }
            }

            bytesWritten += length;
        }

        byte[] valuesAsByteArray = new byte[values.size()];
        int i = 0;
        for (byte b : values) {
            valuesAsByteArray[i] = b;
            i++;
        }

        assertArrayEquals(valuesAsByteArray, byteArrayOutputStream.toByteArray());
    }
}
