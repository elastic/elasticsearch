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

public class BoundedOutputStreamTests extends ESTestCase {

    public void testWriteSingleBytes() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int maxSize = randomIntBetween(0, 100);
        BoundedOutputStream boundedOutputStream = new BoundedOutputStream(byteArrayOutputStream, maxSize);

        byte[] values = new byte[maxSize];
        for (int i = 0; i < maxSize; i++) {
            byte value = randomByte();
            boundedOutputStream.write(value);
            values[i] = value;
        }
        assertArrayEquals(values, byteArrayOutputStream.toByteArray());

        // Subsequent writes all fail since the size is exceeded
        for (int i = 0; i < randomIntBetween(5, 15); i++) {
            assertThrows(
                BoundedOutputStreamFailedWriteException.class,
                () -> boundedOutputStream.write(randomByte())
            );
        }
    }

    public void testWriteByteArray() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int maxSize = randomIntBetween(0, 100);
        BoundedOutputStream boundedOutputStream = new BoundedOutputStream(byteArrayOutputStream, maxSize);

        byte[] values = randomByteArrayOfLength(maxSize);
        boundedOutputStream.write(values);
        assertArrayEquals(values, byteArrayOutputStream.toByteArray());
    }

    public void testWriteByteArrayWithArrayLengthGreaterThanMaxSize() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int maxSize = randomIntBetween(0, 100);
        BoundedOutputStream boundedOutputStream = new BoundedOutputStream(byteArrayOutputStream, maxSize);

        assertThrows(
            BoundedOutputStreamFailedWriteException.class,
            () -> boundedOutputStream.write(randomByteArrayOfLength(maxSize + randomIntBetween(1, 100)))
        );
    }

    public void testWriteByteArrayWithOffset() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int maxSize = randomIntBetween(50, 100);
        BoundedOutputStream boundedOutputStream = new BoundedOutputStream(byteArrayOutputStream, maxSize);

        byte[] values = new byte[maxSize];
        for (int i = 0; i < maxSize; i++) {
            values[i] = randomByte();
        }

        int offset = randomIntBetween(0, 50);
        int length = randomIntBetween(offset, maxSize);
        boundedOutputStream.write(values, offset, length);
        byte[] expectedResult = new byte[length];
        System.arraycopy(values, offset, expectedResult, 0, length);
        assertArrayEquals(expectedResult, byteArrayOutputStream.toByteArray());
    }


    public void testWriteByteArrayWithOffsetWithLengthExceedingMaxSize() {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//        int maxSize = randomIntBetween(0, 100);
        int maxSize = 1;
        BoundedOutputStream boundedOutputStream = new BoundedOutputStream(byteArrayOutputStream, maxSize);

//        int randomOffset = randomIntBetween(0, maxSize);
//        int randomLength = randomIntBetween(maxSize, 200);
        int randomOffset = 0;
        int randomLength = 1;

        logger.warn("maxsize: " + maxSize);
        logger.warn("offset: " + randomOffset);
        logger.warn("length: " + randomLength);

        assertThrows(
            BoundedOutputStreamFailedWriteException.class,
            () -> boundedOutputStream.write(randomByteArrayOfLength(randomLength), randomOffset, randomLength)
        );
    }


    public void testNoFurtherWritesAfterLimitExceeded() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        int maxSize = randomIntBetween(0, 100);
        BoundedOutputStream boundedOutputStream = new BoundedOutputStream(byteArrayOutputStream, maxSize);

        // Write until maxSize is reached
        for (int i = 0; i < maxSize; i++) {
            boundedOutputStream.write(randomByte());
        }

        assertThrows(BoundedOutputStreamFailedWriteException.class, () -> boundedOutputStream.write(randomByte()));
        assertThrows(BoundedOutputStreamFailedWriteException.class, () -> boundedOutputStream.write(randomByteArrayOfLength(randomIntBetween(0, 100))));
        assertThrows(BoundedOutputStreamFailedWriteException.class, () -> boundedOutputStream.write(randomByteArrayOfLength(randomIntBetween(0, 100)), randomIntBetween(0, 100), randomIntBetween(0, 100)));
    }
}
