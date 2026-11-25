/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class Utf8ValueTests extends ESTestCase {

    public void testMaterializedValueCopiesItsSlice() {
        byte[] source = randomByteArrayOfLength(between(0, 200));
        int offset = between(0, source.length);
        int length = between(0, source.length - offset);
        Utf8Value value = Utf8Value.of(source, offset, length);
        assertEquals(length, value.maxLength());

        byte[] dst = new byte[between(0, 50) + value.maxLength() + Utf8Value.PADDING];
        int dstOffset = between(0, dst.length - value.maxLength() - Utf8Value.PADDING);
        assertEquals(length, value.copyTo(dst, dstOffset));
        assertArrayEquals(Arrays.copyOfRange(source, offset, offset + length), Arrays.copyOfRange(dst, dstOffset, dstOffset + length));
    }
}
