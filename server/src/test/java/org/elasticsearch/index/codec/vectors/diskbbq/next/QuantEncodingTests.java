/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.test.ESTestCase;

public class QuantEncodingTests extends ESTestCase {

    public void testSingleBitNibbles() {
        QuantEncoding encoding = QuantEncoding.ONE_BIT_4BIT_QUERY;
        int discretized = encoding.discretizedDimensions(randomIntBetween(1, 1024));
        // should discretize to something that can be packed into bytes from bits and nibbles
        assertEquals(0, discretized % 2);
        assertEquals(0, discretized % 8);
    }

    public void testSingleBitNibblesPackSize() {
        QuantEncoding encoding = QuantEncoding.ONE_BIT_4BIT_QUERY;
        assertEquals(1, encoding.getDocPackedLength(1));
        assertEquals(4, encoding.getQueryPackedLength(1));
        assertEquals(1, encoding.getDocPackedLength(3));
        assertEquals(4, encoding.getQueryPackedLength(3));
        assertEquals(1, encoding.getDocPackedLength(8));
        assertEquals(4, encoding.getQueryPackedLength(8));
        assertEquals(2, encoding.getDocPackedLength(15));
        assertEquals(2, encoding.getDocPackedLength(16));
        assertEquals(8, encoding.getQueryPackedLength(15));
        assertEquals(8, encoding.getQueryPackedLength(16));
    }

    public void testDibitAndNibbles() {
        QuantEncoding encoding = QuantEncoding.TWO_BIT_4BIT_QUERY;
        int discretized = encoding.discretizedDimensions(randomIntBetween(1, 1024));
        // should discretize to something that can be packed into bytes from two bits and nibbles
        assertEquals(0, discretized % 2);
        assertEquals(0, discretized % 4);
    }

    public void testDibitAndNibblesPackSize() {
        QuantEncoding encoding = QuantEncoding.TWO_BIT_4BIT_QUERY;
        assertEquals(1, encoding.getDocPackedLength(1));
        assertEquals(4, encoding.getQueryPackedLength(1));
        assertEquals(1, encoding.getDocPackedLength(3));
        assertEquals(4, encoding.getQueryPackedLength(3));
        assertEquals(2, encoding.getDocPackedLength(8));
        assertEquals(8, encoding.getQueryPackedLength(8));
        assertEquals(4, encoding.getDocPackedLength(15));
        assertEquals(4, encoding.getDocPackedLength(16));
        assertEquals(16, encoding.getQueryPackedLength(15));
        assertEquals(16, encoding.getQueryPackedLength(16));
    }

    public void testHalfByteAndNibbles() {
        QuantEncoding encoding = QuantEncoding.FOUR_BIT_SYMMETRIC;
        int discretized = encoding.discretizedDimensions(randomIntBetween(1, 1024));
        // should discretize to something that can be packed into bytes from four bits and nibbles
        assertEquals(0, discretized % 2);
    }

    public void testHalfByteAndNibblesPackSize() {
        QuantEncoding encoding = QuantEncoding.FOUR_BIT_SYMMETRIC;
        assertEquals(1, encoding.getDocPackedLength(1));
        assertEquals(2, encoding.getQueryPackedLength(1));
        assertEquals(2, encoding.getDocPackedLength(3));
        assertEquals(4, encoding.getQueryPackedLength(3));
        assertEquals(4, encoding.getDocPackedLength(8));
        assertEquals(8, encoding.getQueryPackedLength(8));
        assertEquals(8, encoding.getDocPackedLength(16));
        assertEquals(8, encoding.getDocPackedLength(16));
        assertEquals(16, encoding.getQueryPackedLength(16));
        assertEquals(16, encoding.getQueryPackedLength(16));
    }

    public void testSevenBitPackSize() {
        QuantEncoding encoding = QuantEncoding.SEVEN_BIT_SYMMETRIC;
        assertEquals(1, encoding.getDocPackedLength(1));
        assertEquals(1, encoding.getQueryPackedLength(1));
        assertEquals(3, encoding.getDocPackedLength(3));
        assertEquals(3, encoding.getQueryPackedLength(3));
        assertEquals(8, encoding.getDocPackedLength(8));
        assertEquals(8, encoding.getQueryPackedLength(8));
    }

    public void testOneBitOneBitQueryPackSize() {
        QuantEncoding encoding = QuantEncoding.ONE_BIT_1BIT_QUERY;
        assertEquals(1, encoding.getDocPackedLength(1));
        assertEquals(1, encoding.getQueryPackedLength(1));
        assertEquals(1, encoding.getDocPackedLength(8));
        assertEquals(1, encoding.getQueryPackedLength(8));
        assertEquals(2, encoding.getDocPackedLength(15));
        assertEquals(2, encoding.getQueryPackedLength(15));
        assertEquals(2, encoding.getDocPackedLength(16));
        assertEquals(2, encoding.getQueryPackedLength(16));
    }

    public void testFromDocAndQueryBits() {
        assertEquals(QuantEncoding.ONE_BIT_1BIT_QUERY, QuantEncoding.fromDocAndQueryBits((byte) 1, (byte) 1));
        assertEquals(QuantEncoding.ONE_BIT_4BIT_QUERY, QuantEncoding.fromDocAndQueryBits((byte) 1, (byte) 4));
        assertEquals(QuantEncoding.TWO_BIT_4BIT_QUERY, QuantEncoding.fromDocAndQueryBits((byte) 2, (byte) 4));
        assertEquals(QuantEncoding.FOUR_BIT_SYMMETRIC, QuantEncoding.fromDocAndQueryBits((byte) 4, (byte) 4));
        assertEquals(QuantEncoding.SEVEN_BIT_SYMMETRIC, QuantEncoding.fromDocAndQueryBits((byte) 7, (byte) 7));
        expectThrows(IllegalArgumentException.class, () -> QuantEncoding.fromDocAndQueryBits((byte) 1, (byte) 7));
        expectThrows(IllegalArgumentException.class, () -> QuantEncoding.fromDocAndQueryBits((byte) 4, (byte) 1));
    }

}
