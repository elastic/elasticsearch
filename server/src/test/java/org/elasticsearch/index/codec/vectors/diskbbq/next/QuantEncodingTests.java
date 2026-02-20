/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.elasticsearch.test.ESTestCase;

public class QuantEncodingTests extends ESTestCase {

    public void testSingleBitNibbles() {
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding = ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY;
        int discretized = encoding.discretizedDimensions(randomIntBetween(1, 1024));
        // should discretize to something that can be packed into bytes from bits and nibbles
        assertEquals(0, discretized % 2);
        assertEquals(0, discretized % 8);
    }

    public void testSingleBitNibblesPackSize() {
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding = ESNextDiskBBQVectorsFormat.QuantEncoding.ONE_BIT_4BIT_QUERY;
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
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding = ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY;
        int discretized = encoding.discretizedDimensions(randomIntBetween(1, 1024));
        // should discretize to something that can be packed into bytes from two bits and nibbles
        assertEquals(0, discretized % 2);
        assertEquals(0, discretized % 4);
    }

    public void testDibitAndNibblesPackSize() {
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding = ESNextDiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY;
        assertEquals(2, encoding.getDocPackedLength(3));
        assertEquals(4, encoding.getQueryPackedLength(3));
        assertEquals(2, encoding.getDocPackedLength(8));
        assertEquals(4, encoding.getQueryPackedLength(8));
        assertEquals(4, encoding.getDocPackedLength(15));
        assertEquals(4, encoding.getDocPackedLength(16));
        assertEquals(8, encoding.getQueryPackedLength(15));
        assertEquals(8, encoding.getQueryPackedLength(16));
    }

    public void testHalfByteAndNibbles() {
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding = ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC;
        int discretized = encoding.discretizedDimensions(randomIntBetween(1, 1024));
        // should discretize to something that can be packed into bytes from four bits and nibbles
        assertEquals(0, discretized % 2);
    }

    public void testHalfByteAndNibblesPackSize() {
        ESNextDiskBBQVectorsFormat.QuantEncoding encoding = ESNextDiskBBQVectorsFormat.QuantEncoding.FOUR_BIT_SYMMETRIC;
        assertEquals(4, encoding.getDocPackedLength(3));
        assertEquals(4, encoding.getQueryPackedLength(3));
        assertEquals(4, encoding.getDocPackedLength(8));
        assertEquals(4, encoding.getQueryPackedLength(8));
        assertEquals(8, encoding.getDocPackedLength(16));
        assertEquals(8, encoding.getDocPackedLength(16));
        assertEquals(8, encoding.getQueryPackedLength(16));
        assertEquals(8, encoding.getQueryPackedLength(16));
    }
}
