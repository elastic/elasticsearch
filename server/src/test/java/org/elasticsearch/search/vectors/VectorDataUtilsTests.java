/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.test.ESTestCase;

import java.util.Base64;
import java.util.List;

public class VectorDataUtilsTests extends ESTestCase {

    public void testExtractHexEncodedVectorFromSourceList() {
        VectorData vector = VectorDataUtils.extractVectorDataFromObject(List.of("0BB8"));
        assertNotNull(vector);
        assertArrayEquals(new byte[] { 0x0B, (byte) 0xB8 }, vector.asByteVector());
    }

    public void testExtractBase64EncodedVectorFromSourceList() {
        String encoded = Base64.getEncoder().encodeToString(new byte[] { 0x0B, (byte) 0xB8 });
        VectorData vector = VectorDataUtils.extractVectorDataFromObject(List.of(encoded));
        assertNotNull(vector);
        assertArrayEquals(new byte[] { 0x0B, (byte) 0xB8 }, vector.asByteVector());
    }

    public void testExtractPrefersHexWhenStringIsValidHexAndBase64() {
        // "0BB8" is valid hex (2 bytes) and also valid base64 (3 bytes); indexing prefers hex
        VectorData vector = VectorDataUtils.extractVectorDataFromObject("0BB8");
        assertNotNull(vector);
        assertArrayEquals(new byte[] { 0x0B, (byte) 0xB8 }, vector.asByteVector());
    }

    public void testExtractReturnsNullForNonEncodedString() {
        assertNull(VectorDataUtils.extractVectorDataFromObject(List.of("not a dense vector!")));
    }

    public void testExtractFloatArrayStillWorks() {
        float[] expected = new float[] { 0.4f, 0.2f, 0.4f, 0.4f };
        VectorData vector = VectorDataUtils.extractVectorDataFromObject(List.of(expected));
        assertNotNull(vector);
        assertArrayEquals(expected, vector.asFloatVector(), 0.0f);
    }
}
