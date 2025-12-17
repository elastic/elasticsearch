/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

public class Murmur3BytesRefUtilsFuzzTests extends ESTestCase {

    public void testSimdMatchesBytesRefHashCode() {
        for (int t = 0; t < 100; t++) {
            int n = 50 + randomIntBetween(0, 200); // array length 50–249
            BytesRef[] refs = new BytesRef[n];
            for (int i = 0; i < n; i++) {
                int len = randomIntBetween(0, 50); // element length 0–49
                byte[] b = randomByteArrayOfLength(len);
                int offset = randomBoolean() ? 0 : randomIntBetween(0, len);
                refs[i] = new BytesRef(b, offset, b.length - offset);
            }
            assertMatches(refs);
        }
    }

    private void assertMatches(BytesRef[] refs) {
        int[] expected = bytesRefHashCode(refs);
        int[] actual = new int[refs.length];
        Murmur3BytesRefUtils.hashAll(refs, actual);
        assertArrayEquals("SIMD hash results must match BytesRef.hashCode()", expected, actual);
    }

    static int[] bytesRefHashCode(BytesRef[] refs) {
        // scalar reference: use BytesRef.hashCode()
        int[] result = new int[refs.length];
        for (int i = 0; i < refs.length; i++) {
            result[i] = refs[i].hashCode();
        }
        return result;
    }
}
