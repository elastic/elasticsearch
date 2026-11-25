/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class BytesRefArrayUtilsTests extends ESTestCase {

    public void testAppendPackedMatchesPerValueAppend() {
        int count = randomIntBetween(1, 64);
        byte[][] payloads = new byte[count][];
        int packedLen = 0;
        int[] exclusiveEnds = new int[count];
        for (int i = 0; i < count; i++) {
            payloads[i] = randomByteArrayOfLength(between(0, 40));
            packedLen += payloads[i].length;
            exclusiveEnds[i] = packedLen;
        }
        byte[] packed = new byte[Math.max(packedLen, 1)];
        int pos = 0;
        for (byte[] p : payloads) {
            System.arraycopy(p, 0, packed, pos, p.length);
            pos += p.length;
        }
        BigArrays bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
        try (BytesRefArray bulk = new BytesRefArray(count, bigArrays); BytesRefArray perValue = new BytesRefArray(count, bigArrays)) {
            BytesRefArrayUtils.appendPacked(bulk, packed, 0, packedLen, exclusiveEnds, count);
            for (byte[] p : payloads) {
                perValue.append(new BytesRef(p));
            }
            assertThat(bulk.size(), equalTo((long) count));
            BytesRef a = new BytesRef();
            BytesRef b = new BytesRef();
            for (int i = 0; i < count; i++) {
                assertThat(bulk.get(i, a), equalTo(perValue.get(i, b)));
            }
        }
    }
}
