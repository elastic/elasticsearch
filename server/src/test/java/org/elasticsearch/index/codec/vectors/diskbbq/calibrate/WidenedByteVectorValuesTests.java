/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class WidenedByteVectorValuesTests extends ESTestCase {

    private static List<byte[]> randomBytes(int count, int dim) {
        List<byte[]> vectors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] v = new byte[dim];
            random().nextBytes(v);
            vectors.add(v);
        }
        return vectors;
    }

    public void testWidensExactly() throws IOException {
        int dim = randomIntBetween(1, 64);
        List<byte[]> bytes = randomBytes(randomIntBetween(1, 50), dim);
        FloatVectorValues widened = new WidenedByteVectorValues(ByteVectorValues.fromBytes(bytes, dim));
        assertThat(widened.size(), equalTo(bytes.size()));
        assertThat(widened.dimension(), equalTo(dim));
        for (int ord = 0; ord < bytes.size(); ord++) {
            float[] floats = widened.vectorValue(ord);
            assertThat(floats.length, equalTo(dim));
            for (int i = 0; i < dim; i++) {
                // includes negative bytes: widening must sign-extend, not reinterpret as unsigned
                assertThat(floats[i], equalTo((float) bytes.get(ord)[i]));
            }
        }
    }

    public void testCopyReadsIndependently() throws IOException {
        int dim = randomIntBetween(1, 64);
        List<byte[]> bytes = randomBytes(2, dim);
        FloatVectorValues widened = new WidenedByteVectorValues(ByteVectorValues.fromBytes(bytes, dim));
        FloatVectorValues copy = widened.copy();
        float[] live = widened.vectorValue(0);
        float[] expected = live.clone();
        copy.vectorValue(1);
        assertArrayEquals(expected, live, 0f);
    }

    public void testIteratorVisitsEveryVector() throws IOException {
        int count = randomIntBetween(1, 50);
        FloatVectorValues widened = new WidenedByteVectorValues(ByteVectorValues.fromBytes(randomBytes(count, 4), 4));
        FloatVectorValues.DocIndexIterator iterator = widened.iterator();
        int visited = 0;
        for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
            assertThat(iterator.index(), equalTo(visited++));
        }
        assertThat(visited, equalTo(count));
    }
}
