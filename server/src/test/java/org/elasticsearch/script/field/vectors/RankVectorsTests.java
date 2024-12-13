/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.mapper.vectors.RankVectorsFieldMapper;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntFunction;

public class RankVectorsTests extends ESTestCase {

    @BeforeClass
    public static void setup() {
        assumeTrue("Requires rank-vectors support", RankVectorsFieldMapper.FEATURE_FLAG.isEnabled());
    }

    public void testByteUnsupported() {
        int count = randomIntBetween(1, 16);
        int dims = randomIntBetween(1, 16);
        byte[][] docVector = new byte[count][dims];
        float[][] queryVector = new float[count][dims];
        for (int i = 0; i < docVector.length; i++) {
            random().nextBytes(docVector[i]);
            for (int j = 0; j < dims; j++) {
                queryVector[i][j] = randomFloat();
            }
        }

        RankVectors knn = newByteVector(docVector);
        UnsupportedOperationException e;

        e = expectThrows(UnsupportedOperationException.class, () -> knn.maxSimDotProduct(queryVector));
        assertEquals(e.getMessage(), "use [float maxSimDotProduct(byte[][] queryVector)] instead");
    }

    public void testFloatUnsupported() {
        int count = randomIntBetween(1, 16);
        int dims = randomIntBetween(1, 16);
        float[][] docVector = new float[count][dims];
        byte[][] queryVector = new byte[count][dims];
        for (int i = 0; i < docVector.length; i++) {
            random().nextBytes(queryVector[i]);
            for (int j = 0; j < dims; j++) {
                docVector[i][j] = randomFloat();
            }
        }

        RankVectors knn = newFloatVector(docVector);

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> knn.maxSimDotProduct(queryVector));
        assertEquals(e.getMessage(), "use [float maxSimDotProduct(float[][] queryVector)] instead");
    }

    static RankVectors newFloatVector(float[][] vector) {
        BytesRef magnitudes = magnitudes(vector.length, i -> (float) Math.sqrt(VectorUtil.dotProduct(vector[i], vector[i])));
        return new FloatRankVectors(VectorIterator.from(vector), magnitudes, vector.length, vector[0].length);
    }

    static RankVectors newByteVector(byte[][] vector) {
        BytesRef magnitudes = magnitudes(vector.length, i -> (float) Math.sqrt(VectorUtil.dotProduct(vector[i], vector[i])));
        return new ByteRankVectors(VectorIterator.from(vector), magnitudes, vector.length, vector[0].length);
    }

    static BytesRef magnitudes(int count, IntFunction<Float> magnitude) {
        ByteBuffer magnitudeBuffer = ByteBuffer.allocate(count * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            magnitudeBuffer.putFloat(magnitude.apply(i));
        }
        return new BytesRef(magnitudeBuffer.array());
    }
}
