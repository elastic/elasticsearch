/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.script;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.script.field.vectors.ByteRankVectors;
import org.elasticsearch.script.field.vectors.FloatRankVectors;
import org.elasticsearch.script.field.vectors.RankVectors;
import org.elasticsearch.script.field.vectors.VectorIterator;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntFunction;

public class RankVectorsTests extends ESTestCase {

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
