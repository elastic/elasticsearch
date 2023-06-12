/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field.vectors;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.index.mapper.vectors.BinaryDenseVectorScriptDocValuesTests;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class DenseVectorTests extends ESTestCase {

    public void testBadVectorType() {
        DenseVector knn = new KnnDenseVector(new float[] { 1.0f, 2.0f, 3.5f });
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> knn.dotProduct(new HashMap<>()));
        assertThat(e.getMessage(), containsString("Cannot use vector ["));
        assertThat(e.getMessage(), containsString("] with class [java.util.HashMap] as query vector"));

        e = expectThrows(IllegalArgumentException.class, () -> knn.l1Norm(new HashMap<>()));
        assertThat(e.getMessage(), containsString("Cannot use vector ["));
        assertThat(e.getMessage(), containsString("] with class [java.util.HashMap] as query vector"));

        e = expectThrows(IllegalArgumentException.class, () -> knn.l2Norm(new HashMap<>()));
        assertThat(e.getMessage(), containsString("Cannot use vector ["));
        assertThat(e.getMessage(), containsString("] with class [java.util.HashMap] as query vector"));

        e = expectThrows(IllegalArgumentException.class, () -> knn.cosineSimilarity(new HashMap<>()));
        assertThat(e.getMessage(), containsString("Cannot use vector ["));
        assertThat(e.getMessage(), containsString("] with class [java.util.HashMap] as query vector"));
    }

    public void testFloatVsListQueryVector() {
        int dims = randomIntBetween(1, 16);
        float[] docVector = new float[dims];
        float[] arrayQV = new float[dims];
        List<Number> listQV = new ArrayList<>(dims);
        for (int i = 0; i < docVector.length; i++) {
            docVector[i] = randomFloat();
            float q = randomFloat();
            arrayQV[i] = q;
            listQV.add(q);
        }

        KnnDenseVector knn = new KnnDenseVector(docVector);
        assertEquals(knn.dotProduct(arrayQV), knn.dotProduct(listQV), 0.001f);
        assertEquals(knn.dotProduct((Object) listQV), knn.dotProduct((Object) arrayQV), 0.001f);

        assertEquals(knn.l1Norm(arrayQV), knn.l1Norm(listQV), 0.001f);
        assertEquals(knn.l1Norm((Object) listQV), knn.l1Norm((Object) arrayQV), 0.001f);

        assertEquals(knn.l2Norm(arrayQV), knn.l2Norm(listQV), 0.001f);
        assertEquals(knn.l2Norm((Object) listQV), knn.l2Norm((Object) arrayQV), 0.001f);

        assertEquals(knn.cosineSimilarity(arrayQV), knn.cosineSimilarity(listQV), 0.001f);
        assertEquals(knn.cosineSimilarity((Object) listQV), knn.cosineSimilarity((Object) arrayQV), 0.001f);

        for (Version indexVersion : Arrays.asList(Version.V_7_4_0, Version.CURRENT)) {
            BytesRef value = BinaryDenseVectorScriptDocValuesTests.mockEncodeDenseVector(docVector, ElementType.FLOAT, indexVersion);
            BinaryDenseVector bdv = new BinaryDenseVector(docVector, value, dims, indexVersion);

            assertEquals(bdv.dotProduct(arrayQV), bdv.dotProduct(listQV), 0.001f);
            assertEquals(bdv.dotProduct((Object) listQV), bdv.dotProduct((Object) arrayQV), 0.001f);

            assertEquals(bdv.l1Norm(arrayQV), bdv.l1Norm(listQV), 0.001f);
            assertEquals(bdv.l1Norm((Object) listQV), bdv.l1Norm((Object) arrayQV), 0.001f);

            assertEquals(bdv.l2Norm(arrayQV), bdv.l2Norm(listQV), 0.001f);
            assertEquals(bdv.l2Norm((Object) listQV), bdv.l2Norm((Object) arrayQV), 0.001f);

            assertEquals(bdv.cosineSimilarity(arrayQV), bdv.cosineSimilarity(listQV), 0.001f);
            assertEquals(bdv.cosineSimilarity((Object) listQV), bdv.cosineSimilarity((Object) arrayQV), 0.001f);
        }
    }

    public void testByteVsListQueryVector() {
        int dims = randomIntBetween(1, 16);
        byte[] docVector = new byte[dims];
        float[] floatVector = new float[dims];
        byte[] arrayQV = new byte[dims];
        List<Number> listQV = new ArrayList<>(dims);
        for (int i = 0; i < docVector.length; i++) {
            byte d = randomByte();
            docVector[i] = d;
            floatVector[i] = d;
            byte q = randomByte();
            arrayQV[i] = q;
            listQV.add(q);
        }

        ByteKnnDenseVector knn = new ByteKnnDenseVector(docVector);
        assertEquals(knn.dotProduct(arrayQV), knn.dotProduct(listQV), 0.001f);
        assertEquals(knn.dotProduct((Object) listQV), knn.dotProduct((Object) arrayQV), 0.001f);

        assertEquals(knn.l1Norm(arrayQV), knn.l1Norm(listQV), 0.001f);
        assertEquals(knn.l1Norm((Object) listQV), knn.l1Norm((Object) arrayQV), 0.001f);

        assertEquals(knn.l2Norm(arrayQV), knn.l2Norm(listQV), 0.001f);
        assertEquals(knn.l2Norm((Object) listQV), knn.l2Norm((Object) arrayQV), 0.001f);

        assertEquals(knn.cosineSimilarity(arrayQV), knn.cosineSimilarity(listQV), 0.001f);
        assertEquals(knn.cosineSimilarity((Object) listQV), knn.cosineSimilarity((Object) arrayQV), 0.001f);

        BytesRef value = BinaryDenseVectorScriptDocValuesTests.mockEncodeDenseVector(floatVector, ElementType.BYTE, Version.CURRENT);
        byte[] byteVectorValue = new byte[dims];
        System.arraycopy(value.bytes, value.offset, byteVectorValue, 0, dims);
        ByteBinaryDenseVector bdv = new ByteBinaryDenseVector(byteVectorValue, value, dims);

        assertEquals(bdv.dotProduct(arrayQV), bdv.dotProduct(listQV), 0.001f);
        assertEquals(bdv.dotProduct((Object) listQV), bdv.dotProduct((Object) arrayQV), 0.001f);

        assertEquals(bdv.l1Norm(arrayQV), bdv.l1Norm(listQV), 0.001f);
        assertEquals(bdv.l1Norm((Object) listQV), bdv.l1Norm((Object) arrayQV), 0.001f);

        assertEquals(bdv.l2Norm(arrayQV), bdv.l2Norm(listQV), 0.001f);
        assertEquals(bdv.l2Norm((Object) listQV), bdv.l2Norm((Object) arrayQV), 0.001f);

        assertEquals(bdv.cosineSimilarity(arrayQV), bdv.cosineSimilarity(listQV), 0.001f);
        assertEquals(bdv.cosineSimilarity((Object) listQV), bdv.cosineSimilarity((Object) arrayQV), 0.001f);
    }

    public void testByteUnsupported() {
        int dims = randomIntBetween(1, 16);
        byte[] docVector = new byte[dims];
        float[] queryVector = new float[dims];
        for (int i = 0; i < docVector.length; i++) {
            docVector[i] = randomByte();
            queryVector[i] = randomByte();
        }

        ByteKnnDenseVector knn = new ByteKnnDenseVector(docVector);
        UnsupportedOperationException e;

        e = expectThrows(UnsupportedOperationException.class, () -> knn.dotProduct(queryVector));
        assertEquals(e.getMessage(), "use [int dotProduct(byte[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> knn.dotProduct((Object) queryVector));
        assertEquals(e.getMessage(), "use [int dotProduct(byte[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> knn.l1Norm(queryVector));
        assertEquals(e.getMessage(), "use [int l1Norm(byte[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> knn.l1Norm((Object) queryVector));
        assertEquals(e.getMessage(), "use [int l1Norm(byte[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> knn.l2Norm(queryVector));
        assertEquals(e.getMessage(), "use [double l2Norm(byte[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> knn.l2Norm((Object) queryVector));
        assertEquals(e.getMessage(), "use [double l2Norm(byte[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> knn.cosineSimilarity(queryVector));
        assertEquals(e.getMessage(), "use [double cosineSimilarity(byte[] queryVector, float qvMagnitude)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> knn.cosineSimilarity((Object) queryVector));
        assertEquals(e.getMessage(), "use [double cosineSimilarity(byte[] queryVector, float qvMagnitude)] instead");

        ByteBinaryDenseVector binary = new ByteBinaryDenseVector(docVector, new BytesRef(docVector), dims);

        e = expectThrows(UnsupportedOperationException.class, () -> binary.dotProduct(queryVector));
        assertEquals(e.getMessage(), "use [int dotProduct(byte[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> binary.dotProduct((Object) queryVector));
        assertEquals(e.getMessage(), "use [int dotProduct(byte[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> binary.l1Norm(queryVector));
        assertEquals(e.getMessage(), "use [int l1Norm(byte[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> binary.l1Norm((Object) queryVector));
        assertEquals(e.getMessage(), "use [int l1Norm(byte[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> binary.l2Norm(queryVector));
        assertEquals(e.getMessage(), "use [double l2Norm(byte[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> binary.l2Norm((Object) queryVector));
        assertEquals(e.getMessage(), "use [double l2Norm(byte[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> binary.cosineSimilarity(queryVector));
        assertEquals(e.getMessage(), "use [double cosineSimilarity(byte[] queryVector, float qvMagnitude)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> binary.cosineSimilarity((Object) queryVector));
        assertEquals(e.getMessage(), "use [double cosineSimilarity(byte[] queryVector, float qvMagnitude)] instead");
    }

    public void testFloatUnsupported() {
        int dims = randomIntBetween(1, 16);
        float[] docVector = new float[dims];
        ByteBuffer docBuffer = ByteBuffer.allocate(dims * 4);
        byte[] queryVector = new byte[dims];
        for (int i = 0; i < docVector.length; i++) {
            docVector[i] = randomFloat();
            docBuffer.putFloat(docVector[i]);
            queryVector[i] = randomByte();
        }

        KnnDenseVector knn = new KnnDenseVector(docVector);
        UnsupportedOperationException e;

        e = expectThrows(UnsupportedOperationException.class, () -> knn.dotProduct(queryVector));
        assertEquals(e.getMessage(), "use [double dotProduct(float[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> knn.dotProduct((Object) queryVector));
        assertEquals(e.getMessage(), "use [double dotProduct(float[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> knn.l1Norm(queryVector));
        assertEquals(e.getMessage(), "use [double l1Norm(float[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> knn.l1Norm((Object) queryVector));
        assertEquals(e.getMessage(), "use [double l1Norm(float[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> knn.l2Norm(queryVector));
        assertEquals(e.getMessage(), "use [double l2Norm(float[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> knn.l2Norm((Object) queryVector));
        assertEquals(e.getMessage(), "use [double l2Norm(float[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> knn.cosineSimilarity(queryVector));
        assertEquals(e.getMessage(), "use [double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> knn.cosineSimilarity((Object) queryVector));
        assertEquals(e.getMessage(), "use [double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector)] instead");

        BinaryDenseVector binary = new BinaryDenseVector(docVector, new BytesRef(docBuffer.array()), dims, Version.CURRENT);

        e = expectThrows(UnsupportedOperationException.class, () -> binary.dotProduct(queryVector));
        assertEquals(e.getMessage(), "use [double dotProduct(float[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> binary.dotProduct((Object) queryVector));
        assertEquals(e.getMessage(), "use [double dotProduct(float[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> binary.l1Norm(queryVector));
        assertEquals(e.getMessage(), "use [double l1Norm(float[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> binary.l1Norm((Object) queryVector));
        assertEquals(e.getMessage(), "use [double l1Norm(float[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> binary.l2Norm(queryVector));
        assertEquals(e.getMessage(), "use [double l2Norm(float[] queryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> binary.l2Norm((Object) queryVector));
        assertEquals(e.getMessage(), "use [double l2Norm(float[] queryVector)] instead");

        e = expectThrows(UnsupportedOperationException.class, () -> binary.cosineSimilarity(queryVector));
        assertEquals(e.getMessage(), "use [double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector)] instead");
        e = expectThrows(UnsupportedOperationException.class, () -> binary.cosineSimilarity((Object) queryVector));
        assertEquals(e.getMessage(), "use [double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector)] instead");
    }
}
