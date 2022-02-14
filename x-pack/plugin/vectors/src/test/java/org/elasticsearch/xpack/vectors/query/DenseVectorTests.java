/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

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

        QueryVector qv = new QueryVector(listQV);

        KnnDenseVector knn = new KnnDenseVector(docVector);
        assertEquals(knn.dotProduct(qv), knn.dotProduct(arrayQV), 0.001f);
        assertEquals(knn.dotProduct(arrayQV), knn.dotProduct(listQV), 0.001f);
        assertEquals(knn.dotProduct((Object) qv), knn.dotProduct((Object) arrayQV), 0.001f);
        assertEquals(knn.dotProduct((Object) qv), knn.dotProduct(listQV), 0.001f);

        assertEquals(knn.l1Norm(qv), knn.l1Norm(arrayQV), 0.001f);
        assertEquals(knn.l1Norm(arrayQV), knn.l1Norm(listQV), 0.001f);
        assertEquals(knn.l1Norm((Object) qv), knn.l1Norm((Object) arrayQV), 0.001f);
        assertEquals(knn.l1Norm((Object) qv), knn.l1Norm(listQV), 0.001f);

        assertEquals(knn.l2Norm(qv), knn.l2Norm(arrayQV), 0.001f);
        assertEquals(knn.l2Norm(arrayQV), knn.l2Norm(listQV), 0.001f);
        assertEquals(knn.l2Norm((Object) qv), knn.l2Norm((Object) arrayQV), 0.001f);
        assertEquals(knn.l2Norm((Object) qv), knn.l2Norm(listQV), 0.001f);

        assertEquals(knn.cosineSimilarity(qv), knn.cosineSimilarity(arrayQV), 0.001f);
        assertEquals(knn.cosineSimilarity(arrayQV), knn.cosineSimilarity(listQV), 0.001f);
        assertEquals(knn.cosineSimilarity((Object) qv), knn.cosineSimilarity((Object) arrayQV), 0.001f);
        assertEquals(knn.cosineSimilarity((Object) qv), knn.cosineSimilarity((Object) listQV), 0.001f);

        for (Version indexVersion : Arrays.asList(Version.V_7_4_0, Version.CURRENT)) {
            BytesRef value = BinaryDenseVectorScriptDocValuesTests.mockEncodeDenseVector(docVector, indexVersion);
            BinaryDenseVector bdv = new BinaryDenseVector(value, dims, indexVersion);
            assertEquals(bdv.dotProduct(qv), bdv.dotProduct(arrayQV), 0.001f);
            assertEquals(bdv.dotProduct(arrayQV), bdv.dotProduct(listQV), 0.001f);
            assertEquals(bdv.dotProduct((Object) qv), bdv.dotProduct((Object) arrayQV), 0.001f);
            assertEquals(bdv.dotProduct((Object) qv), bdv.dotProduct(listQV), 0.001f);

            assertEquals(bdv.l1Norm(qv), bdv.l1Norm(arrayQV), 0.001f);
            assertEquals(bdv.l1Norm(arrayQV), bdv.l1Norm(listQV), 0.001f);
            assertEquals(bdv.l1Norm((Object) qv), bdv.l1Norm((Object) arrayQV), 0.001f);
            assertEquals(bdv.l1Norm((Object) qv), bdv.l1Norm(listQV), 0.001f);

            assertEquals(bdv.l2Norm(qv), bdv.l2Norm(arrayQV), 0.001f);
            assertEquals(bdv.l2Norm(arrayQV), bdv.l2Norm(listQV), 0.001f);
            assertEquals(bdv.l2Norm((Object) qv), bdv.l2Norm((Object) arrayQV), 0.001f);
            assertEquals(bdv.l2Norm((Object) qv), bdv.l2Norm(listQV), 0.001f);

            assertEquals(bdv.cosineSimilarity(qv), bdv.cosineSimilarity(arrayQV), 0.001f);
            assertEquals(bdv.cosineSimilarity(arrayQV), bdv.cosineSimilarity(listQV), 0.001f);
            assertEquals(bdv.cosineSimilarity((Object) qv), bdv.cosineSimilarity((Object) arrayQV), 0.001f);
            assertEquals(bdv.cosineSimilarity((Object) qv), bdv.cosineSimilarity(listQV), 0.001f);
        }
    }

}
