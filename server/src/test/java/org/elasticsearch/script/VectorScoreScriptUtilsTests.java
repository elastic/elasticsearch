/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.vectors.BinaryDenseVectorScriptDocValuesTests;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.KnnDenseVectorScriptDocValuesTests;
import org.elasticsearch.script.VectorScoreScriptUtils.CosineSimilarity;
import org.elasticsearch.script.VectorScoreScriptUtils.DotProduct;
import org.elasticsearch.script.VectorScoreScriptUtils.L1Norm;
import org.elasticsearch.script.VectorScoreScriptUtils.L2Norm;
import org.elasticsearch.script.field.vectors.BinaryDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.ByteBinaryDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.ByteKnnDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.DenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.KnnDenseVectorDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VectorScoreScriptUtilsTests extends ESTestCase {

    public void testFloatVectorClassBindings() throws IOException {
        String fieldName = "vector";
        int dims = 5;
        float[] docVector = new float[] { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f };
        List<Number> queryVector = Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f);
        List<Number> invalidQueryVector = Arrays.asList(0.5, 111.3);

        List<DenseVectorDocValuesField> fields = List.of(
            new BinaryDenseVectorDocValuesField(
                BinaryDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.FLOAT, IndexVersion.V_7_4_0),
                "test",
                ElementType.FLOAT,
                dims,
                IndexVersion.V_7_4_0
            ),
            new BinaryDenseVectorDocValuesField(
                BinaryDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.FLOAT, IndexVersion.CURRENT),
                "test",
                ElementType.FLOAT,
                dims,
                IndexVersion.CURRENT
            ),
            new KnnDenseVectorDocValuesField(KnnDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }), "test", dims)
        );
        for (DenseVectorDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field("vector")).thenAnswer(mock -> field);

            // Test cosine similarity explicitly, as it must perform special logic on top of the doc values
            CosineSimilarity function = new CosineSimilarity(scoreScript, queryVector, fieldName);
            float cosineSimilarityExpected = 0.790f;
            assertEquals(
                "cosineSimilarity result is not equal to the expected value!",
                cosineSimilarityExpected,
                function.cosineSimilarity(),
                0.001
            );

            // Test normalization for cosineSimilarity
            float[] queryVectorArray = new float[queryVector.size()];
            for (int i = 0; i < queryVectorArray.length; i++) {
                queryVectorArray[i] = queryVector.get(i).floatValue();
            }
            assertEquals(
                "cosineSimilarity result is not equal to the expected value!",
                cosineSimilarityExpected,
                field.getInternal().cosineSimilarity(queryVectorArray, true),
                0.001
            );

            // Check each function rejects query vectors with the wrong dimension
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new DotProduct(scoreScript, invalidQueryVector, fieldName)
            );
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );
            e = expectThrows(IllegalArgumentException.class, () -> new CosineSimilarity(scoreScript, invalidQueryVector, fieldName));
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );
            e = expectThrows(IllegalArgumentException.class, () -> new L1Norm(scoreScript, invalidQueryVector, fieldName));
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );
            e = expectThrows(IllegalArgumentException.class, () -> new L2Norm(scoreScript, invalidQueryVector, fieldName));
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );

            // Check scripting infrastructure integration
            DotProduct dotProduct = new DotProduct(scoreScript, queryVector, fieldName);
            assertEquals(65425.6249, dotProduct.dotProduct(), 0.001);
            assertEquals(485.1837, new L1Norm(scoreScript, queryVector, fieldName).l1norm(), 0.001);
            assertEquals(301.3614, new L2Norm(scoreScript, queryVector, fieldName).l2norm(), 0.001);
            when(scoreScript._getDocId()).thenReturn(1);
            e = expectThrows(IllegalArgumentException.class, dotProduct::dotProduct);
            assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
        }
    }

    public void testByteVectorClassBindings() throws IOException {
        String fieldName = "vector";
        int dims = 5;
        float[] docVector = new float[] { 1, 127, -128, 5, -10 };
        List<Number> queryVector = Arrays.asList((byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4);
        List<Number> invalidQueryVector = Arrays.asList((byte) 1, (byte) 1);

        List<DenseVectorDocValuesField> fields = List.of(
            new ByteBinaryDenseVectorDocValuesField(
                BinaryDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.BYTE, IndexVersion.CURRENT),
                "test",
                ElementType.BYTE,
                dims
            ),
            new ByteKnnDenseVectorDocValuesField(KnnDenseVectorScriptDocValuesTests.wrapBytes(new float[][] { docVector }), "test", dims)
        );
        for (DenseVectorDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field(fieldName)).thenAnswer(mock -> field);

            // Test cosine similarity explicitly, as it must perform special logic on top of the doc values
            CosineSimilarity function = new CosineSimilarity(scoreScript, queryVector, fieldName);
            float cosineSimilarityExpected = 0.765f;
            assertEquals(
                "cosineSimilarity result is not equal to the expected value!",
                cosineSimilarityExpected,
                function.cosineSimilarity(),
                0.001
            );

            // Test normalization for cosineSimilarity
            float[] queryVectorArray = new float[queryVector.size()];
            for (int i = 0; i < queryVectorArray.length; i++) {
                queryVectorArray[i] = queryVector.get(i).floatValue();
            }
            UnsupportedOperationException uoe = expectThrows(
                UnsupportedOperationException.class,
                () -> field.getInternal().cosineSimilarity(queryVectorArray, true)
            );
            assertThat(uoe.getMessage(), containsString("use [double cosineSimilarity(byte[] queryVector, float qvMagnitude)] instead"));

            // Check each function rejects query vectors with the wrong dimension
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new DotProduct(scoreScript, invalidQueryVector, fieldName)
            );
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );
            e = expectThrows(IllegalArgumentException.class, () -> new CosineSimilarity(scoreScript, invalidQueryVector, fieldName));
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );
            e = expectThrows(IllegalArgumentException.class, () -> new L1Norm(scoreScript, invalidQueryVector, fieldName));
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );
            e = expectThrows(IllegalArgumentException.class, () -> new L2Norm(scoreScript, invalidQueryVector, fieldName));
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );

            // Check scripting infrastructure integration
            DotProduct dotProduct = new DotProduct(scoreScript, queryVector, fieldName);
            assertEquals(17382.0, dotProduct.dotProduct(), 0.001);
            assertEquals(135.0, new L1Norm(scoreScript, queryVector, fieldName).l1norm(), 0.001);
            assertEquals(116.897, new L2Norm(scoreScript, queryVector, fieldName).l2norm(), 0.001);
            when(scoreScript._getDocId()).thenReturn(1);
            e = expectThrows(IllegalArgumentException.class, dotProduct::dotProduct);
            assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
        }
    }

    public void testByteVsFloatSimilarity() throws IOException {
        int dims = 5;
        float[] docVector = new float[] { 1f, 127f, -128f, 5f, -10f };
        List<Number> listFloatVector = Arrays.asList(1f, 125f, -12f, 2f, 4f);
        List<Number> listByteVector = Arrays.asList((byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4);
        float[] floatVector = new float[] { 1f, 125f, -12f, 2f, 4f };
        byte[] byteVector = new byte[] { (byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4 };

        List<DenseVectorDocValuesField> fields = List.of(
            new BinaryDenseVectorDocValuesField(
                BinaryDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.FLOAT, IndexVersion.V_7_4_0),
                "field0",
                ElementType.FLOAT,
                dims,
                IndexVersion.V_7_4_0
            ),
            new BinaryDenseVectorDocValuesField(
                BinaryDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.FLOAT, IndexVersion.CURRENT),
                "field1",
                ElementType.FLOAT,
                dims,
                IndexVersion.CURRENT
            ),
            new KnnDenseVectorDocValuesField(KnnDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }), "field2", dims),
            new ByteBinaryDenseVectorDocValuesField(
                BinaryDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.BYTE, IndexVersion.CURRENT),
                "field3",
                ElementType.BYTE,
                dims
            ),
            new ByteKnnDenseVectorDocValuesField(KnnDenseVectorScriptDocValuesTests.wrapBytes(new float[][] { docVector }), "field4", dims)
        );
        for (DenseVectorDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field("vector")).thenAnswer(mock -> field);

            int dotProductExpected = 17382;
            DotProduct dotProduct = new DotProduct(scoreScript, listFloatVector, "vector");
            assertEquals(field.getName(), dotProductExpected, dotProduct.dotProduct(), 0.001);
            dotProduct = new DotProduct(scoreScript, listByteVector, "vector");
            assertEquals(field.getName(), dotProductExpected, dotProduct.dotProduct(), 0.001);
            assertEquals(field.getName(), dotProductExpected, field.get().dotProduct(listFloatVector), 0.001);
            assertEquals(field.getName(), dotProductExpected, field.get().dotProduct(listByteVector), 0.001);
            switch (field.getElementType()) {
                case BYTE -> {
                    assertEquals(field.getName(), dotProductExpected, field.get().dotProduct(byteVector));
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().dotProduct(floatVector)
                    );
                    assertThat(e.getMessage(), containsString("use [int dotProduct(byte[] queryVector)] instead"));
                }
                case FLOAT -> {
                    assertEquals(field.getName(), dotProductExpected, field.get().dotProduct(floatVector), 0.001);
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().dotProduct(byteVector)
                    );
                    assertThat(e.getMessage(), containsString("use [double dotProduct(float[] queryVector)] instead"));
                }
            }
            ;

            int l1NormExpected = 135;
            L1Norm l1Norm = new L1Norm(scoreScript, listFloatVector, "vector");
            assertEquals(field.getName(), l1NormExpected, l1Norm.l1norm(), 0.001);
            l1Norm = new L1Norm(scoreScript, listByteVector, "vector");
            assertEquals(field.getName(), l1NormExpected, l1Norm.l1norm(), 0.001);
            assertEquals(field.getName(), l1NormExpected, field.get().l1Norm(listFloatVector), 0.001);
            assertEquals(field.getName(), l1NormExpected, field.get().l1Norm(listByteVector), 0.001);
            switch (field.getElementType()) {
                case BYTE -> {
                    assertEquals(field.getName(), l1NormExpected, field.get().l1Norm(byteVector));
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().l1Norm(floatVector)
                    );
                    assertThat(e.getMessage(), containsString("use [int l1Norm(byte[] queryVector)] instead"));
                }
                case FLOAT -> {
                    assertEquals(field.getName(), l1NormExpected, field.get().l1Norm(floatVector), 0.001);
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().l1Norm(byteVector)
                    );
                    assertThat(e.getMessage(), containsString("use [double l1Norm(float[] queryVector)] instead"));
                }
            }
            ;

            float l2NormExpected = 116.897f;
            L2Norm l2Norm = new L2Norm(scoreScript, listFloatVector, "vector");
            assertEquals(field.getName(), l2NormExpected, l2Norm.l2norm(), 0.001);
            l2Norm = new L2Norm(scoreScript, listByteVector, "vector");
            assertEquals(field.getName(), l2NormExpected, l2Norm.l2norm(), 0.001);
            assertEquals(field.getName(), l2NormExpected, field.get().l2Norm(listFloatVector), 0.001);
            assertEquals(field.getName(), l2NormExpected, field.get().l2Norm(listByteVector), 0.001);
            switch (field.getElementType()) {
                case BYTE -> {
                    assertEquals(field.getName(), l2NormExpected, field.get().l2Norm(byteVector), 0.001);
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().l2Norm(floatVector)
                    );
                    assertThat(e.getMessage(), containsString("use [double l2Norm(byte[] queryVector)] instead"));
                }
                case FLOAT -> {
                    assertEquals(field.getName(), l2NormExpected, field.get().l2Norm(floatVector), 0.001);
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().l2Norm(byteVector)
                    );
                    assertThat(e.getMessage(), containsString("use [double l2Norm(float[] queryVector)] instead"));
                }
            }
            ;

            float cosineSimilarityExpected = 0.765f;
            CosineSimilarity cosineSimilarity = new CosineSimilarity(scoreScript, listFloatVector, "vector");
            assertEquals(field.getName(), cosineSimilarityExpected, cosineSimilarity.cosineSimilarity(), 0.001);
            cosineSimilarity = new CosineSimilarity(scoreScript, listByteVector, "vector");
            assertEquals(field.getName(), cosineSimilarityExpected, cosineSimilarity.cosineSimilarity(), 0.001);
            assertEquals(field.getName(), cosineSimilarityExpected, field.get().cosineSimilarity(listFloatVector), 0.001);
            assertEquals(field.getName(), cosineSimilarityExpected, field.get().cosineSimilarity(listByteVector), 0.001);
            switch (field.getElementType()) {
                case BYTE -> {
                    assertEquals(field.getName(), cosineSimilarityExpected, field.get().cosineSimilarity(byteVector), 0.001);
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().cosineSimilarity(floatVector)
                    );
                    assertThat(
                        e.getMessage(),
                        containsString("use [double cosineSimilarity(byte[] queryVector, float qvMagnitude)] instead")
                    );
                }
                case FLOAT -> {
                    assertEquals(field.getName(), cosineSimilarityExpected, field.get().cosineSimilarity(floatVector), 0.001);
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().cosineSimilarity(byteVector)
                    );
                    assertThat(
                        e.getMessage(),
                        containsString("use [double cosineSimilarity(float[] queryVector, boolean normalizeQueryVector)] instead")
                    );
                }
            }
        }
    }

    public void testByteBoundaries() throws IOException {
        String fieldName = "vector";
        int dims = 1;
        float[] docVector = new float[] { 0 };
        List<Number> greaterThanVector = List.of(128);
        List<Number> lessThanVector = List.of(-129);
        List<Number> decimalVector = List.of(0.5);

        List<DenseVectorDocValuesField> fields = List.of(
            new ByteBinaryDenseVectorDocValuesField(
                BinaryDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.BYTE, IndexVersion.CURRENT),
                "test",
                ElementType.BYTE,
                dims
            ),
            new ByteKnnDenseVectorDocValuesField(KnnDenseVectorScriptDocValuesTests.wrapBytes(new float[][] { docVector }), "test", dims)
        );

        for (DenseVectorDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field(fieldName)).thenAnswer(mock -> field);

            IllegalArgumentException e;

            e = expectThrows(IllegalArgumentException.class, () -> new DotProduct(scoreScript, greaterThanVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support integers between [-128, 127] but found [128.0] at dim [0]; "
                    + "Preview of invalid vector: [128.0]"
            );
            e = expectThrows(IllegalArgumentException.class, () -> new L1Norm(scoreScript, greaterThanVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support integers between [-128, 127] but found [128.0] at dim [0]; "
                    + "Preview of invalid vector: [128.0]"
            );
            e = expectThrows(IllegalArgumentException.class, () -> new L2Norm(scoreScript, greaterThanVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support integers between [-128, 127] but found [128.0] at dim [0]; "
                    + "Preview of invalid vector: [128.0]"
            );
            e = expectThrows(IllegalArgumentException.class, () -> new CosineSimilarity(scoreScript, greaterThanVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support integers between [-128, 127] but found [128.0] at dim [0]; "
                    + "Preview of invalid vector: [128.0]"
            );

            e = expectThrows(IllegalArgumentException.class, () -> new DotProduct(scoreScript, lessThanVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support integers between [-128, 127] but found [-129.0] at dim [0]; "
                    + "Preview of invalid vector: [-129.0]"
            );
            e = expectThrows(IllegalArgumentException.class, () -> new L1Norm(scoreScript, lessThanVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support integers between [-128, 127] but found [-129.0] at dim [0]; "
                    + "Preview of invalid vector: [-129.0]"
            );
            e = expectThrows(IllegalArgumentException.class, () -> new L2Norm(scoreScript, lessThanVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support integers between [-128, 127] but found [-129.0] at dim [0]; "
                    + "Preview of invalid vector: [-129.0]"
            );
            e = expectThrows(IllegalArgumentException.class, () -> new CosineSimilarity(scoreScript, lessThanVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support integers between [-128, 127] but found [-129.0] at dim [0]; "
                    + "Preview of invalid vector: [-129.0]"
            );

            e = expectThrows(IllegalArgumentException.class, () -> new DotProduct(scoreScript, decimalVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support non-decimal values but found decimal value [0.5] at dim [0]; "
                    + "Preview of invalid vector: [0.5]"
            );
            e = expectThrows(IllegalArgumentException.class, () -> new L1Norm(scoreScript, decimalVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support non-decimal values but found decimal value [0.5] at dim [0]; "
                    + "Preview of invalid vector: [0.5]"
            );
            e = expectThrows(IllegalArgumentException.class, () -> new L2Norm(scoreScript, decimalVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support non-decimal values but found decimal value [0.5] at dim [0]; "
                    + "Preview of invalid vector: [0.5]"
            );
            e = expectThrows(IllegalArgumentException.class, () -> new CosineSimilarity(scoreScript, decimalVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support non-decimal values but found decimal value [0.5] at dim [0]; "
                    + "Preview of invalid vector: [0.5]"
            );
        }
    }
}
