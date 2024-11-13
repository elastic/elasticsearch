/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.index.mapper.vectors.MultiDenseVectorScriptDocValuesTests;
import org.elasticsearch.script.MultiVectorScoreScriptUtils.MaxSimDotProduct;
import org.elasticsearch.script.MultiVectorScoreScriptUtils.MaxSimInvHamming;
import org.elasticsearch.script.field.vectors.BitMultiDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.ByteMultiDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.FloatMultiDenseVectorDocValuesField;
import org.elasticsearch.script.field.vectors.MultiDenseVectorDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MultiVectorScoreScriptUtilsTests extends ESTestCase {

    public void testFloatMultiVectorClassBindings() throws IOException {
        String fieldName = "vector";
        int dims = 5;
        float[][] docVectors = new float[][] {
            { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f },
            { 100.0f, 200.0f, -50.0f, 10.0f, -150.0f } };
        List<Number> queryVector = Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f);
        List<Number> invalidQueryVector = Arrays.asList(0.5, 111.3);

        List<MultiDenseVectorDocValuesField> fields = List.of(
            new FloatMultiDenseVectorDocValuesField(
                MultiDenseVectorScriptDocValuesTests.wrap(docVectors, ElementType.FLOAT, IndexVersions.MINIMUM_COMPATIBLE),
                "test",
                ElementType.FLOAT,
                dims,
                IndexVersions.MINIMUM_COMPATIBLE
            ),
            new FloatMultiDenseVectorDocValuesField(
                MultiDenseVectorScriptDocValuesTests.wrap(docVectors, ElementType.FLOAT, IndexVersion.current()),
                "test",
                ElementType.FLOAT,
                dims,
                IndexVersion.current()
            )
        );
        for (MultiDenseVectorDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field("vector")).thenAnswer(mock -> field);

            // Test max similarity dot product
            MaxSimDotProduct maxSimDotProduct = new MaxSimDotProduct(scoreScript, queryVector, fieldName);
            float maxSimDotProductExpected = 65425.6249f; // Adjust this value based on expected max similarity
            assertEquals(
                "maxSimDotProduct result is not equal to the expected value!",
                maxSimDotProductExpected,
                maxSimDotProduct.maxSimDotProduct(),
                0.001
            );

            // Test max similarity inverse hamming
            MaxSimInvHamming maxSimInvHamming = new MaxSimInvHamming(scoreScript, queryVector, fieldName);
            float maxSimInvHammingExpected = 13.0f; // Adjust this value based on expected max similarity
            assertEquals(
                "maxSimInvHamming result is not equal to the expected value!",
                maxSimInvHammingExpected,
                maxSimInvHamming.maxSimInvHamming(),
                0.001
            );

            // Check each function rejects query vectors with the wrong dimension
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new MultiVectorScoreScriptUtils.MaxSimDotProduct(scoreScript, invalidQueryVector, fieldName)
            );
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );
            e = expectThrows(IllegalArgumentException.class, () -> new MaxSimInvHamming(scoreScript, invalidQueryVector, fieldName));
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );

            // Check scripting infrastructure integration
            assertEquals(65425.6249, new MaxSimDotProduct(scoreScript, queryVector, fieldName).maxSimDotProduct(), 0.001);
            assertEquals(13.0, new MaxSimInvHamming(scoreScript, queryVector, fieldName).maxSimInvHamming(), 0.001);
            when(scoreScript._getDocId()).thenReturn(1);
            e = expectThrows(
                IllegalArgumentException.class,
                () -> new MaxSimDotProduct(scoreScript, queryVector, fieldName).maxSimDotProduct()
            );
            assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
        }
    }

    public void testFloatMultiVectorClassBindings() throws IOException {
        String fieldName = "vector";
        int dims = 5;
        float[] docVector = new float[] { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f };
        List<Number> queryVector = Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f);
        List<Number> invalidQueryVector = Arrays.asList(0.5, 111.3);

        List<MultiDenseVectorDocValuesField> fields = List.of(
            new FloatMultiDenseVectorDocValuesField(
                MultiDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.FLOAT, IndexVersions.MINIMUM_COMPATIBLE),
                "test",
                ElementType.FLOAT,
                dims,
                IndexVersions.MINIMUM_COMPATIBLE
            ),
            new FloatMultiDenseVectorDocValuesField(
                MultiDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.FLOAT, IndexVersion.current()),
                "test",
                ElementType.FLOAT,
                dims,
                IndexVersion.current()
            )
        );
        for (MultiDenseVectorDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field("vector")).thenAnswer(mock -> field);

            // Check each function rejects query vectors with the wrong dimension
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new MaxSimDotProduct(scoreScript, invalidQueryVector, fieldName)
            );
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );
            e = expectThrows(IllegalArgumentException.class, () -> new MaxSimInvHamming(scoreScript, queryVector, fieldName));
            assertThat(e.getMessage(), containsString("hamming distance is only supported for byte or bit vectors"));

            e = expectThrows(IllegalArgumentException.class, () -> new MaxSimInvHamming(scoreScript, invalidQueryVector, fieldName));
            assertThat(e.getMessage(), containsString("hamming distance is only supported for byte or bit vectors"));

            // Check scripting infrastructure integration
            MaxSimDotProduct maxSimDotProduct = new MaxSimDotProduct(scoreScript, queryVector, fieldName);
            assertEquals(65425.6249, maxSimDotProduct.maxSimDotProduct(), 0.001);
            when(scoreScript._getDocId()).thenReturn(1);
            e = expectThrows(IllegalArgumentException.class, maxSimDotProduct::maxSimDotProduct);
            assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
        }
    }

    public void testByteMultiVectorClassBindings() throws IOException {
        String fieldName = "vector";
        int dims = 5;
        float[] docVector = new float[] { 1, 127, -128, 5, -10 };
        List<Number> queryVector = Arrays.asList((byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4);
        List<Number> invalidQueryVector = Arrays.asList((byte) 1, (byte) 1);
        String hexidecimalString = HexFormat.of().formatHex(new byte[] { 1, 125, -12, 2, 4 });

        List<MultiDenseVectorDocValuesField> fields = List.of(
            new ByteMultiDenseVectorDocValuesField(
                MultiDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.BYTE, IndexVersion.current()),
                "test",
                ElementType.BYTE,
                dims
            )
        );
        for (MultiDenseVectorDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field(fieldName)).thenAnswer(mock -> field);

            // Check each function rejects query vectors with the wrong dimension
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new MaxSimDotProduct(scoreScript, invalidQueryVector, fieldName)
            );
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );
            e = expectThrows(IllegalArgumentException.class, () -> new MaxSimInvHamming(scoreScript, invalidQueryVector, fieldName));
            assertThat(
                e.getMessage(),
                containsString("query vector has a different number of dimensions [2] than the document vectors [5]")
            );

            // Check scripting infrastructure integration
            assertEquals(17382.0, new MaxSimDotProduct(scoreScript, queryVector, fieldName).maxSimDotProduct(), 0.001);
            assertEquals(17382.0, new MaxSimDotProduct(scoreScript, hexidecimalString, fieldName).maxSimDotProduct(), 0.001);
            assertEquals(13.0, new MaxSimInvHamming(scoreScript, queryVector, fieldName).hamming(), 0.001);
            assertEquals(13.0, new MaxSimInvHamming(scoreScript, hexidecimalString, fieldName).hamming(), 0.001);
            MaxSimDotProduct maxSimDotProduct = new MaxSimDotProduct(scoreScript, queryVector, fieldName);
            when(scoreScript._getDocId()).thenReturn(1);
            e = expectThrows(IllegalArgumentException.class, maxSimDotProduct::maxSimDotProduct);
            assertEquals("A document doesn't have a value for a vector field!", e.getMessage());
        }
    }

    public void testBitMultiVectorClassBindingsDotProduct() throws IOException {
        String fieldName = "vector";
        int dims = 8;
        float[] docVector = new float[] { 124 };
        // 124 in binary is b01111100
        List<Number> queryVector = Arrays.asList((byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4, (byte) 1, (byte) 125, (byte) -12);
        List<Number> floatQueryVector = Arrays.asList(1.4f, -1.4f, 0.42f, 0.0f, 1f, -1f, -0.42f, 1.2f);
        List<Number> invalidQueryVector = Arrays.asList((byte) 1, (byte) 1);
        String hexidecimalString = HexFormat.of().formatHex(new byte[] { 124 });

        List<MultiDenseVectorDocValuesField> fields = List.of(
            new BitMultiDenseVectorDocValuesField(
                MultiDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.BIT, IndexVersion.current()),
                "test",
                ElementType.BIT,
                dims
            )
        );
        for (MultiDenseVectorDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field(fieldName)).thenAnswer(mock -> field);

            MaxSimDotProduct function = new MaxSimDotProduct(scoreScript, queryVector, fieldName);
            assertEquals(
                "maxSimDotProduct result is not equal to the expected value!",
                -12 + 2 + 4 + 1 + 125,
                function.maxSimDotProduct(),
                0.001
            );

            function = new MaxSimDotProduct(scoreScript, floatQueryVector, fieldName);
            assertEquals(
                "maxSimDotProduct result is not equal to the expected value!",
                0.42f + 0f + 1f - 1f - 0.42f,
                function.maxSimDotProduct(),
                0.001
            );

            function = new MaxSimDotProduct(scoreScript, hexidecimalString, fieldName);
            assertEquals(
                "maxSimDotProduct result is not equal to the expected value!",
                Integer.bitCount(124),
                function.maxSimDotProduct(),
                0.0
            );

            // Check each function rejects query vectors with the wrong dimension
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new MaxSimDotProduct(scoreScript, invalidQueryVector, fieldName)
            );
            assertThat(
                e.getMessage(),
                containsString(
                    "query vector has an incorrect number of dimensions. "
                        + "Must be [1] for bitwise operations, or [8] for byte wise operations: provided [2]."
                )
            );
        }
    }

    public void testByteVsFloatSimilarity() throws IOException {
        int dims = 5;
        float[] docVector = new float[] { 1f, 127f, -128f, 5f, -10f };
        List<Number> listFloatVector = Arrays.asList(1f, 125f, -12f, 2f, 4f);
        List<Number> listByteVector = Arrays.asList((byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4);
        float[] floatVector = new float[] { 1f, 125f, -12f, 2f, 4f };
        byte[] byteVector = new byte[] { (byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4 };

        List<MultiDenseVectorDocValuesField> fields = List.of(
            new FloatMultiDenseVectorDocValuesField(
                MultiDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.FLOAT, IndexVersions.MINIMUM_COMPATIBLE),
                "field0",
                ElementType.FLOAT,
                dims,
                IndexVersions.MINIMUM_COMPATIBLE
            ),
            new FloatMultiDenseVectorDocValuesField(
                MultiDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.FLOAT, IndexVersion.current()),
                "field1",
                ElementType.FLOAT,
                dims,
                IndexVersion.current()
            ),
            new ByteMultiDenseVectorDocValuesField(
                MultiDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.BYTE, IndexVersion.current()),
                "field3",
                ElementType.BYTE,
                dims
            )
        );
        for (MultiDenseVectorDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field("vector")).thenAnswer(mock -> field);

            int dotProductExpected = 17382;
            MaxSimDotProduct maxSimDotProduct = new MaxSimDotProduct(scoreScript, listFloatVector, "vector");
            assertEquals(field.getName(), dotProductExpected, maxSimDotProduct.maxSimDotProduct(), 0.001);
            maxSimDotProduct = new MaxSimDotProduct(scoreScript, listByteVector, "vector");
            assertEquals(field.getName(), dotProductExpected, maxSimDotProduct.maxSimDotProduct(), 0.001);
            assertEquals(field.getName(), dotProductExpected, field.get().maxSimDotProduct(listFloatVector), 0.001);
            assertEquals(field.getName(), dotProductExpected, field.get().maxSimDotProduct(listByteVector), 0.001);
            switch (field.getElementType()) {
                case BYTE -> {
                    assertEquals(field.getName(), dotProductExpected, field.get().maxSimDotProduct(byteVector));
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().maxSimDotProduct(floatVector)
                    );
                    assertThat(e.getMessage(), containsString("use [int maxSimDotProduct(byte[] queryVector)] instead"));
                }
                case FLOAT -> {
                    assertEquals(field.getName(), dotProductExpected, field.get().maxSimDotProduct(floatVector), 0.001);
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().maxSimDotProduct(byteVector)
                    );
                    assertThat(e.getMessage(), containsString("use [double maxSimDotProduct(float[] queryVector)] instead"));
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

        List<MultiDenseVectorDocValuesField> fields = List.of(
            new ByteMultiDenseVectorDocValuesField(
                MultiDenseVectorScriptDocValuesTests.wrap(new float[][] { docVector }, ElementType.BYTE, IndexVersion.current()),
                "test",
                ElementType.BYTE,
                dims
            )
        );

        for (MultiDenseVectorDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field(fieldName)).thenAnswer(mock -> field);

            IllegalArgumentException e;

            e = expectThrows(IllegalArgumentException.class, () -> new MaxSimDotProduct(scoreScript, greaterThanVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support integers between [-128, 127] but found [128.0] at dim [0]; "
                    + "Preview of invalid vector: [128.0]"
            );

            e = expectThrows(IllegalArgumentException.class, () -> new MaxSimDotProduct(scoreScript, lessThanVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support integers between [-128, 127] but found [-129.0] at dim [0]; "
                    + "Preview of invalid vector: [-129.0]"
            );
            e = expectThrows(IllegalArgumentException.class, () -> new MaxSimDotProduct(scoreScript, decimalVector, fieldName));
            assertEquals(
                e.getMessage(),
                "element_type [byte] vectors only support non-decimal values but found decimal value [0.5] at dim [0]; "
                    + "Preview of invalid vector: [0.5]"
            );
        }
    }

    public void testDimMismatch() throws IOException {

    }
}
