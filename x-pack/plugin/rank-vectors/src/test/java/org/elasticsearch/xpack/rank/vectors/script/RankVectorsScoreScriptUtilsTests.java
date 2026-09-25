/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.script;

import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.ElementType;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.field.vectors.BFloat16RankVectorsDocValuesField;
import org.elasticsearch.script.field.vectors.BitRankVectorsDocValuesField;
import org.elasticsearch.script.field.vectors.ByteRankVectorsDocValuesField;
import org.elasticsearch.script.field.vectors.FloatRankVectorsDocValuesField;
import org.elasticsearch.script.field.vectors.RankVectorsDocValuesField;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsFieldMapper;
import org.elasticsearch.xpack.rank.vectors.mapper.RankVectorsScriptDocValuesTests;
import org.elasticsearch.xpack.rank.vectors.script.RankVectorsScoreScriptUtils.MaxSimDotProduct;
import org.elasticsearch.xpack.rank.vectors.script.RankVectorsScoreScriptUtils.MaxSimInvHamming;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RankVectorsScoreScriptUtilsTests extends ESTestCase {

    public void testFloatMultiVectorClassBindings() throws IOException {
        String fieldName = "vector";
        int dims = 5;
        float[][][] docVectors = new float[][][] {
            { { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f }, { 100.0f, 200.0f, -50.0f, 10.0f, -150.0f } } };
        float[][] docMagnitudes = new float[][] { { 0.0f, 0.0f } };
        for (int i = 0; i < docVectors.length; i++) {
            for (int j = 0; j < docVectors[i].length; j++) {
                docMagnitudes[i][j] = (float) Math.sqrt(VectorUtil.dotProduct(docVectors[i][j], docVectors[i][j]));
            }
        }

        // the first query vector scores highest against the document's first vector, the second against its second one,
        // so the result is only correct if each query vector takes its own maximum
        List<List<Number>> queryVector = List.of(
            Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f),
            Arrays.asList(0.0f, 0.0f, -1.0f, 0.0f, 0.0f)
        );
        List<List<Number>> invalidQueryVector = List.of(Arrays.asList(0.5, 111.3));

        RankVectorsDocValuesField field = new FloatRankVectorsDocValuesField(
            RankVectorsScriptDocValuesTests.wrap(docVectors, ElementType.FLOAT),
            RankVectorsScriptDocValuesTests.wrap(docMagnitudes),
            "test",
            ElementType.FLOAT,
            dims
        );
        field.setNextDocId(0);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript.field("vector")).thenAnswer(mock -> field);

        // Test max similarity dot product: 65425.63 against the first vector plus 50.0 against the second
        MaxSimDotProduct maxSimDotProduct = new MaxSimDotProduct(scoreScript, queryVector, fieldName);
        float maxSimDotProductExpected = 65475.625f;
        assertEquals(
            "maxSimDotProduct result is not equal to the expected value!",
            maxSimDotProductExpected,
            maxSimDotProduct.maxSimDotProduct(),
            0.001
        );

        // Check each function rejects query vectors with the wrong dimension
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new MaxSimDotProduct(scoreScript, invalidQueryVector, fieldName)
        );
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
        e = expectThrows(IllegalArgumentException.class, () -> new MaxSimInvHamming(scoreScript, invalidQueryVector, fieldName));
        assertThat(e.getMessage(), containsString("hamming distance is only supported for byte or bit vectors"));

        // Check scripting infrastructure integration
        assertEquals(65475.625, new MaxSimDotProduct(scoreScript, queryVector, fieldName).maxSimDotProduct(), 0.001);
        when(scoreScript._getDocId()).thenReturn(1);
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new MaxSimDotProduct(scoreScript, queryVector, fieldName).maxSimDotProduct()
        );
        assertEquals("A document doesn't have a value for a multi-vector field!", e.getMessage());
    }

    public void testBFloat16MultiVectorClassBindings() throws IOException {
        String fieldName = "vector";
        int dims = 5;
        float[][][] docVectors = new float[][][] {
            { { 230.0f, 300.33f, -34.8988f, 15.555f, -200.0f }, { 100.0f, 200.0f, -50.0f, 10.0f, -150.0f } } };
        float[][] docMagnitudes = new float[][] { { 0.0f, 0.0f } };
        for (int i = 0; i < docVectors.length; i++) {
            for (int j = 0; j < docVectors[i].length; j++) {
                docMagnitudes[i][j] = (float) Math.sqrt(VectorUtil.dotProduct(docVectors[i][j], docVectors[i][j]));
            }
        }

        List<List<Number>> queryVector = List.of(
            Arrays.asList(0.5f, 111.3f, -13.0f, 14.8f, -156.0f),
            Arrays.asList(0.0f, 0.0f, -1.0f, 0.0f, 0.0f)
        );
        List<List<Number>> invalidQueryVector = List.of(Arrays.asList(0.5, 111.3));

        RankVectorsDocValuesField field = new BFloat16RankVectorsDocValuesField(
            RankVectorsScriptDocValuesTests.wrap(docVectors, ElementType.BFLOAT16),
            RankVectorsScriptDocValuesTests.wrap(docMagnitudes),
            "test",
            ElementType.BFLOAT16,
            dims
        );
        field.setNextDocId(0);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript.field("vector")).thenAnswer(mock -> field);

        // Test max similarity dot product: 65390.32 against the first vector plus 50.0 against the second
        MaxSimDotProduct maxSimDotProduct = new MaxSimDotProduct(scoreScript, queryVector, fieldName);
        float maxSimDotProductExpected = 65440.32421875f;
        assertEquals(
            "maxSimDotProduct result is not equal to the expected value!",
            maxSimDotProductExpected,
            maxSimDotProduct.maxSimDotProduct(),
            0.1
        );

        // Check each function rejects query vectors with the wrong dimension
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new MaxSimDotProduct(scoreScript, invalidQueryVector, fieldName)
        );
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
        e = expectThrows(IllegalArgumentException.class, () -> new MaxSimInvHamming(scoreScript, invalidQueryVector, fieldName));
        assertThat(e.getMessage(), containsString("hamming distance is only supported for byte or bit vectors"));

        // Check scripting infrastructure integration
        assertEquals(65440.32421875, new MaxSimDotProduct(scoreScript, queryVector, fieldName).maxSimDotProduct(), 0.1);
        when(scoreScript._getDocId()).thenReturn(1);
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new MaxSimDotProduct(scoreScript, queryVector, fieldName).maxSimDotProduct()
        );
        assertEquals("A document doesn't have a value for a multi-vector field!", e.getMessage());
    }

    public void testByteMultiVectorClassBindings() throws IOException {
        String fieldName = "vector";
        int dims = 5;
        float[][] docVector = new float[][] { { 1, 127, -128, 5, -10 } };
        float[][] magnitudes = new float[][] { { 0 } };
        for (int i = 0; i < docVector.length; i++) {
            magnitudes[i][0] = (float) Math.sqrt(VectorUtil.dotProduct(docVector[i], docVector[i]));
        }
        List<List<Number>> queryVector = List.of(
            Arrays.asList((byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4),
            Arrays.asList((byte) 1, (byte) 0, (byte) 0, (byte) 0, (byte) 0)
        );
        List<List<Number>> invalidQueryVector = List.of(Arrays.asList((byte) 1, (byte) 1));
        List<String> hexidecimalString = List.of(
            HexFormat.of().formatHex(new byte[] { 1, 125, -12, 2, 4 }),
            HexFormat.of().formatHex(new byte[] { 1, 0, 0, 0, 0 })
        );

        RankVectorsDocValuesField field = new ByteRankVectorsDocValuesField(
            RankVectorsScriptDocValuesTests.wrap(new float[][][] { docVector }, ElementType.BYTE),
            RankVectorsScriptDocValuesTests.wrap(magnitudes),
            "test",
            ElementType.BYTE,
            dims
        );
        field.setNextDocId(0);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript.field(fieldName)).thenAnswer(mock -> field);

        // Check each function rejects query vectors with the wrong dimension
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new MaxSimDotProduct(scoreScript, invalidQueryVector, fieldName)
        );
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));
        e = expectThrows(IllegalArgumentException.class, () -> new MaxSimInvHamming(scoreScript, invalidQueryVector, fieldName));
        assertThat(e.getMessage(), containsString("query vector has a different number of dimensions [2] than the document vectors [5]"));

        // Check scripting infrastructure integration: 17382 for the first query vector plus 1 for the second
        assertEquals(17383.0, new MaxSimDotProduct(scoreScript, queryVector, fieldName).maxSimDotProduct(), 0.001);
        assertEquals(17383.0, new MaxSimDotProduct(scoreScript, hexidecimalString, fieldName).maxSimDotProduct(), 0.001);
        assertEquals(1.275, new MaxSimInvHamming(scoreScript, queryVector, fieldName).maxSimInvHamming(), 0.001);
        assertEquals(1.275, new MaxSimInvHamming(scoreScript, hexidecimalString, fieldName).maxSimInvHamming(), 0.001);
        MaxSimDotProduct maxSimDotProduct = new MaxSimDotProduct(scoreScript, queryVector, fieldName);
        when(scoreScript._getDocId()).thenReturn(1);
        e = expectThrows(IllegalArgumentException.class, maxSimDotProduct::maxSimDotProduct);
        assertEquals("A document doesn't have a value for a multi-vector field!", e.getMessage());
    }

    public void testBitMultiVectorClassBindingsDotProduct() throws IOException {
        String fieldName = "vector";
        int dims = 8;
        float[][] docVector = new float[][] { { 124 } };
        // 124 in binary is b01111100
        List<List<Number>> queryVector = List.of(
            Arrays.asList((byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4, (byte) 1, (byte) 125, (byte) -12),
            Arrays.asList((byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1, (byte) 1)
        );
        List<List<Number>> floatQueryVector = List.of(
            Arrays.asList(1.4f, -1.4f, 0.42f, 0.0f, 1f, -1f, -0.42f, 1.2f),
            Arrays.asList(1f, 1f, 1f, 1f, 1f, 1f, 1f, 1f)
        );
        List<List<Number>> invalidQueryVector = List.of(Arrays.asList((byte) 1, (byte) 1));
        List<String> hexidecimalString = List.of(HexFormat.of().formatHex(new byte[] { 124 }), HexFormat.of().formatHex(new byte[] { 96 }));

        RankVectorsDocValuesField field = new BitRankVectorsDocValuesField(
            RankVectorsScriptDocValuesTests.wrap(new float[][][] { docVector }, ElementType.BIT),
            RankVectorsScriptDocValuesTests.wrap(new float[][] { { 5 } }),
            "test",
            ElementType.BIT,
            dims
        );
        field.setNextDocId(0);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript.field(fieldName)).thenAnswer(mock -> field);

        // the second query vector sums the dimensions the document's set bits select, i.e. one per set bit
        MaxSimDotProduct function = new MaxSimDotProduct(scoreScript, queryVector, fieldName);
        assertEquals(
            "maxSimDotProduct result is not equal to the expected value!",
            (-12 + 2 + 4 + 1 + 125) + Integer.bitCount(124),
            function.maxSimDotProduct(),
            0.001
        );

        function = new MaxSimDotProduct(scoreScript, floatQueryVector, fieldName);
        assertEquals(
            "maxSimDotProduct result is not equal to the expected value!",
            (-1.4f + 0.42f + 0f + 1f - 1f) + Integer.bitCount(124),
            function.maxSimDotProduct(),
            0.001
        );

        function = new MaxSimDotProduct(scoreScript, hexidecimalString, fieldName);
        assertEquals(
            "maxSimDotProduct result is not equal to the expected value!",
            Integer.bitCount(124) + Integer.bitCount(124 & 96),
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
                "query vector contains inner vectors which have incorrect number of dimensions. "
                    + "Must be [1] for bitwise operations, or [8] for byte wise operations: provided [2]."
            )
        );
    }

    public void testByteVsFloatSimilarity() throws IOException {
        int dims = 5;
        float[][] docVector = new float[][] { { 1f, 127f, -128f, 5f, -10f } };
        float[][] magnitudes = new float[][] { { 0 } };
        for (int i = 0; i < docVector.length; i++) {
            magnitudes[i][0] = (float) Math.sqrt(VectorUtil.dotProduct(docVector[i], docVector[i]));
        }
        List<List<Number>> listFloatVector = List.of(Arrays.asList(1f, 125f, -12f, 2f, 4f));
        List<List<Number>> listByteVector = List.of(Arrays.asList((byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4));
        float[][] floatVector = new float[][] { { 1f, 125f, -12f, 2f, 4f } };
        byte[][] byteVector = new byte[][] { { (byte) 1, (byte) 125, (byte) -12, (byte) 2, (byte) 4 } };

        List<RankVectorsDocValuesField> fields = List.of(
            new FloatRankVectorsDocValuesField(
                RankVectorsScriptDocValuesTests.wrap(new float[][][] { docVector }, ElementType.FLOAT),
                RankVectorsScriptDocValuesTests.wrap(magnitudes),
                "field1",
                ElementType.FLOAT,
                dims
            ),
            new ByteRankVectorsDocValuesField(
                RankVectorsScriptDocValuesTests.wrap(new float[][][] { docVector }, ElementType.BYTE),
                RankVectorsScriptDocValuesTests.wrap(magnitudes),
                "field3",
                ElementType.BYTE,
                dims
            )
        );
        for (RankVectorsDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field("vector")).thenAnswer(mock -> field);

            int dotProductExpected = 17382;
            MaxSimDotProduct maxSimDotProduct = new MaxSimDotProduct(scoreScript, listFloatVector, "vector");
            assertEquals(field.getName(), dotProductExpected, maxSimDotProduct.maxSimDotProduct(), 0.001);
            maxSimDotProduct = new MaxSimDotProduct(scoreScript, listByteVector, "vector");
            assertEquals(field.getName(), dotProductExpected, maxSimDotProduct.maxSimDotProduct(), 0.001);
            switch (field.getElementType()) {
                case BYTE -> {
                    assertEquals(field.getName(), dotProductExpected, field.get().maxSimDotProduct(byteVector), 0.001);
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().maxSimDotProduct(floatVector)
                    );
                    assertThat(e.getMessage(), containsString("use [float maxSimDotProduct(byte[][] queryVector)] instead"));
                }
                case FLOAT -> {
                    assertEquals(field.getName(), dotProductExpected, field.get().maxSimDotProduct(floatVector), 0.001);
                    UnsupportedOperationException e = expectThrows(
                        UnsupportedOperationException.class,
                        () -> field.get().maxSimDotProduct(byteVector)
                    );
                    assertThat(e.getMessage(), containsString("use [float maxSimDotProduct(float[][] queryVector)] instead"));
                }
            }
        }
    }

    public void testByteBoundaries() throws IOException {
        String fieldName = "vector";
        int dims = 1;
        float[] docVector = new float[] { 0 };
        List<List<Number>> greaterThanVector = List.of(List.of(128));
        List<List<Number>> lessThanVector = List.of(List.of(-129));
        List<List<Number>> decimalVector = List.of(List.of(0.5));

        List<RankVectorsDocValuesField> fields = List.of(
            new ByteRankVectorsDocValuesField(
                RankVectorsScriptDocValuesTests.wrap(new float[][][] { { docVector } }, ElementType.BYTE),
                RankVectorsScriptDocValuesTests.wrap(new float[][] { { 1 } }),
                "test",
                ElementType.BYTE,
                dims
            )
        );

        for (RankVectorsDocValuesField field : fields) {
            field.setNextDocId(0);

            ScoreScript scoreScript = mock(ScoreScript.class);
            when(scoreScript.field(fieldName)).thenAnswer(mock -> field);

            IllegalArgumentException e;

            e = expectThrows(IllegalArgumentException.class, () -> new MaxSimDotProduct(scoreScript, greaterThanVector, fieldName));
            assertEquals(
                "element_type [byte] vectors only support integers between [-128, 127] but found [128.0] at dim [0]; "
                    + "Preview of invalid vector: [128.0]",
                e.getMessage()
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

    public void testByteBoundariesValidation() throws IOException {
        String fieldName = "vector";
        int dims = 3;
        float[] docVector = new float[] { 0, 0, 0 };
        // The offending value sits in the middle of a token, so it is only caught if every dimension is validated
        List<List<Number>> outOfBoundsVector = List.of(List.of(1, 200, 3), List.of(1, 2, 3));
        List<List<Number>> decimalVector = List.of(List.of(1, 2, 3), List.of(1, 0.5, 3));

        RankVectorsDocValuesField field = new ByteRankVectorsDocValuesField(
            RankVectorsScriptDocValuesTests.wrap(new float[][][] { { docVector } }, ElementType.BYTE),
            RankVectorsScriptDocValuesTests.wrap(new float[][] { { 1 } }),
            "test",
            ElementType.BYTE,
            dims
        );
        field.setNextDocId(0);

        ScoreScript scoreScript = mock(ScoreScript.class);
        when(scoreScript.field(fieldName)).thenAnswer(mock -> field);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new MaxSimDotProduct(scoreScript, outOfBoundsVector, fieldName)
        );
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support integers between [-128, 127] but found [200.0] at dim [1]")
        );

        e = expectThrows(IllegalArgumentException.class, () -> new MaxSimDotProduct(scoreScript, decimalVector, fieldName));
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support non-decimal values but found decimal value [0.5] at dim [1]")
        );

        e = expectThrows(IllegalArgumentException.class, () -> new MaxSimInvHamming(scoreScript, outOfBoundsVector, fieldName));
        assertThat(
            e.getMessage(),
            containsString("element_type [byte] vectors only support integers between [-128, 127] but found [200.0] at dim [1]")
        );
    }

    public void testTooManyQueryVectorsAreRejected() throws IOException {
        String fieldName = "vector";
        int dims = 8;
        int tooMany = RankVectorsFieldMapper.MAX_VECTORS + 1;
        String expectedMessage = "The query vector contains ["
            + tooMany
            + "] vectors, which exceeds the maximum of ["
            + RankVectorsFieldMapper.MAX_VECTORS
            + "].";

        float[][] floatDocVector = new float[][] { { 1, 127, -128, 5, -10, 1, 2, 3 } };
        float[][] magnitudes = new float[][] { { (float) Math.sqrt(VectorUtil.dotProduct(floatDocVector[0], floatDocVector[0])) } };
        List<List<Number>> floatQuery = Collections.nCopies(tooMany, Arrays.asList(0.5f, 1.5f, -1.0f, 2.0f, 0.0f, 1.0f, -2.0f, 3.0f));
        List<List<Number>> byteQuery = Collections.nCopies(
            tooMany,
            Arrays.asList((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5, (byte) 6, (byte) 7, (byte) 8)
        );

        RankVectorsDocValuesField floatField = new FloatRankVectorsDocValuesField(
            RankVectorsScriptDocValuesTests.wrap(new float[][][] { floatDocVector }, ElementType.FLOAT),
            RankVectorsScriptDocValuesTests.wrap(magnitudes),
            "test",
            ElementType.FLOAT,
            dims
        );
        floatField.setNextDocId(0);
        ScoreScript floatScript = mock(ScoreScript.class);
        when(floatScript.field(fieldName)).thenAnswer(mock -> floatField);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new MaxSimDotProduct(floatScript, floatQuery, fieldName)
        );
        assertEquals(expectedMessage, e.getMessage());

        RankVectorsDocValuesField byteField = new ByteRankVectorsDocValuesField(
            RankVectorsScriptDocValuesTests.wrap(new float[][][] { floatDocVector }, ElementType.BYTE),
            RankVectorsScriptDocValuesTests.wrap(magnitudes),
            "test",
            ElementType.BYTE,
            dims
        );
        byteField.setNextDocId(0);
        ScoreScript byteScript = mock(ScoreScript.class);
        when(byteScript.field(fieldName)).thenAnswer(mock -> byteField);
        e = expectThrows(IllegalArgumentException.class, () -> new MaxSimDotProduct(byteScript, byteQuery, fieldName));
        assertEquals(expectedMessage, e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new MaxSimDotProduct(
                byteScript,
                Collections.nCopies(tooMany, HexFormat.of().formatHex(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 })),
                fieldName
            )
        );
        assertEquals(expectedMessage, e.getMessage());

        RankVectorsDocValuesField bitField = new BitRankVectorsDocValuesField(
            RankVectorsScriptDocValuesTests.wrap(new float[][][] { new float[][] { { 124 } } }, ElementType.BIT),
            RankVectorsScriptDocValuesTests.wrap(new float[][] { { 5 } }),
            "test",
            ElementType.BIT,
            dims
        );
        bitField.setNextDocId(0);
        ScoreScript bitScript = mock(ScoreScript.class);
        when(bitScript.field(fieldName)).thenAnswer(mock -> bitField);
        e = expectThrows(IllegalArgumentException.class, () -> new MaxSimDotProduct(bitScript, byteQuery, fieldName));
        assertEquals(expectedMessage, e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> new MaxSimDotProduct(bitScript, Collections.nCopies(tooMany, HexFormat.of().formatHex(new byte[] { 124 })), fieldName)
        );
        assertEquals(expectedMessage, e.getMessage());
    }

}
