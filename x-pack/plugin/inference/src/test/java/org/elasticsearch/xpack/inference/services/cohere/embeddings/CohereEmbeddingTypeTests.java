/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class CohereEmbeddingTypeTests extends ESTestCase {
    public void testTranslateToVersion_ReturnsInt8_WhenVersionIsBeforeByteEnumAddition_WhenSpecifyingByte() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.BYTE, new TransportVersion(8_613_00_0)),
            is(CohereEmbeddingType.INT8)
        );
    }

    public void testTranslateToVersion_ReturnsInt8_WhenVersionIsBeforeByteEnumAddition_WhenSpecifyingInt8() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.INT8, new TransportVersion(8_613_00_0)),
            is(CohereEmbeddingType.INT8)
        );
    }

    public void testTranslateToVersion_ReturnsFloat_WhenVersionIsBeforeByteEnumAddition_WhenSpecifyingFloat() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.FLOAT, new TransportVersion(8_613_00_0)),
            is(CohereEmbeddingType.FLOAT)
        );
    }

    public void testTranslateToVersion_ReturnsByte_WhenVersionOnByteEnumAddition_WhenSpecifyingByte() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.BYTE, TransportVersions.V_8_14_0),
            is(CohereEmbeddingType.BYTE)
        );
    }

    public void testTranslateToVersion_ReturnsFloat_WhenVersionOnByteEnumAddition_WhenSpecifyingFloat() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.FLOAT, TransportVersions.V_8_14_0),
            is(CohereEmbeddingType.FLOAT)
        );
    }

    public void testTranslateToVersion_ReturnsInt8_WhenVersionIsBeforeBitEnumAdditionPatch_WhenSpecifyingBit() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.BIT, new TransportVersion(8_840_0_00)),
            is(CohereEmbeddingType.INT8)
        );
    }

    public void testTranslateToVersion_ReturnsInt8_WhenVersionIsBeforeBitEnumAddition_WhenSpecifyingBit() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.BIT, new TransportVersion(9_000_0_00)),
            is(CohereEmbeddingType.INT8)
        );
    }

    public void testTranslateToVersion_ReturnsBit_WhenVersionOnBitEnumAddition_WhenSpecifyingBit() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.BIT, TransportVersions.COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED),
            is(CohereEmbeddingType.BIT)
        );
    }

    public void testTranslateToVersion_ReturnsBit_WhenVersionOnBitEnumAdditionPatch_WhenSpecifyingBit() {
        assertThat(
            CohereEmbeddingType.translateToVersion(
                CohereEmbeddingType.BIT,
                TransportVersions.COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED_BACKPORT_8_X
            ),
            is(CohereEmbeddingType.BIT)
        );
    }

    public void testTranslateToVersion_ReturnsFloat_WhenVersionOnBitEnumAddition_WhenSpecifyingFloat() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.FLOAT, TransportVersions.COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED),
            is(CohereEmbeddingType.FLOAT)
        );
    }

    public void testFromElementType_CovertsFloatToCohereEmbeddingTypeFloat() {
        assertThat(CohereEmbeddingType.fromElementType(DenseVectorFieldMapper.ElementType.FLOAT), is(CohereEmbeddingType.FLOAT));
    }

    public void testFromElementType_CovertsByteToCohereEmbeddingTypeByte() {
        assertThat(CohereEmbeddingType.fromElementType(DenseVectorFieldMapper.ElementType.BYTE), is(CohereEmbeddingType.BYTE));
    }

    public void testFromElementType_ConvertsBitToCohereEmbeddingTypeBinary() {
        assertThat(CohereEmbeddingType.fromElementType(DenseVectorFieldMapper.ElementType.BIT), is(CohereEmbeddingType.BIT));
    }
}
