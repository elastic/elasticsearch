/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class CohereEmbeddingTypeTests extends ESTestCase {

    private static final TransportVersion COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED = TransportVersion.fromName(
        "cohere_bit_embedding_type_support_added"
    );
    private static final TransportVersion COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED_PATCH = COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED
        .nextPatchVersion();

    public void testTranslateToVersion_ReturnsInt8_WhenVersionIsBeforeBitEnumAddition_WhenSpecifyingBit() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.BIT, new TransportVersion(9_000_0_00)),
            is(CohereEmbeddingType.INT8)
        );
    }

    public void testTranslateToVersion_ReturnsBit_WhenVersionOnBitEnumAddition_WhenSpecifyingBit() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.BIT, COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED),
            is(CohereEmbeddingType.BIT)
        );
    }

    public void testTranslateToVersion_ReturnsBit_WhenVersionOnBitEnumAdditionPatch_WhenSpecifyingBit() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.BIT, COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED_PATCH),
            is(CohereEmbeddingType.BIT)
        );
    }

    public void testTranslateToVersion_ReturnsFloat_WhenVersionOnBitEnumAddition_WhenSpecifyingFloat() {
        assertThat(
            CohereEmbeddingType.translateToVersion(CohereEmbeddingType.FLOAT, COHERE_BIT_EMBEDDING_TYPE_SUPPORT_ADDED),
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
