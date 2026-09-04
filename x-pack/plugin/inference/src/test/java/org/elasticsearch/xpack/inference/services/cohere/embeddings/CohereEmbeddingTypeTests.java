/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;

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

    public void testFromCohereOrElementType_GivenCohereEmbeddingType() {
        for (var cohereEmbeddingType : CohereEmbeddingType.values()) {
            var embeddingType = CohereEmbeddingType.fromCohereOrElementType(cohereEmbeddingType.toString());
            assertThat(embeddingType, is(cohereEmbeddingType));
        }
    }

    public void testFromCohereOrElementType_GivenSupportedElementType() {
        for (var elementType : CohereEmbeddingType.SUPPORTED_ELEMENT_TYPES) {
            var embeddingType = CohereEmbeddingType.fromCohereOrElementType(elementType.toString());
            assertThat(embeddingType.toElementType(), is(elementType));
        }
    }

    public void testFromCohereOrElementType_GivenUnsupportedElementType() {
        for (var elementType : Sets.difference(
            EnumSet.allOf(DenseVectorFieldMapper.ElementType.class),
            CohereEmbeddingType.SUPPORTED_ELEMENT_TYPES
        )) {
            expectThrows(IllegalArgumentException.class, () -> CohereEmbeddingType.fromCohereOrElementType(elementType.toString()));
        }
    }
}
