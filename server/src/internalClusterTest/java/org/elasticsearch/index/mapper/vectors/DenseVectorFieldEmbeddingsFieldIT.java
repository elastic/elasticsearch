/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.inference.VectorType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapperTestUtils.getSupportedSimilarities;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Integration test that fetches embeddings from {@code dense_vector} fields via
 * {@link org.elasticsearch.search.builder.SearchSourceBuilder#fetchEmbeddingsField}, exercising the
 * {@code EmbeddingsFieldSource.DOC_VALUES} fetch path through
 * {@link org.elasticsearch.search.fetch.subphase.FetchDocValuesContext}.
 */
public class DenseVectorFieldEmbeddingsFieldIT extends AbstractVectorFieldEmbeddingsFieldIT<
    DenseVectorFieldEmbeddingsFieldIT.DenseVectorFieldConfig> {

    /**
     * Dimension count for all dense vector fields. A multiple of 8 so that the {@code BIT} element type is supported, and large enough
     * to satisfy the minimum dimension requirements of BBQ index options.
     */
    static final int VECTOR_DIMENSIONS = 128;

    /**
     * Maximum absolute error introduced by BFLOAT16 rounding. {@link VectorTestUtils#randomFloatVector} bounds every component to
     * [-1, +1), and bfloat16 has an 8-bit significand rounded to nearest-even, so the error is at most 1 * 2^-8 ~= 0.004.
     */
    private static final double BFLOAT16_EPSILON = 0.004;

    static class DenseVectorFieldConfig extends VectorFieldConfig<Object> {
        private final DenseVectorFieldMapper.ElementType elementType;
        private final boolean indexed;
        private final DenseVectorFieldMapper.VectorSimilarity similarity;
        private final DenseVectorFieldMapper.DenseVectorIndexOptions indexOptions;

        DenseVectorFieldConfig(String fieldName) {
            this(fieldName, randomFrom(DenseVectorFieldMapper.ElementType.values()), randomBoolean());
        }

        private DenseVectorFieldConfig(String fieldName, DenseVectorFieldMapper.ElementType elementType, boolean indexed) {
            super(fieldName, randomValue(elementType));
            this.elementType = elementType;
            this.indexed = indexed;
            if (indexed) {
                this.similarity = randomFrom(getSupportedSimilarities(elementType)).vectorSimilarity();
                // Optionally pick a random index_options that is compatible with this element type and dimension count.
                // BBQ_DISK (BBQIVFIndexOptions) is skipped because it requires an enterprise VectorsFormatProvider that is not
                // available in the internalClusterTest cluster.
                this.indexOptions = randomBoolean()
                    ? null
                    : randomValueOtherThanMany(
                        opts -> opts instanceof DenseVectorFieldMapper.BBQIVFIndexOptions
                            || opts.validate(elementType, VECTOR_DIMENSIONS, false) == false,
                        DenseVectorFieldTypeTests::randomIndexOptionsAll
                    );
            } else {
                // index: false forbids both similarity and index_options.
                this.similarity = null;
                this.indexOptions = null;
            }
        }

        @Override
        public VectorType vectorType() {
            return VectorType.DENSE_VECTOR;
        }

        @Override
        public void writeMapping(XContentBuilder builder) throws IOException {
            builder.startObject(fieldName())
                .field("type", DenseVectorFieldMapper.CONTENT_TYPE)
                .field("dims", VECTOR_DIMENSIONS)
                .field("element_type", elementType)
                .field("index", indexed);
            if (similarity != null) {
                builder.field("similarity", similarity);
            }
            if (indexOptions != null) {
                builder.startObject("index_options");
                indexOptions.toXContentFragment(builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
            }
            builder.endObject();
        }

        @Override
        public String toString() {
            return Strings.format(
                "DenseVectorFieldConfig{fieldName=%s, elementType=%s, indexed=%s, similarity=%s, indexOptions=%s}",
                fieldName(),
                elementType,
                indexed,
                similarity,
                indexOptions
            );
        }

        private static Object randomValue(DenseVectorFieldMapper.ElementType elementType) {
            return switch (elementType) {
                case FLOAT, BFLOAT16 -> VectorTestUtils.randomFloatVector(VECTOR_DIMENSIONS);
                case BYTE -> VectorTestUtils.randomByteVector(VECTOR_DIMENSIONS);
                case BIT -> VectorTestUtils.randomByteVector(VECTOR_DIMENSIONS / Byte.SIZE);
            };
        }
    }

    @Override
    DenseVectorFieldConfig createVectorFieldConfig(String fieldName) {
        return new DenseVectorFieldConfig(fieldName);
    }

    @Override
    void assertEmbeddingsFieldValue(String message, DenseVectorFieldConfig field, DocumentField actual) {
        switch (field.elementType) {
            case FLOAT -> assertFloatVector(message, field, actual, 0.0);
            case BFLOAT16 -> assertFloatVector(message, field, actual, BFLOAT16_EPSILON);
            case BYTE, BIT -> {
                byte[] actualBytes = singleValue(message, actual);
                byte[] expectedBytes = (byte[]) field.value();
                assertArrayEquals(expectedBytes, actualBytes);
            }
        }
    }

    private static void assertFloatVector(String message, DenseVectorFieldConfig field, DocumentField actual, double epsilon) {
        float[] actualVector = singleValue(message, actual);
        float[] expectedVector = (float[]) field.value();
        assertThat(message + ": vector length", actualVector.length, equalTo(expectedVector.length));
        for (int i = 0; i < expectedVector.length; i++) {
            assertThat(message + ": vector[" + i + "]", (double) actualVector[i], closeTo(expectedVector[i], epsilon));
        }
    }
}
