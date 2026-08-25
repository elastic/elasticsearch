/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.inference.VectorType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;

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

    /**
     * Maximum absolute error introduced by the cosine normalize/denormalize round trip. {@link DenseVectorFieldMapper} divides each
     * component by the vector's L2 length at index time and {@link DenormalizedCosineFloatVectorValues} multiplies it back on read,
     * using the identical float32 length in both directions. Each step rounds once, so the error is bounded by {@code 2 * 2^-24 ~=
     * 1.2e-7} relative; {@link VectorTestUtils#randomFloatVector} bounds every component to {@code [-1, +1)}, making that an absolute
     * bound.
     */
    private static final double COSINE_EPSILON = 1e-6;

    /**
     * Encoding used when writing the vector value into {@code _source}.
     */
    private enum SourceEncoding {
        ARRAY,
        BASE64,
        HEX
    }

    static class DenseVectorFieldConfig extends VectorFieldConfig<Object> {
        private final DenseVectorFieldMapper.ElementType elementType;
        private final boolean indexed;
        private final DenseVectorFieldMapper.VectorSimilarity similarity;
        private final DenseVectorFieldMapper.DenseVectorIndexOptions indexOptions;
        private final SourceEncoding sourceEncoding;
        private final int bfloat16BytesPerDim;

        DenseVectorFieldConfig(String fieldName) {
            this(fieldName, randomFrom(DenseVectorFieldMapper.ElementType.values()), randomBoolean());
        }

        private DenseVectorFieldConfig(String fieldName, DenseVectorFieldMapper.ElementType elementType, boolean indexed) {
            super(fieldName, randomValue(elementType));
            this.elementType = elementType;
            this.indexed = indexed;
            if (indexed) {
                List<SimilarityMeasure> supportedSimilarities = new ArrayList<>(getSupportedSimilarities(elementType));
                // Dot product requires unit vectors. This test generates random vectors, which may or may not be unit vectors.
                supportedSimilarities.remove(SimilarityMeasure.DOT_PRODUCT);
                this.similarity = randomFrom(supportedSimilarities).vectorSimilarity();
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
            this.sourceEncoding = randomSourceEncoding(elementType);
            this.bfloat16BytesPerDim = elementType == DenseVectorFieldMapper.ElementType.BFLOAT16
                ? randomFrom(Float.BYTES, BFloat16.BYTES)
                : 0;
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

        /**
         * Returns the value in the form that will be written into {@code _source}: a numeric array, a base64-encoded string, or a
         * hex-encoded string, according to the randomly chosen {@link #sourceEncoding}.
         */
        @Override
        public Object sourceValue() {
            return switch (sourceEncoding) {
                case ARRAY -> switch (elementType) {
                    case FLOAT, BFLOAT16 -> DenseVectorFieldMapperTests.convertToList((float[]) value());
                    // A List is written as a numeric array in every XContent type, whereas a primitive byte[] in a source map is
                    // written by XContentBuilder's binary writer and only round-trips correctly under JSON.
                    case BYTE, BIT -> DenseVectorFieldMapperTests.convertToList((byte[]) value());
                };
                case BASE64 -> switch (elementType) {
                    case FLOAT -> encodeBase64((float[]) value(), Float.BYTES);
                    case BFLOAT16 -> encodeBase64((float[]) value(), bfloat16BytesPerDim);
                    case BYTE, BIT -> Base64.getEncoder().encodeToString((byte[]) value());
                };
                case HEX -> HexFormat.of().formatHex((byte[]) value());
            };
        }

        @Override
        public String toString() {
            return Strings.format(
                "DenseVectorFieldConfig{fieldName=%s, elementType=%s, indexed=%s, similarity=%s, indexOptions=%s, sourceEncoding=%s,"
                    + " bfloat16BytesPerDim=%s}",
                fieldName(),
                elementType,
                indexed,
                similarity,
                indexOptions,
                sourceEncoding,
                bfloat16BytesPerDim
            );
        }

        private static Object randomValue(DenseVectorFieldMapper.ElementType elementType) {
            return switch (elementType) {
                case FLOAT, BFLOAT16 -> VectorTestUtils.randomFloatVector(VECTOR_DIMENSIONS);
                case BYTE -> VectorTestUtils.randomByteVector(VECTOR_DIMENSIONS);
                case BIT -> VectorTestUtils.randomByteVector(VECTOR_DIMENSIONS / Byte.SIZE);
            };
        }

        private static SourceEncoding randomSourceEncoding(DenseVectorFieldMapper.ElementType elementType) {
            return switch (elementType) {
                case FLOAT, BFLOAT16 -> randomFrom(SourceEncoding.ARRAY, SourceEncoding.BASE64);
                case BYTE, BIT -> randomFrom(SourceEncoding.ARRAY, SourceEncoding.BASE64, SourceEncoding.HEX);
            };
        }

        private static String encodeBase64(float[] vector, int bytesPerDim) {
            byte[] buf = new byte[bytesPerDim * vector.length];
            switch (bytesPerDim) {
                case BFloat16.BYTES -> BFloat16.floatToBFloat16(vector, 0, buf, 0, vector.length, ByteOrder.BIG_ENDIAN);
                case Float.BYTES -> ByteBuffer.wrap(buf).order(ByteOrder.BIG_ENDIAN).asFloatBuffer().put(vector);
                default -> throw new AssertionError("Unhandled bytes per dim [" + bytesPerDim + "]");
            }
            return Base64.getEncoder().encodeToString(buf);
        }
    }

    @ParametersFactory(argumentFormatting = "excludeSourceVectors=%b")
    public static Iterable<Object[]> parameters() {
        return List.of(new Object[] { false }, new Object[] { true });
    }

    public DenseVectorFieldEmbeddingsFieldIT(@Name("excludeSourceVectors") boolean excludeSourceVectors) {
        super(excludeSourceVectors);
    }

    @Override
    int vectorFieldCount() {
        return randomIntBetween(30, 50);
    }

    @Override
    DenseVectorFieldConfig createVectorFieldConfig(String fieldName) {
        return new DenseVectorFieldConfig(fieldName);
    }

    @Override
    void assertEmbeddingsFieldValue(String message, DenseVectorFieldConfig field, DocumentField actual) {
        switch (field.elementType) {
            case FLOAT -> assertFloatVector(
                message,
                field,
                actual,
                field.similarity == DenseVectorFieldMapper.VectorSimilarity.COSINE ? COSINE_EPSILON : 0.0
            );
            case BFLOAT16 -> assertFloatVector(message, field, actual, BFLOAT16_EPSILON);
            case BYTE, BIT -> {
                // Byte and bit vectors are fetched from doc values as a boxed Byte[].
                // Byte[] has no dedicated StreamOutput writer, so writeGenericValue falls back to the generic Object[] writer and the
                // value arrives at the coordinating node as an Object[] of Byte.
                // Cosine similarity does not normalize byte or bit vectors at index time, so this assertion is exact.
                Object[] fetchedVector = singleValue(message, actual);
                byte[] actualVector = new byte[fetchedVector.length];
                for (int i = 0; i < fetchedVector.length; i++) {
                    actualVector[i] = (Byte) fetchedVector[i];
                }
                assertArrayEquals(message + ": vector", (byte[]) field.value(), actualVector);
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
