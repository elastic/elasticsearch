/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.inference.VectorType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Integration test that fetches embeddings from {@code sparse_vector} fields via
 * {@link org.elasticsearch.search.builder.SearchSourceBuilder#fetchEmbeddingsField}, exercising the
 * {@code EmbeddingsFieldSource.FIELDS} fetch path through {@link org.elasticsearch.search.fetch.subphase.FetchFieldsContext}.
 */
public class SparseVectorFieldEmbeddingsFieldIT extends AbstractVectorFieldEmbeddingsFieldIT<
    SparseVectorFieldEmbeddingsFieldIT.SparseVectorFieldConfig> {

    /**
     * Relative tolerance for fetched token weights.
     *
     * <p>When {@code _source} is synthetic, weights are rebuilt from the indexed {@code FeatureField} term frequency by
     * {@link XFeatureField#decodeFeatureValue}, which is {@code tf << 15}: the low 15 bits of the float bit-pattern are discarded,
     * retaining only the top 8 mantissa bits, truncated toward zero. For a value {@code (1 + f) * 2^e} the dropped mantissa
     * fraction is under {@code 2^-8}, giving a max relative error of {@code 2^-8} = 0.390625%
     * (e.g. 3.2f is returned as 3.1953125f). 0.5% leaves a comfortable margin above that bound.
     */
    private static final float WEIGHT_TOLERANCE_PERCENT = 0.005f;

    static class SparseVectorFieldConfig extends VectorFieldConfig<Map<String, Float>> {

        SparseVectorFieldConfig(String fieldName) {
            super(fieldName, randomWeights());
        }

        @Override
        public VectorType vectorType() {
            return VectorType.SPARSE_VECTOR;
        }

        @Override
        public void writeMapping(XContentBuilder builder) throws IOException {
            builder.startObject(fieldName()).field("type", SparseVectorFieldMapper.CONTENT_TYPE).endObject();
        }

        @Override
        public String toString() {
            return "SparseVectorFieldConfig{fieldName=" + fieldName() + "}";
        }

        private static Map<String, Float> randomWeights() {
            Map<String, Float> weights = new HashMap<>();
            int numTokens = randomIntBetween(1, 10);
            for (int i = 0; i < numTokens; i++) {
                weights.put(randomAlphaOfLengthBetween(3, 10), randomFloatBetween(1.0f, 9.0f, true));
            }
            return weights;
        }
    }

    @Override
    int vectorFieldCount() {
        return randomIntBetween(3, 5);
    }

    @Override
    SparseVectorFieldConfig createVectorFieldConfig(String fieldName) {
        return new SparseVectorFieldConfig(fieldName);
    }

    @Override
    void assertEmbeddingsFieldValue(String message, SparseVectorFieldConfig field, DocumentField actual) {
        // SourceValueFetcher.identity returns values parsed from JSON source, so numeric values are Double, not Float.
        // When _source is synthetic, weights are reconstructed from FeatureField term frequencies via XFeatureField.decodeFeatureValue, so
        // assertEqualsPercent is used with WEIGHT_TOLERANCE_PERCENT to absorb the quantization error.
        Map<String, ? extends Number> actualWeights = singleValue(message, actual);
        Map<String, Float> expectedWeights = field.value();

        assertThat(message + ": token set", actualWeights.keySet(), equalTo(expectedWeights.keySet()));
        for (Map.Entry<String, Float> entry : expectedWeights.entrySet()) {
            assertEqualsPercent(entry.getValue(), actualWeights.get(entry.getKey()).floatValue(), WEIGHT_TOLERANCE_PERCENT);
        }
    }
}
