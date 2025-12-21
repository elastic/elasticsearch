/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.search.vectors.VectorData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SemanticTextFieldVectorSupplier implements FieldVectorSupplier {

    private final String diversificationField;
    private final DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits;
    private Map<Integer, List<VectorData>> fieldVectors = null;

    public SemanticTextFieldVectorSupplier(String diversificationField, DiversifyRetrieverBuilder.RankDocWithSearchHit[] hits) {
        this.diversificationField = diversificationField;
        this.searchHits = hits;
    }

    @Override
    public Map<Integer, List<VectorData>> getFieldVectors() {
        if (fieldVectors != null) {
            return fieldVectors;
        }

        fieldVectors = new HashMap<>();

        for (DiversifyRetrieverBuilder.RankDocWithSearchHit hit : searchHits) {
            var inferenceFieldValue = hit.hit().getFields().getOrDefault("_inference_fields", null);
            if (inferenceFieldValue == null) {
                continue;
            }

            var fieldValues = inferenceFieldValue.getValues();
            if (fieldValues == null || fieldValues.isEmpty()) {
                continue;
            }

            if (fieldValues.getFirst() instanceof Map<?, ?> mappedValues) {
                var fieldValue = mappedValues.getOrDefault(diversificationField, null);
                if (fieldValue instanceof InferenceChunkSupplier chunkSupplier) {
                    List<ChunkedInference.Chunk> chunks = chunkSupplier.getChunks(diversificationField);
                    // chunks should be a bytesref value here that has the vector data

                    List<VectorData> vectorData = new ArrayList<>();

                    fieldVectors.put(hit.rank, vectorData);
                }
            }
        }

        return fieldVectors;
    }

    public static boolean isFieldSemanticTextVector(String fieldName, DiversifyRetrieverBuilder.RankDocWithSearchHit hit) {
        var inferenceFieldValue = hit.hit().getFields().getOrDefault("_inference_fields", null);
        if (inferenceFieldValue == null) {
            return false;
        }

        var fieldValues = inferenceFieldValue.getValues();
        if (fieldValues == null || fieldValues.isEmpty()) {
            return false;
        }

        if (fieldValues.getFirst() instanceof Map<?, ?> mappedValues) {
            var fieldValue = mappedValues.getOrDefault(fieldName, null);
            if (fieldValue instanceof InferenceChunkSupplier chunkSupplier) {
                List<ChunkedInference.Chunk> chunks = chunkSupplier.getChunks(fieldName);
                if (chunks == null || chunks.isEmpty()) {
                    return false;
                }
                return true;
            }
        }

        return false;
    }
}
