/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.elasticsearch.search.vectors.VectorData;

import java.util.Map;

public class SemanticTextFieldVectorSupplier implements FieldVectorSupplier {

    // hit.hit().getFields().getOrDefault("_inference_fields", null)

    // ((SemanticTextField)((HashMap)hit.hit().getFields().getOrDefault("_inference_fields",
    // null).getValues().get(0)).getOrDefault("content", null)).inference().chunks()
    // ((SemanticTextField)((HashMap)hit.hit().getFields().getOrDefault("_inference_fields",
    // null).getValues().get(0)).getOrDefault("content", null)).inference().chunks().get("content")
    // ((SemanticTextField)((HashMap)hit.hit().getFields().getOrDefault("_inference_fields",
    // null).getValues().get(0)).getOrDefault("content", null)).inference().chunks().get("content").get(0).rawEmbeddings()

    @Override
    public Map<Integer, VectorData> getFieldVectors() {
        return Map.of();
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
            if (fieldValue != null) {

                // chunks are found at:
                // ((SemanticTextField)fieldValue).inference().chunks().get(fieldName)
                // Embeddings:
                // for (var chunk : ((SemanticTextField)fieldValue).inference().chunks().get(fieldName)) {
                // var embeddingBytesArray = chunk.rawEmbeddings()
                // }

                return true;
            }
        }

        return false;
    }

}
