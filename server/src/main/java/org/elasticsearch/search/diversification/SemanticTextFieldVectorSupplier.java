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
                if (fieldValue instanceof DenseVectorSupplierField vectorSupplier) {
                    List<VectorData> vectorData = vectorSupplier.getVectorData(diversificationField);
                    if (vectorData != null && vectorData.isEmpty() == false) {
                        fieldVectors.put(hit.rank, vectorData);
                    }
                }
            }
        }

        return fieldVectors;
    }

    private static float[] toFloatArray(byte[] values) {
        float[] floatArray = new float[values.length];
        for (int i = 0; i < values.length; i++) {
            floatArray[i] = ((Byte) values[i]).floatValue();
        }
        return floatArray;
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
            if (fieldValue instanceof DenseVectorSupplierField vectorSupplier) {
                List<VectorData> vectorData = vectorSupplier.getVectorData(fieldName);
                return (vectorData != null && vectorData.isEmpty() == false);
            }
        }

        return false;
    }
}
