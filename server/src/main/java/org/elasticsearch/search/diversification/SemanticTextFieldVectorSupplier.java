/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.search.vectors.VectorData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SemanticTextFieldVectorSupplier implements FieldVectorSupplier {

    private final String diversificationField;
    private Map<Integer, List<VectorData>> fieldVectors = null;

    public SemanticTextFieldVectorSupplier(String diversificationField) {
        this.diversificationField = diversificationField;
    }

    @Override
    public Map<Integer, List<VectorData>> getFieldVectors(DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits) {
        if (fieldVectors != null) {
            return fieldVectors;
        }

        fieldVectors = new HashMap<>();

        for (DiversifyRetrieverBuilder.RankDocWithSearchHit hit : searchHits) {
            List<VectorData> vectorData = extractVectorDataFromSearchHit(diversificationField, hit);
            if (vectorData != null && vectorData.isEmpty() == false) {
                fieldVectors.put(hit.rank, vectorData);
            }
        }

        return fieldVectors;
    }

    public static boolean isFieldSemanticTextVector(String fieldName, DiversifyRetrieverBuilder.RankDocWithSearchHit hit) {
        List<VectorData> vectorData = extractVectorDataFromSearchHit(fieldName, hit);
        return (vectorData != null && vectorData.isEmpty() == false);
    }

    private static List<VectorData> extractVectorDataFromSearchHit(String fieldName, DiversifyRetrieverBuilder.RankDocWithSearchHit hit) {
        var inferenceFieldValue = hit.hit().getFields().getOrDefault(InferenceMetadataFieldsMapper.NAME, null);
        if (inferenceFieldValue == null) {
            return null;
        }

        var fieldValues = inferenceFieldValue.getValues();
        if (fieldValues == null || fieldValues.isEmpty()) {
            return null;
        }

        if (fieldValues.getFirst() instanceof Map<?, ?> mappedValues) {
            var fieldValue = mappedValues.getOrDefault(fieldName, null);
            if (fieldValue instanceof DenseVectorSupplierField vectorSupplier) {
                return vectorSupplier.getDenseVectorData(fieldName);
            }
        }

        return null;
    }
}
