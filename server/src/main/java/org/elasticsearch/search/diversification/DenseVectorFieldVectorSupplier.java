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

public class DenseVectorFieldVectorSupplier implements FieldVectorSupplier {

    private final String diversificationField;
    private final DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits;
    private Map<Integer, List<VectorData>> fieldVectors = null;

    public DenseVectorFieldVectorSupplier(String diversificationField, DiversifyRetrieverBuilder.RankDocWithSearchHit[] hits) {
        this.diversificationField = diversificationField;
        this.searchHits = hits;
    }

    @Override
    public Map<Integer, List<VectorData>> getFieldVectors() {
        if (fieldVectors != null) {
            return fieldVectors;
        }

        fieldVectors = new HashMap<>();
        for (DiversifyRetrieverBuilder.RankDocWithSearchHit searchHit : searchHits) {
            var field = searchHit.hit().getFields().getOrDefault(diversificationField, null);
            if (field != null) {
                VectorData vector = extractFieldVectorData(field.getValues());
                if (vector != null) {
                    fieldVectors.put(searchHit.rank, List.of(vector));
                }
            }
        }

        return fieldVectors;
    }

    public static boolean canFieldBeDenseVector(String fieldName, DiversifyRetrieverBuilder.RankDocWithSearchHit hit) {
        var field = hit.hit().getFields().getOrDefault(fieldName, null);
        if (field == null) {
            return false;
        }

        VectorData vector = extractFieldVectorData(field.getValues());
        return vector != null;
    }

    private static float[] unboxedFloatArray(Float[] array) {
        float[] unboxedArray = new float[array.length];
        int bIndex = 0;
        for (Float b : array) {
            unboxedArray[bIndex++] = b;
        }
        return unboxedArray;
    }

    private static byte[] unboxedByteArray(Byte[] array) {
        byte[] unboxedArray = new byte[array.length];
        int bIndex = 0;
        for (Byte b : array) {
            unboxedArray[bIndex++] = b;
        }
        return unboxedArray;
    }

    private static VectorData extractFieldVectorData(List<?> fieldValues) {
        if (fieldValues == null || fieldValues.isEmpty()) {
            return null;
        }

        var firstValue = fieldValues.getFirst();

        if (firstValue instanceof float[] floatArray) {
            return new VectorData(floatArray);
        }

        if (firstValue instanceof byte[] byteArray) {
            return new VectorData(byteArray);
        }

        if (firstValue instanceof Float) {
            Float[] asFloatArray = fieldValues.stream().map(x -> (Float) x).toArray(Float[]::new);
            return new VectorData(unboxedFloatArray(asFloatArray));
        }

        if (firstValue instanceof Byte) {
            Byte[] asByteArray = fieldValues.stream().map(x -> (Byte) x).toArray(Byte[]::new);
            return new VectorData(unboxedByteArray(asByteArray));
        }

        return null;
    }
}
