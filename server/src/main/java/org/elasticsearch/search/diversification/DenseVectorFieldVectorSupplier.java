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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DenseVectorFieldVectorSupplier implements FieldVectorSupplier {

    private final String diversificationField;
    private final DiversifyRetrieverBuilder.RankDocWithSearchHit[] searchHits;
    private Map<Integer, VectorData> fieldVectors = null;

    public DenseVectorFieldVectorSupplier(String diversificationField, DiversifyRetrieverBuilder.RankDocWithSearchHit[] hits) {
        this.diversificationField = diversificationField;
        this.searchHits = hits;
    }

    @Override
    public Map<Integer, VectorData> getFieldVectors() {
        if (fieldVectors != null) {
            return fieldVectors;
        }

        fieldVectors = new HashMap<>();
        for (DiversifyRetrieverBuilder.RankDocWithSearchHit searchHit : searchHits) {
            var field = searchHit.hit().getFields().getOrDefault(diversificationField, null);
            if (field != null) {
                var fieldValue = field.getValue();
                VectorData vector = extractFieldVectorData(fieldValue);
                if (vector != null) {
                    fieldVectors.put(searchHit.rank, vector);
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

        VectorData vector = extractFieldVectorData(field.getValue());
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

    private static VectorData extractFieldVectorData(Object fieldValue) {
        if (fieldValue == null) {
            return null;
        }

        switch (fieldValue) {
            case float[] floatArray -> {
                return new VectorData(floatArray);
            }
            case byte[] byteArray -> {
                return new VectorData(byteArray);
            }
            case Float[] boxedFloatArray -> {
                return new VectorData(unboxedFloatArray(boxedFloatArray));
            }
            case Byte[] boxedByteArray -> {
                return new VectorData(unboxedByteArray(boxedByteArray));
            }
            default -> {
            }
        }

        // CCS search returns a generic Object[] array, so we must
        // examine the individual element type here.
        if (fieldValue instanceof Object[] objectArray) {
            if (objectArray.length == 0) {
                return null;
            }

            if (objectArray[0] instanceof Byte) {
                Byte[] asByteArray = Arrays.stream(objectArray).map(x -> (Byte) x).toArray(Byte[]::new);
                return new VectorData(unboxedByteArray(asByteArray));
            }

            if (objectArray[0] instanceof Float) {
                Float[] asFloatArray = Arrays.stream(objectArray).map(x -> (Float) x).toArray(Float[]::new);
                return new VectorData(unboxedFloatArray(asFloatArray));
            }
        }

        return null;
    }
}
