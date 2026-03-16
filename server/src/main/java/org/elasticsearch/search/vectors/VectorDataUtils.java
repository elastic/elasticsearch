/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Arrays;
import java.util.List;

/**
 * Helper to try and extract {@link VectorData} objects from generic object data, such as Lucene fields values
 */
public final class VectorDataUtils {
    public static DenseVectorFieldMapper.ElementType getDenseVectorElementType(Object value) {
        if (value == null) {
            return null;
        }

        var thisFieldValue = normalizeVectorFieldValue(value);

        switch (thisFieldValue) {
            case float[] floatArray -> {
                return DenseVectorFieldMapper.ElementType.FLOAT;
            }
            case byte[] byteArray -> {
                return DenseVectorFieldMapper.ElementType.BYTE;
            }
            case Float[] boxedFloatArray -> {
                return DenseVectorFieldMapper.ElementType.FLOAT;
            }
            case Byte[] boxedByteArray -> {
                return DenseVectorFieldMapper.ElementType.BYTE;
            }
            case Object[] objectArray -> {
                if (objectArray.length == 0) {
                    return null;
                }

                if (objectArray[0] instanceof Byte) {
                    return DenseVectorFieldMapper.ElementType.BYTE;
                }

                if (objectArray[0] instanceof Float) {
                    return DenseVectorFieldMapper.ElementType.FLOAT;
                }
            }
            default -> {
            }
        }

        return null;
    }

    public static VectorData extractVectorDataFromObject(Object value) {
        if (value == null) {
            return null;
        }

        var thisFieldValue = normalizeVectorFieldValue(value);
        if (thisFieldValue instanceof Object[] objectArray) {
            if (objectArray.length == 0) {
                return null;
            }

            if (objectArray[0] instanceof Byte) {
                thisFieldValue = Arrays.stream(objectArray).map(x -> (Byte) x).toArray(Byte[]::new);
            }

            if (objectArray[0] instanceof Float) {
                thisFieldValue = Arrays.stream(objectArray).map(x -> (Float) x).toArray(Float[]::new);
            }
        }

        switch (thisFieldValue) {
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

        return null;
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

    private static Object normalizeVectorFieldValue(Object value) {
        if (value instanceof List<?> asList && asList.isEmpty() == false) {
            if (asList.getFirst().getClass().isArray()) {
                // if it's a multivalued field object, get the first value
                return asList.getFirst();
            } else {
                return asList.toArray();
            }
        }
        return value;
    }
}
