/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.util;

import java.util.Map;

/**
 * Utilities for mapping between primitive and boxed types and for obtaining default values.
 * Used when resolving method references and building instrumentation metadata.
 */
public final class TypeUtils {

    private static final Map<Class<?>, Class<?>> PRIMITIVE_TO_BOXED = Map.of(
        boolean.class,
        Boolean.class,
        byte.class,
        Byte.class,
        short.class,
        Short.class,
        char.class,
        Character.class,
        int.class,
        Integer.class,
        long.class,
        Long.class,
        float.class,
        Float.class,
        double.class,
        Double.class,
        void.class,
        Void.class
    );

    private static final Map<Class<?>, Class<?>> BOXED_TO_PRIMITIVE = Map.of(
        Boolean.class,
        boolean.class,
        Byte.class,
        byte.class,
        Short.class,
        short.class,
        Character.class,
        char.class,
        Integer.class,
        int.class,
        Long.class,
        long.class,
        Float.class,
        float.class,
        Double.class,
        double.class,
        Void.class,
        void.class
    );

    private TypeUtils() {}

    /**
     * Returns the primitive type corresponding to the given type, or the type itself if it is not a boxed primitive.
     */
    public static Class<?> toPrimitive(Class<?> type) {
        return BOXED_TO_PRIMITIVE.getOrDefault(type, type);
    }

    /**
     * Returns the boxed type corresponding to the given type, or the type itself if it is not a primitive.
     */
    public static Class<?> toBoxed(Class<?> type) {
        return PRIMITIVE_TO_BOXED.getOrDefault(type, type);
    }

    /**
     * Returns a default value for the given type (e.g. 0 for int, false for boolean, null for reference types).
     */
    public static Object defaultValueFor(Class<?> type) {
        if (type == void.class || type == Void.class) {
            return null;
        }
        if (type == boolean.class || type == Boolean.class) {
            return false;
        }
        if (type == byte.class || type == Byte.class) {
            return (byte) 0;
        }
        if (type == short.class || type == Short.class) {
            return (short) 0;
        }
        if (type == char.class || type == Character.class) {
            return (char) 0;
        }
        if (type == int.class || type == Integer.class) {
            return 0;
        }
        if (type == long.class || type == Long.class) {
            return 0L;
        }
        if (type == float.class || type == Float.class) {
            return 0f;
        }
        if (type == double.class || type == Double.class) {
            return 0d;
        }
        return null;
    }

    /**
     * Returns a string suitable for use as a parameter type in instrumentation keys (e.g. method descriptors).
     * Uses {@link Class#getName()} and appends "[]" for array component types.
     */
    public static String getParameterTypeName(Class<?> clazz) {
        if (clazz.isArray()) {
            return clazz.getComponentType().getName() + "[]";
        }
        return clazz.getName();
    }
}
