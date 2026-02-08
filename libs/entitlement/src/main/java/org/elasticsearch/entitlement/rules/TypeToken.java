/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.rules;

import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Utility class for capturing and representing generic type information at runtime.
 * <p>
 * This class allows preserving type parameter information that would normally be erased
 * due to Java's type erasure. It's particularly useful when working with generic methods
 * and APIs that need to maintain type information.
 * <p>
 * Example usage:
 * <pre>{@code
 * TypeToken<List<String>> token = new TypeToken<List<String>>() {};
 * Type type = token.getType(); // Returns ParameterizedType for List<String>
 * Class<?> rawType = token.getRawType(); // Returns List.class
 * }</pre>
 *
 * @param <T> the type this token represents
 */
public abstract class TypeToken<T> {
    private final Type type;
    private final Class<? super T> rawType;

    /**
     * Protected constructor for subclasses to capture generic type information.
     * <p>
     * Subclasses must be instantiated with explicit type parameters to capture
     * the generic type information.
     *
     * @throws IllegalArgumentException if instantiated without generic type information
     */
    protected TypeToken() {
        Type superclass = getClass().getGenericSuperclass();
        if (superclass instanceof ParameterizedType parameterizedType) {
            this.type = parameterizedType.getActualTypeArguments()[0];
        } else {
            throw new IllegalArgumentException("TypeToken must be instantiated with generic type information.");
        }
        this.rawType = resolveRawType(type);
    }

    /**
     * Private constructor for creating simple type tokens from class objects.
     *
     * @param rawType the raw class type
     */
    private TypeToken(Class<? super T> rawType) {
        this.type = rawType;
        this.rawType = rawType;
    }

    /**
     * Creates a type token for a simple (non-generic) class.
     *
     * @param <T> the type parameter
     * @param clazz the class to create a type token for
     * @return a type token representing the specified class
     */
    public static <T> TypeToken<T> of(Class<? super T> clazz) {
        return new SimpleTypeToken<>(clazz);
    }

    /**
     * Returns the generic type information captured by this token.
     *
     * @return the type represented by this token
     */
    public Type getType() {
        return type;
    }

    /**
     * Returns the raw class type, erasing any generic type information.
     *
     * @return the raw class of the type represented by this token
     */
    public Class<? super T> getRawType() {
        return rawType;
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<? super T> resolveRawType(Type type) {
        if (type instanceof Class<?> clazz) {
            return (Class<? super T>) clazz;
        } else if (type instanceof ParameterizedType parameterizedType) {
            return (Class<? super T>) parameterizedType.getRawType();
        } else if (type instanceof GenericArrayType arrayType) {
            return (Class<? super T>) Array.newInstance(resolveRawType(arrayType.getGenericComponentType()), 0).getClass();
        } else {
            throw new IllegalArgumentException("Unsupported type: " + type.getClass());
        }
    }

    private static class SimpleTypeToken<T> extends TypeToken<T> {
        private SimpleTypeToken(Class<? super T> rawType) {
            super(rawType);
        }
    }
}
