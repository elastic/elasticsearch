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

public abstract class TypeToken<T> {
    private final Type type;
    private final Class<? super T> rawType;

    protected TypeToken() {
        Type superclass = getClass().getGenericSuperclass();
        if (superclass instanceof ParameterizedType parameterizedType) {
            this.type = parameterizedType.getActualTypeArguments()[0];
        } else {
            throw new IllegalArgumentException("TypeToken must be instantiated with generic type information.");
        }
        this.rawType = resolveRawType(type);
    }

    private TypeToken(Class<? super T> rawType) {
        this.type = rawType;
        this.rawType = rawType;
    }

    public static <T> TypeToken<T> of(Class<? super T> clazz) {
        return new SimpleTypeToken<>(clazz);
    }

    public Type getType() {
        return type;
    }

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
