/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.xpack.ql.QlIllegalArgumentException;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;

public class ReflectionUtils {

    @SuppressWarnings("unchecked")
    public static <E> Class<E> detectSuperTypeForRuleLike(Class<?> c) {
        Class<?> clazz = c;
        for (Type type = clazz.getGenericSuperclass(); clazz != Object.class; type = clazz.getGenericSuperclass()) {
            if (type instanceof ParameterizedType) {
                Type[] typeArguments = ((ParameterizedType) type).getActualTypeArguments();
                if (typeArguments.length != 2 && typeArguments.length != 1) {
                    throw new QlIllegalArgumentException(
                        "Unexpected number of type arguments {} for {}",
                        Arrays.toString(typeArguments),
                        c
                    );
                }

                Type tp = typeArguments[0];

                if (tp instanceof Class<?>) {
                    return (Class<E>) tp;
                } else if (tp instanceof ParameterizedType) {
                    Type rawType = ((ParameterizedType) tp).getRawType();
                    if (rawType instanceof Class<?>) {
                        return (Class<E>) rawType;
                    }
                }
                throw new QlIllegalArgumentException("Unexpected class structure for class {}", c);
            }
            clazz = clazz.getSuperclass();
        }
        throw new QlIllegalArgumentException("Unexpected class structure for class {}", c);
    }

    // remove packaging from the name - strategy used for naming rules by default
    public static String ruleLikeNaming(Class<?> c) {
        String className = c.getName();
        int parentPackage = className.lastIndexOf(".");
        if (parentPackage > 0) {
            int grandParentPackage = className.substring(0, parentPackage).lastIndexOf(".");
            return (grandParentPackage > 0 ? className.substring(grandParentPackage + 1) : className.substring(parentPackage));
        } else {
            return className;
        }
    }
}
