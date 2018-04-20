/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

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
                    throw new SqlIllegalArgumentException("Unexpected number of type arguments {} for {}", Arrays.toString(typeArguments),
                            c);
                }

                return (Class<E>) typeArguments[0];
            }
            clazz = clazz.getSuperclass();
        }
        throw new SqlIllegalArgumentException("Unexpected class structure for class {}", c);
    }
    
    // remove packaging from the name - strategy used for naming rules by default
    public static String ruleLikeNaming(Class<?> c) {
        String className = c.getName();
        int parentPackage = className.lastIndexOf(".");
        if (parentPackage > 0) {
            int grandParentPackage = className.substring(0, parentPackage).lastIndexOf(".");
            return (grandParentPackage > 0 ? className.substring(grandParentPackage + 1) : className.substring(parentPackage));
        }
        else {
            return className;
        }
    }
}
