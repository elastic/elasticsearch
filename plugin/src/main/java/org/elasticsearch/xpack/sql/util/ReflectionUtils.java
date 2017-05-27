/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.util;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.util.Arrays;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

public class ReflectionUtils {

    @SuppressWarnings("unchecked")
    public static <E> Class<E> detectType(Type t) {
        if (t instanceof Class<?>) {
            return (Class<E>) t;
        }
        if (t instanceof ParameterizedType) {
            Type[] typeArguments = ((ParameterizedType) t).getActualTypeArguments();
            Assert.isTrue(typeArguments.length == 1,
                    "Unexpected number of type arguments %s for %s", Arrays.toString(typeArguments), t);

            return detectType(typeArguments[0]);
        }
        if (t instanceof WildcardType) {
            WildcardType wt = (WildcardType) t;
            if (wt.getLowerBounds().length == 1) {
                return detectType(wt.getLowerBounds()[0]);
            }
            Type[] upperBounds = wt.getUpperBounds();
            Assert.isTrue(upperBounds.length == 1,
                    "Unexpected number of upper bounds %s for %s", Arrays.toString(upperBounds), t);

            return detectType(upperBounds[0]);
        }
        if (t instanceof GenericArrayType) {
            return detectType(((GenericArrayType) t).getGenericComponentType());
        }

        throw new SqlIllegalArgumentException("Unrecognized type %s", t);
    }
    
    @SuppressWarnings("unchecked")
    public static <E> Class<E> detectSuperTypeForRuleLike(Class<?> c) {
        Class<?> clazz = c;
        for (Type type = clazz.getGenericSuperclass(); clazz != Object.class; type = clazz.getGenericSuperclass()) {
            if (type instanceof ParameterizedType) {
                Type[] typeArguments = ((ParameterizedType) type).getActualTypeArguments();
                Assert.isTrue(typeArguments.length == 2 || typeArguments.length == 1,
                        "Unexpected number of type arguments %s for %s", Arrays.toString(typeArguments), c);

                return (Class<E>) typeArguments[0];
            }
            clazz = clazz.getSuperclass();
        }
        throw new SqlIllegalArgumentException("Unexpected class structure for class %s", c);
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
