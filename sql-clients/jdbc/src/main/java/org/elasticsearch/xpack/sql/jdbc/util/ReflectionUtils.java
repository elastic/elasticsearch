/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcException;

public abstract class ReflectionUtils {

    public static Field findField(Class<?> clazz, String name) {
        return findField(clazz, name, null);
    }

    public static Field findField(Class<?> clazz, String name, Class<?> type) {
        Assert.notNull(clazz, "Class must not be null");
        Assert.isTrue(name != null || type != null, "Either name or type of the field must be specified");
        Class<?> searchType = clazz;
        while (!Object.class.equals(searchType) && searchType != null) {
            Field[] fields = searchType.getDeclaredFields();
            for (Field field : fields) {
                if ((name == null || name.equals(field.getName())) && (type == null || type.equals(field.getType()))) {
                    return field;
                }
            }
            searchType = searchType.getSuperclass();
        }
        return null;
    }

    public static void makeAccessible(AccessibleObject accessible) {
        if (!accessible.isAccessible()) {
            accessible.setAccessible(true);
        }
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T getField(Field field, Object target) {
        try {
            return (T) field.get(target);
        } catch (IllegalAccessException ex) {
            throw new JdbcException("Unexpected reflection exception - %s: %s", ex.getClass().getName(), ex.getMessage());
        }
    }
    
    public static Method findMethod(Class<?> targetClass, String name, Class<?>... paramTypes) {
        while (targetClass != null) {
            Method[] methods = (targetClass.isInterface() ? targetClass.getMethods() : targetClass.getDeclaredMethods());
            for (Method method : methods) {
                if (name.equals(method.getName())
                        && (paramTypes == null || Arrays.equals(paramTypes, method.getParameterTypes()))) {
                    return method;
                }
            }
            targetClass = targetClass.getSuperclass();
        }
        return null;
    }
}
