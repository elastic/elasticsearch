/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.objectweb.asm.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

class TestMethodUtils {

    /**
     * @return a {@link MethodKey} suitable for looking up the given {@code targetMethod} in the entitlements trampoline
     */
    static MethodKey methodKeyForTarget(Method targetMethod) {
        Type actualType = Type.getMethodType(Type.getMethodDescriptor(targetMethod));
        return new MethodKey(
            Type.getInternalName(targetMethod.getDeclaringClass()),
            targetMethod.getName(),
            Stream.of(actualType.getArgumentTypes()).map(Type::getInternalName).toList()
        );
    }

    static MethodKey methodKeyForConstructor(Class<?> classToInstrument, List<String> params) {
        return new MethodKey(classToInstrument.getName().replace('.', '/'), "<init>", params);
    }

    static CheckMethod getCheckMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
        var method = clazz.getMethod(methodName, parameterTypes);
        return new CheckMethod(
            Type.getInternalName(clazz),
            method.getName(),
            Arrays.stream(Type.getArgumentTypes(method)).map(Type::getDescriptor).toList()
        );
    }

    /**
     * Calling a static method of a dynamically loaded class is significantly more cumbersome
     * than calling a virtual method.
     */
    static void callStaticMethod(Class<?> c, String methodName, int arg) throws NoSuchMethodException, IllegalAccessException {
        try {
            c.getMethod(methodName, int.class).invoke(null, arg);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TestException n) {
                // Sometimes we're expecting this one!
                throw n;
            } else {
                throw new AssertionError(cause);
            }
        }
    }

    static void callStaticMethod(Class<?> c, String methodName, int arg1, String arg2) throws NoSuchMethodException,
        IllegalAccessException {
        try {
            c.getMethod(methodName, int.class, String.class).invoke(null, arg1, arg2);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TestException n) {
                // Sometimes we're expecting this one!
                throw n;
            } else {
                throw new AssertionError(cause);
            }
        }
    }
}
