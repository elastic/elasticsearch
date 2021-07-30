/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.lookup;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PainlessMethod {

    public final Method javaMethod;
    public final Class<?> targetClass;
    public final Class<?> returnType;
    public final List<Class<?>> typeParameters;
    public final MethodHandle methodHandle;
    public final MethodType methodType;
    public final Map<Class<?>, Object> annotations;

    public PainlessMethod(Method javaMethod, Class<?> targetClass, Class<?> returnType, List<Class<?>> typeParameters,
            MethodHandle methodHandle, MethodType methodType, Map<Class<?>, Object> annotations) {

        this.javaMethod = javaMethod;
        this.targetClass = targetClass;
        this.returnType = returnType;
        this.typeParameters = List.copyOf(typeParameters);
        this.methodHandle = methodHandle;
        this.methodType = methodType;
        this.annotations = annotations;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessMethod that = (PainlessMethod)object;

        return Objects.equals(javaMethod, that.javaMethod) &&
                Objects.equals(targetClass, that.targetClass) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(typeParameters, that.typeParameters) &&
                Objects.equals(methodType, that.methodType) &&
                Objects.equals(annotations, that.annotations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(javaMethod, targetClass, returnType, typeParameters, methodType, annotations);
    }
}
