/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.lookup;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PainlessClassBinding {

    public final Constructor<?> javaConstructor;
    public final Method javaMethod;

    public final Class<?> returnType;
    public final List<Class<?>> typeParameters;
    public final Map<Class<?>, Object> annotations;

    PainlessClassBinding(Constructor<?> javaConstructor, Method javaMethod, Class<?> returnType, List<Class<?>> typeParameters,
        Map<Class<?>, Object> annotations) {
        this.javaConstructor = javaConstructor;
        this.javaMethod = javaMethod;

        this.returnType = returnType;
        this.typeParameters = typeParameters;
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

        PainlessClassBinding that = (PainlessClassBinding)object;

        return Objects.equals(javaConstructor, that.javaConstructor) &&
                Objects.equals(javaMethod, that.javaMethod) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(typeParameters, that.typeParameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(javaConstructor, javaMethod, returnType, typeParameters);
    }
}
