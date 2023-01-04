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
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record PainlessConstructor(
    Constructor<?> javaConstructor,
    List<Class<?>> typeParameters,
    MethodHandle methodHandle,
    MethodType methodType,
    Map<Class<?>, Object> annotations
) {

    public PainlessConstructor(
        Constructor<?> javaConstructor,
        List<Class<?>> typeParameters,
        MethodHandle methodHandle,
        MethodType methodType,
        Map<Class<?>, Object> annotations
    ) {
        this.javaConstructor = javaConstructor;
        this.typeParameters = List.copyOf(typeParameters);
        this.methodHandle = methodHandle;
        this.methodType = methodType;
        this.annotations = Map.copyOf(annotations);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessConstructor that = (PainlessConstructor) object;
        return Objects.equals(javaConstructor, that.javaConstructor)
            && Objects.equals(typeParameters, that.typeParameters)
            && Objects.equals(methodType, that.methodType)
            && Objects.equals(annotations, that.annotations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(javaConstructor, typeParameters, methodType, annotations);
    }
}
