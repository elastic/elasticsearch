/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.lookup;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;

public final class PainlessField {

    public final Field javaField;
    public final Class<?> typeParameter;
    public final Map<Class<?>, Object> annotations;

    public final MethodHandle getterMethodHandle;
    public final MethodHandle setterMethodHandle;

    PainlessField(Field javaField, Class<?> typeParameter, Map<Class<?>, Object> annotations,
            MethodHandle getterMethodHandle, MethodHandle setterMethodHandle) {

        this.javaField = javaField;
        this.typeParameter = typeParameter;
        this.annotations = annotations;

        this.getterMethodHandle = getterMethodHandle;
        this.setterMethodHandle = setterMethodHandle;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessField that = (PainlessField)object;

        return Objects.equals(javaField, that.javaField) &&
                Objects.equals(typeParameter, that.typeParameter) &&
                Objects.equals(annotations, that.annotations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(javaField, typeParameter, annotations);
    }
}
