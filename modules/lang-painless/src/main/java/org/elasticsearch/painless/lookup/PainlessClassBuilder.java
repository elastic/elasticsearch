/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.lookup;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

final class PainlessClassBuilder {

    final Map<String, PainlessConstructor> constructors;
    final Map<String, PainlessMethod> staticMethods;
    final Map<String, PainlessMethod> methods;
    final Map<String, PainlessField> staticFields;
    final Map<String, PainlessField> fields;
    PainlessMethod functionalInterfaceMethod;
    final Map<Class<?>, Object> annotations;

    final Map<String, PainlessMethod> runtimeMethods;
    final Map<String, MethodHandle> getterMethodHandles;
    final Map<String, MethodHandle> setterMethodHandles;

    PainlessClassBuilder() {
        constructors = new HashMap<>();
        staticMethods = new HashMap<>();
        methods = new HashMap<>();
        staticFields = new HashMap<>();
        fields = new HashMap<>();
        functionalInterfaceMethod = null;
        annotations = new HashMap<>();

        runtimeMethods = new HashMap<>();
        getterMethodHandles = new HashMap<>();
        setterMethodHandles = new HashMap<>();
    }

    PainlessClass build() {
        return new PainlessClass(
            constructors,
            staticMethods,
            methods,
            staticFields,
            fields,
            functionalInterfaceMethod,
            annotations,
            runtimeMethods,
            getterMethodHandles,
            setterMethodHandles
        );
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessClassBuilder that = (PainlessClassBuilder) object;

        return Objects.equals(constructors, that.constructors)
            && Objects.equals(staticMethods, that.staticMethods)
            && Objects.equals(methods, that.methods)
            && Objects.equals(staticFields, that.staticFields)
            && Objects.equals(fields, that.fields)
            && Objects.equals(functionalInterfaceMethod, that.functionalInterfaceMethod)
            && Objects.equals(annotations, that.annotations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(constructors, staticMethods, methods, staticFields, fields, functionalInterfaceMethod, annotations);
    }
}
