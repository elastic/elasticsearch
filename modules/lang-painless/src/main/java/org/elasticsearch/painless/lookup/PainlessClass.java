/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.lookup;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.Objects;

public final class PainlessClass {

    public final Map<String, PainlessConstructor> constructors;
    public final Map<String, PainlessMethod> staticMethods;
    public final Map<String, PainlessMethod> methods;
    public final Map<String, PainlessField> staticFields;
    public final Map<String, PainlessField> fields;
    public final PainlessMethod functionalInterfaceMethod;

    public final Map<String, PainlessMethod> runtimeMethods;
    public final Map<String, MethodHandle> getterMethodHandles;
    public final Map<String, MethodHandle> setterMethodHandles;

    PainlessClass(Map<String, PainlessConstructor> constructors,
            Map<String, PainlessMethod> staticMethods, Map<String, PainlessMethod> methods,
            Map<String, PainlessField> staticFields, Map<String, PainlessField> fields,
            PainlessMethod functionalInterfaceMethod,
            Map<String, PainlessMethod> runtimeMethods,
            Map<String, MethodHandle> getterMethodHandles, Map<String, MethodHandle> setterMethodHandles) {

        this.constructors = Map.copyOf(constructors);
        this.staticMethods = Map.copyOf(staticMethods);
        this.methods = Map.copyOf(methods);
        this.staticFields = Map.copyOf(staticFields);
        this.fields = Map.copyOf(fields);
        this.functionalInterfaceMethod = functionalInterfaceMethod;

        this.getterMethodHandles = Map.copyOf(getterMethodHandles);
        this.setterMethodHandles = Map.copyOf(setterMethodHandles);
        this.runtimeMethods = Map.copyOf(runtimeMethods);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessClass that = (PainlessClass)object;

        return Objects.equals(constructors, that.constructors) &&
                Objects.equals(staticMethods, that.staticMethods) &&
                Objects.equals(methods, that.methods) &&
                Objects.equals(staticFields, that.staticFields) &&
                Objects.equals(fields, that.fields) &&
                Objects.equals(functionalInterfaceMethod, that.functionalInterfaceMethod);
    }

    @Override
    public int hashCode() {
        return Objects.hash(constructors, staticMethods, methods, staticFields, fields, functionalInterfaceMethod);
    }
}
