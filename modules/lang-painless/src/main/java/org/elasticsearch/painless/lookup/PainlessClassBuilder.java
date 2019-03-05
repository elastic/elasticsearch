/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

        runtimeMethods = new HashMap<>();
        getterMethodHandles = new HashMap<>();
        setterMethodHandles = new HashMap<>();
    }

    PainlessClass build() {
        return new PainlessClass(constructors, staticMethods, methods, staticFields, fields, functionalInterfaceMethod,
                runtimeMethods, getterMethodHandles, setterMethodHandles);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessClassBuilder that = (PainlessClassBuilder)object;

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
