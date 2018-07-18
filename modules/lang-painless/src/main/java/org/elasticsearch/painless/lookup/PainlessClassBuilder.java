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

import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandle;
import java.util.HashMap;
import java.util.Map;

final class PainlessClassBuilder {
    final String name;
    final Class<?> clazz;
    final Type type;

    final Map<String, PainlessMethod> constructors;
    final Map<String, PainlessMethod> staticMethods;
    final Map<String, PainlessMethod> methods;

    final Map<String, PainlessField> staticMembers;
    final Map<String, PainlessField> members;

    final Map<String, MethodHandle> getters;
    final Map<String, MethodHandle> setters;

    PainlessMethod functionalMethod;

    PainlessClassBuilder(String name, Class<?> clazz, Type type) {
        this.name = name;
        this.clazz = clazz;
        this.type = type;

        constructors = new HashMap<>();
        staticMethods = new HashMap<>();
        methods = new HashMap<>();

        staticMembers = new HashMap<>();
        members = new HashMap<>();

        getters = new HashMap<>();
        setters = new HashMap<>();

        functionalMethod = null;
    }

    PainlessClass build() {
        return new PainlessClass(name, clazz, type,
            constructors, staticMethods, methods,
            staticMembers, members,
            getters, setters,
            functionalMethod);
    }
}
