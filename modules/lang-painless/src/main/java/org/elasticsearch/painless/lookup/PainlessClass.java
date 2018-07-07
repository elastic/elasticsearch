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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class PainlessClass {
    public final String name;
    public final Class<?> clazz;
    public final Type type;

    public final Map<String, PainlessMethod> constructors;
    public final Map<String, PainlessMethod> staticMethods;
    public final Map<String, PainlessMethod> methods;

    public final Map<String, PainlessField> staticMembers;
    public final Map<String, PainlessField> members;

    public final Map<String, MethodHandle> getters;
    public final Map<String, MethodHandle> setters;

    public final PainlessMethod functionalMethod;

    PainlessClass(String name, Class<?> clazz, Type type) {
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

    PainlessClass(PainlessClass painlessClass, PainlessMethod functionalMethod) {
        name = painlessClass.name;
        clazz = painlessClass.clazz;
        type = painlessClass.type;

        constructors = Collections.unmodifiableMap(painlessClass.constructors);
        staticMethods = Collections.unmodifiableMap(painlessClass.staticMethods);
        methods = Collections.unmodifiableMap(painlessClass.methods);

        staticMembers = Collections.unmodifiableMap(painlessClass.staticMembers);
        members = Collections.unmodifiableMap(painlessClass.members);

        getters = Collections.unmodifiableMap(painlessClass.getters);
        setters = Collections.unmodifiableMap(painlessClass.setters);

        this.functionalMethod = functionalMethod;
    }

    public PainlessClass freeze(PainlessMethod functionalMethod) {
        return new PainlessClass(this, functionalMethod);
    }
}
