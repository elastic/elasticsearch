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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class PainlessClass {
    public final String name;
    public final Class<?> clazz;
    public final org.objectweb.asm.Type type;

    public final Map<PainlessMethodKey, PainlessMethod> constructors;
    public final Map<PainlessMethodKey, PainlessMethod> staticMethods;
    public final Map<PainlessMethodKey, PainlessMethod> methods;

    public final Map<String, PainlessField> staticMembers;
    public final Map<String, PainlessField> members;

    public final Map<String, MethodHandle> getters;
    public final Map<String, MethodHandle> setters;

    public final PainlessMethod functionalMethod;

    PainlessClass(String name, Class<?> clazz, org.objectweb.asm.Type type) {
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

    private PainlessClass(PainlessClass struct, PainlessMethod functionalMethod) {
        name = struct.name;
        clazz = struct.clazz;
        type = struct.type;

        constructors = Collections.unmodifiableMap(struct.constructors);
        staticMethods = Collections.unmodifiableMap(struct.staticMethods);
        methods = Collections.unmodifiableMap(struct.methods);

        staticMembers = Collections.unmodifiableMap(struct.staticMembers);
        members = Collections.unmodifiableMap(struct.members);

        getters = Collections.unmodifiableMap(struct.getters);
        setters = Collections.unmodifiableMap(struct.setters);

        this.functionalMethod = functionalMethod;
    }

    public PainlessClass freeze(PainlessMethod functionalMethod) {
        return new PainlessClass(this, functionalMethod);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessClass struct = (PainlessClass)object;

        return name.equals(struct.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
