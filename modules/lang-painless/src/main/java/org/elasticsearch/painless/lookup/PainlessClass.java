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
import java.util.Map;

public final class PainlessClass {
    public final Map<String, PainlessConstructor> constructors;

    public final Map<String, PainlessMethod> staticMethods;
    public final Map<String, PainlessMethod> methods;

    public final Map<String, PainlessField> staticFields;
    public final Map<String, PainlessField> fields;

    public final Map<String, MethodHandle> getterMethodHandles;
    public final Map<String, MethodHandle> setterMethodHandles;

    public final PainlessMethod functionalMethod;

    PainlessClass(Map<String, PainlessConstructor> constructors,
            Map<String, PainlessMethod> staticMethods, Map<String, PainlessMethod> methods,
            Map<String, PainlessField> staticFields, Map<String, PainlessField> fields,
            Map<String, MethodHandle> getterMethodHandles, Map<String, MethodHandle> setterMethodHandles,
            PainlessMethod functionalMethod) {

        this.constructors = Collections.unmodifiableMap(constructors);

        this.staticMethods = Collections.unmodifiableMap(staticMethods);
        this.methods = Collections.unmodifiableMap(methods);

        this.staticFields = Collections.unmodifiableMap(staticFields);
        this.fields = Collections.unmodifiableMap(fields);

        this.getterMethodHandles = Collections.unmodifiableMap(getterMethodHandles);
        this.setterMethodHandles = Collections.unmodifiableMap(setterMethodHandles);

        this.functionalMethod = functionalMethod;
    }
}
