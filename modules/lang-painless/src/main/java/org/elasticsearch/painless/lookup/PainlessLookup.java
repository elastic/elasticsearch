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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * The entire API for Painless.  Also used as a whitelist for checking for legal
 * methods and fields during at both compile-time and runtime.
 */
public final class PainlessLookup {

    public Collection<PainlessClass> getStructs() {
        return javaClassesToPainlessStructs.values();
    }

    private final Map<String, Class<?>> painlessTypesToJavaClasses;
    private final Map<Class<?>, PainlessClass> javaClassesToPainlessStructs;

    PainlessLookup(Map<String, Class<?>> painlessTypesToJavaClasses, Map<Class<?>, PainlessClass> javaClassesToPainlessStructs) {
        this.painlessTypesToJavaClasses = Collections.unmodifiableMap(painlessTypesToJavaClasses);
        this.javaClassesToPainlessStructs = Collections.unmodifiableMap(javaClassesToPainlessStructs);
    }

    public Class<?> getClassFromBinaryName(String painlessType) {
        return painlessTypesToJavaClasses.get(painlessType.replace('$', '.'));
    }

    public boolean isSimplePainlessType(String painlessType) {
        return painlessTypesToJavaClasses.containsKey(painlessType);
    }

    public PainlessClass getPainlessStructFromJavaClass(Class<?> clazz) {
        return javaClassesToPainlessStructs.get(clazz);
    }

    public Class<?> getJavaClassFromPainlessType(String painlessType) {
        return PainlessLookupUtility.painlessTypeNameToPainlessType(painlessType, painlessTypesToJavaClasses);
    }
}
