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

    public Collection<Class<?>> getStructs() {
        return classesToPainlessClasses.keySet();
    }

    private final Map<String, Class<?>> canonicalClassNamesToClasses;
    private final Map<Class<?>, PainlessClass> classesToPainlessClasses;

    PainlessLookup(Map<String, Class<?>> canonicalClassNamesToClasses, Map<Class<?>, PainlessClass> classesToPainlessClasses) {
        this.canonicalClassNamesToClasses = Collections.unmodifiableMap(canonicalClassNamesToClasses);
        this.classesToPainlessClasses = Collections.unmodifiableMap(classesToPainlessClasses);
    }

    public Class<?> getClassFromBinaryName(String painlessType) {
        return canonicalClassNamesToClasses.get(painlessType.replace('$', '.'));
    }

    public boolean isSimplePainlessType(String painlessType) {
        return canonicalClassNamesToClasses.containsKey(painlessType);
    }

    public PainlessClass getPainlessStructFromJavaClass(Class<?> clazz) {
        return classesToPainlessClasses.get(clazz);
    }

    public Class<?> getJavaClassFromPainlessType(String painlessType) {
        return PainlessLookupUtility.canonicalTypeNameToType(painlessType, canonicalClassNamesToClasses);
    }
}
