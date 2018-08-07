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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessConstructorKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessFieldKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessMethodKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

public final class PainlessLookup {

    private final Map<String, Class<?>> canonicalClassNamesToClasses;
    private final Map<Class<?>, PainlessClass> classesToPainlessClasses;

    PainlessLookup(Map<String, Class<?>> canonicalClassNamesToClasses, Map<Class<?>, PainlessClass> classesToPainlessClasses) {
        Objects.requireNonNull(canonicalClassNamesToClasses);
        Objects.requireNonNull(classesToPainlessClasses);

        this.canonicalClassNamesToClasses = Collections.unmodifiableMap(canonicalClassNamesToClasses);
        this.classesToPainlessClasses = Collections.unmodifiableMap(classesToPainlessClasses);
    }

    public boolean isValidCanonicalClassName(String canonicalClassName) {
        Objects.requireNonNull(canonicalClassName);

        return canonicalClassNamesToClasses.containsKey(canonicalClassName);
    }

    public Class<?> canonicalTypeNameToType(String painlessType) {
        Objects.requireNonNull(painlessType);

        return PainlessLookupUtility.canonicalTypeNameToType(painlessType, canonicalClassNamesToClasses);
    }

    public Set<Class<?>> getClasses() {
        return classesToPainlessClasses.keySet();
    }

    public PainlessClass lookupPainlessClass(Class<?> targetClass) {
        return classesToPainlessClasses.get(targetClass);
    }

    public PainlessConstructor lookupPainlessConstructor(Class<?> targetClass, int constructorArity) {
        Objects.requireNonNull(targetClass);

        PainlessClass targetPainlessClass = classesToPainlessClasses.get(targetClass);
        String painlessConstructorKey = buildPainlessConstructorKey(constructorArity);

        if (targetPainlessClass == null) {
            throw new IllegalArgumentException("target class [" + typeToCanonicalTypeName(targetClass) + "] " +
                    "not found for constructor [" + painlessConstructorKey + "]");
        }

        PainlessConstructor painlessConstructor = targetPainlessClass.constructors.get(painlessConstructorKey);

        if (painlessConstructor == null) {
            throw new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(targetClass) + ", " + painlessConstructorKey + "] not found");
        }

        return painlessConstructor;
    }

    public PainlessMethod lookupPainlessMethod(Class<?> targetClass, boolean isStatic, String methodName, int methodArity) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);

        if (targetClass.isPrimitive()) {
            targetClass = PainlessLookupUtility.typeToBoxedType(targetClass);
        }

        PainlessClass targetPainlessClass = classesToPainlessClasses.get(targetClass);
        String painlessMethodKey = buildPainlessMethodKey(methodName, methodArity);

        if (targetPainlessClass == null) {
            throw new IllegalArgumentException(
                    "target class [" + typeToCanonicalTypeName(targetClass) + "] not found for method [" + painlessMethodKey + "]");
        }

        PainlessMethod painlessMethod = isStatic ?
                targetPainlessClass.staticMethods.get(painlessMethodKey) :
                targetPainlessClass.methods.get(painlessMethodKey);

        if (painlessMethod == null) {
            throw new IllegalArgumentException(
                    "method [" + typeToCanonicalTypeName(targetClass) + ", " + painlessMethodKey + "] not found");
        }

        return painlessMethod;
    }

    public PainlessField lookupPainlessField(Class<?> targetClass, boolean isStatic, String fieldName) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(fieldName);

        PainlessClass targetPainlessClass = classesToPainlessClasses.get(targetClass);
        String painlessFieldKey = buildPainlessFieldKey(fieldName);

        if (targetPainlessClass == null) {
            throw new IllegalArgumentException(
                    "target class [" + typeToCanonicalTypeName(targetClass) + "] not found for field [" + painlessFieldKey + "]");
        }

        PainlessField painlessField = isStatic ?
                targetPainlessClass.staticFields.get(painlessFieldKey) :
                targetPainlessClass.fields.get(painlessFieldKey);

        if (painlessField == null) {
            throw new IllegalArgumentException(
                    "field [" + typeToCanonicalTypeName(targetClass) + ", " + painlessFieldKey + "] not found");
        }

        return painlessField;
    }
}
