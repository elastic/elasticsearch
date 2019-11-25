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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.DEF_CLASS_NAME;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessConstructorKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessFieldKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessMethodKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToBoxedType;

public final class PainlessLookup {

    private final Map<String, Class<?>> javaClassNamesToClasses;
    private final Map<String, Class<?>> canonicalClassNamesToClasses;
    private final Map<Class<?>, PainlessClass> classesToPainlessClasses;

    private final Map<String, PainlessMethod> painlessMethodKeysToImportedPainlessMethods;
    private final Map<String, PainlessClassBinding> painlessMethodKeysToPainlessClassBindings;
    private final Map<String, PainlessInstanceBinding> painlessMethodKeysToPainlessInstanceBindings;

    PainlessLookup(
            Map<String, Class<?>> javaClassNamesToClasses,
            Map<String, Class<?>> canonicalClassNamesToClasses,
            Map<Class<?>, PainlessClass> classesToPainlessClasses,
            Map<String, PainlessMethod> painlessMethodKeysToImportedPainlessMethods,
            Map<String, PainlessClassBinding> painlessMethodKeysToPainlessClassBindings,
            Map<String, PainlessInstanceBinding> painlessMethodKeysToPainlessInstanceBindings) {

        Objects.requireNonNull(javaClassNamesToClasses);
        Objects.requireNonNull(canonicalClassNamesToClasses);
        Objects.requireNonNull(classesToPainlessClasses);

        Objects.requireNonNull(painlessMethodKeysToImportedPainlessMethods);
        Objects.requireNonNull(painlessMethodKeysToPainlessClassBindings);
        Objects.requireNonNull(painlessMethodKeysToPainlessInstanceBindings);

        this.javaClassNamesToClasses = javaClassNamesToClasses;
        this.canonicalClassNamesToClasses = Map.copyOf(canonicalClassNamesToClasses);
        this.classesToPainlessClasses = Map.copyOf(classesToPainlessClasses);

        this.painlessMethodKeysToImportedPainlessMethods = Map.copyOf(painlessMethodKeysToImportedPainlessMethods);
        this.painlessMethodKeysToPainlessClassBindings = Map.copyOf(painlessMethodKeysToPainlessClassBindings);
        this.painlessMethodKeysToPainlessInstanceBindings = Map.copyOf(painlessMethodKeysToPainlessInstanceBindings);
    }

    public Class<?> javaClassNameToClass(String javaClassName) {
        return javaClassNamesToClasses.get(javaClassName);
    }

    public boolean isValidCanonicalClassName(String canonicalClassName) {
        Objects.requireNonNull(canonicalClassName);

        return DEF_CLASS_NAME.equals(canonicalClassName) || canonicalClassNamesToClasses.containsKey(canonicalClassName);
    }

    public Class<?> canonicalTypeNameToType(String canonicalTypeName) {
        Objects.requireNonNull(canonicalTypeName);

        return PainlessLookupUtility.canonicalTypeNameToType(canonicalTypeName, canonicalClassNamesToClasses);
    }

    public Set<Class<?>> getClasses() {
        return classesToPainlessClasses.keySet();
    }

    public Set<String> getImportedPainlessMethodsKeys() {
        return painlessMethodKeysToImportedPainlessMethods.keySet();
    }

    public Set<String> getPainlessClassBindingsKeys() {
        return painlessMethodKeysToPainlessClassBindings.keySet();
    }

    public Set<String> getPainlessInstanceBindingsKeys() {
        return painlessMethodKeysToPainlessInstanceBindings.keySet();
    }

    public PainlessClass lookupPainlessClass(Class<?> targetClass) {
        return classesToPainlessClasses.get(targetClass);
    }

    public PainlessConstructor lookupPainlessConstructor(String targetCanonicalClassName, int constructorArity) {
        Objects.requireNonNull(targetCanonicalClassName);

        Class<?> targetClass = canonicalTypeNameToType(targetCanonicalClassName);

        if (targetClass == null) {
            return null;
        }

        return lookupPainlessConstructor(targetClass, constructorArity);
    }

    public PainlessConstructor lookupPainlessConstructor(Class<?> targetClass, int constructorArity) {
        Objects.requireNonNull(targetClass);

        PainlessClass targetPainlessClass = classesToPainlessClasses.get(targetClass);
        String painlessConstructorKey = buildPainlessConstructorKey(constructorArity);

        if (targetPainlessClass == null) {
            return null;
        }

        PainlessConstructor painlessConstructor = targetPainlessClass.constructors.get(painlessConstructorKey);

        if (painlessConstructor == null) {
            return null;
        }

        return painlessConstructor;
    }

    public PainlessMethod lookupPainlessMethod(String targetCanonicalClassName, boolean isStatic, String methodName, int methodArity) {
        Objects.requireNonNull(targetCanonicalClassName);

        Class<?> targetClass = canonicalTypeNameToType(targetCanonicalClassName);

        if (targetClass == null) {
            return null;
        }

        return lookupPainlessMethod(targetClass, isStatic, methodName, methodArity);
    }

    public PainlessMethod lookupPainlessMethod(Class<?> targetClass, boolean isStatic, String methodName, int methodArity) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);

        if (targetClass.isPrimitive()) {
            targetClass = typeToBoxedType(targetClass);
        }

        PainlessClass targetPainlessClass = classesToPainlessClasses.get(targetClass);
        String painlessMethodKey = buildPainlessMethodKey(methodName, methodArity);

        if (targetPainlessClass == null) {
            return null;
        }

        return isStatic ?
                targetPainlessClass.staticMethods.get(painlessMethodKey) :
                targetPainlessClass.methods.get(painlessMethodKey);
    }

    public PainlessField lookupPainlessField(String targetCanonicalClassName, boolean isStatic, String fieldName) {
        Objects.requireNonNull(targetCanonicalClassName);

        Class<?> targetClass = canonicalTypeNameToType(targetCanonicalClassName);

        if (targetClass == null) {
            return null;
        }

        return lookupPainlessField(targetClass, isStatic, fieldName);
    }

    public PainlessField lookupPainlessField(Class<?> targetClass, boolean isStatic, String fieldName) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(fieldName);

        PainlessClass targetPainlessClass = classesToPainlessClasses.get(targetClass);
        String painlessFieldKey = buildPainlessFieldKey(fieldName);

        if (targetPainlessClass == null) {
            return null;
        }

        PainlessField painlessField = isStatic ?
                targetPainlessClass.staticFields.get(painlessFieldKey) :
                targetPainlessClass.fields.get(painlessFieldKey);

        if (painlessField == null) {
            return null;
        }

        return painlessField;
    }

    public PainlessMethod lookupImportedPainlessMethod(String methodName, int arity) {
        Objects.requireNonNull(methodName);

        String painlessMethodKey = buildPainlessMethodKey(methodName, arity);

        return painlessMethodKeysToImportedPainlessMethods.get(painlessMethodKey);
    }

    public PainlessClassBinding lookupPainlessClassBinding(String methodName, int arity) {
        Objects.requireNonNull(methodName);

        String painlessMethodKey = buildPainlessMethodKey(methodName, arity);

        return painlessMethodKeysToPainlessClassBindings.get(painlessMethodKey);
    }

    public PainlessInstanceBinding lookupPainlessInstanceBinding(String methodName, int arity) {
        Objects.requireNonNull(methodName);

        String painlessMethodKey = buildPainlessMethodKey(methodName, arity);

        return painlessMethodKeysToPainlessInstanceBindings.get(painlessMethodKey);
    }

    public PainlessMethod lookupFunctionalInterfacePainlessMethod(Class<?> targetClass) {
        PainlessClass targetPainlessClass = classesToPainlessClasses.get(targetClass);

        if (targetPainlessClass == null) {
            return null;
        }

        return targetPainlessClass.functionalInterfaceMethod;
    }

    public PainlessMethod lookupRuntimePainlessMethod(Class<?> originalTargetClass, String methodName, int methodArity) {
        Objects.requireNonNull(originalTargetClass);
        Objects.requireNonNull(methodName);

        String painlessMethodKey = buildPainlessMethodKey(methodName, methodArity);
        Function<PainlessClass, PainlessMethod> objectLookup =
                targetPainlessClass -> targetPainlessClass.runtimeMethods.get(painlessMethodKey);

        return lookupRuntimePainlessObject(originalTargetClass, objectLookup);
    }

    public MethodHandle lookupRuntimeGetterMethodHandle(Class<?> originalTargetClass, String getterName) {
        Objects.requireNonNull(originalTargetClass);
        Objects.requireNonNull(getterName);

        Function<PainlessClass, MethodHandle> objectLookup = targetPainlessClass -> targetPainlessClass.getterMethodHandles.get(getterName);

        return lookupRuntimePainlessObject(originalTargetClass, objectLookup);
    }

    public MethodHandle lookupRuntimeSetterMethodHandle(Class<?> originalTargetClass, String setterName) {
        Objects.requireNonNull(originalTargetClass);
        Objects.requireNonNull(setterName);

        Function<PainlessClass, MethodHandle> objectLookup = targetPainlessClass -> targetPainlessClass.setterMethodHandles.get(setterName);

        return lookupRuntimePainlessObject(originalTargetClass, objectLookup);
    }

    private <T> T lookupRuntimePainlessObject(Class<?> originalTargetClass, Function<PainlessClass, T> objectLookup) {
        Class<?> currentTargetClass = originalTargetClass;

        while (currentTargetClass != null) {
            PainlessClass targetPainlessClass = classesToPainlessClasses.get(currentTargetClass);

            if (targetPainlessClass != null) {
                T painlessObject = objectLookup.apply(targetPainlessClass);

                if (painlessObject != null) {
                    return painlessObject;
                }
            }

            currentTargetClass = currentTargetClass.getSuperclass();
        }

        currentTargetClass = originalTargetClass;

        while (currentTargetClass != null) {
            for (Class<?> targetInterface : currentTargetClass.getInterfaces()) {
                PainlessClass targetPainlessClass = classesToPainlessClasses.get(targetInterface);

                if (targetPainlessClass != null) {
                    T painlessObject = objectLookup.apply(targetPainlessClass);

                    if (painlessObject != null) {
                        return painlessObject;
                    }
                }
            }

            currentTargetClass = currentTargetClass.getSuperclass();
        }

        return null;
    }
}
