/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.lookup;

import java.lang.invoke.MethodHandle;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
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
    private final Map<Class<?>, Set<Class<?>>> classesToDirectSubClasses;

    private final Map<String, PainlessMethod> painlessMethodKeysToImportedPainlessMethods;
    private final Map<String, PainlessClassBinding> painlessMethodKeysToPainlessClassBindings;
    private final Map<String, PainlessInstanceBinding> painlessMethodKeysToPainlessInstanceBindings;

    PainlessLookup(
        Map<String, Class<?>> javaClassNamesToClasses,
        Map<String, Class<?>> canonicalClassNamesToClasses,
        Map<Class<?>, PainlessClass> classesToPainlessClasses,
        Map<Class<?>, Set<Class<?>>> classesToDirectSubClasses,
        Map<String, PainlessMethod> painlessMethodKeysToImportedPainlessMethods,
        Map<String, PainlessClassBinding> painlessMethodKeysToPainlessClassBindings,
        Map<String, PainlessInstanceBinding> painlessMethodKeysToPainlessInstanceBindings
    ) {
        this.javaClassNamesToClasses = Map.copyOf(javaClassNamesToClasses);
        this.canonicalClassNamesToClasses = Map.copyOf(canonicalClassNamesToClasses);
        this.classesToPainlessClasses = Map.copyOf(classesToPainlessClasses);
        this.classesToDirectSubClasses = Map.copyOf(classesToDirectSubClasses);

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

    public Set<Class<?>> getDirectSubClasses(Class<?> superClass) {
        return classesToDirectSubClasses.get(superClass);
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

        if (classesToPainlessClasses.containsKey(targetClass) == false) {
            return null;
        }

        if (targetClass.isPrimitive()) {
            targetClass = typeToBoxedType(targetClass);

            if (classesToPainlessClasses.containsKey(targetClass) == false) {
                return null;
            }
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, methodArity);
        Function<PainlessClass, PainlessMethod> objectLookup = isStatic
            ? targetPainlessClass -> targetPainlessClass.staticMethods.get(painlessMethodKey)
            : targetPainlessClass -> targetPainlessClass.methods.get(painlessMethodKey);

        return lookupPainlessObject(targetClass, objectLookup);
    }

    public List<PainlessMethod> lookupPainlessSubClassesMethod(Class<?> targetClass, String methodName, int methodArity) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);

        if (classesToPainlessClasses.containsKey(targetClass) == false) {
            return null;
        }

        if (targetClass.isPrimitive()) {
            targetClass = typeToBoxedType(targetClass);

            if (classesToPainlessClasses.containsKey(targetClass) == false) {
                return null;
            }
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, methodArity);
        Deque<Class<?>> subClasses = new ArrayDeque<>(classesToDirectSubClasses.get(targetClass));
        Set<Class<?>> resolvedSubClasses = new HashSet<>();
        List<PainlessMethod> subMethods = null;

        Class<?> subClass;
        while ((subClass = subClasses.pollFirst()) != null) {

            if (resolvedSubClasses.add(subClass)) {
                subClasses.addAll(classesToDirectSubClasses.get(subClass));

                PainlessClass painlessClass = classesToPainlessClasses.get(subClass);
                PainlessMethod painlessMethod = painlessClass.methods.get(painlessMethodKey);

                if (painlessMethod != null) {
                    if (subMethods == null) {
                        subMethods = new ArrayList<>();
                    }

                    subMethods.add(painlessMethod);
                }
            }
        }

        return subMethods;
    }

    public PainlessField lookupPainlessField(Class<?> targetClass, boolean isStatic, String fieldName) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(fieldName);

        if (classesToPainlessClasses.containsKey(targetClass) == false) {
            return null;
        }

        String painlessFieldKey = buildPainlessFieldKey(fieldName);
        Function<PainlessClass, PainlessField> objectLookup = isStatic
            ? targetPainlessClass -> targetPainlessClass.staticFields.get(painlessFieldKey)
            : targetPainlessClass -> targetPainlessClass.fields.get(painlessFieldKey);

        return lookupPainlessObject(targetClass, objectLookup);
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
        Function<PainlessClass, PainlessMethod> objectLookup = targetPainlessClass -> targetPainlessClass.runtimeMethods.get(
            painlessMethodKey
        );

        return lookupPainlessObject(originalTargetClass, objectLookup);
    }

    public MethodHandle lookupRuntimeGetterMethodHandle(Class<?> originalTargetClass, String getterName) {
        Objects.requireNonNull(originalTargetClass);
        Objects.requireNonNull(getterName);

        Function<PainlessClass, MethodHandle> objectLookup = targetPainlessClass -> targetPainlessClass.getterMethodHandles.get(getterName);

        return lookupPainlessObject(originalTargetClass, objectLookup);
    }

    public MethodHandle lookupRuntimeSetterMethodHandle(Class<?> originalTargetClass, String setterName) {
        Objects.requireNonNull(originalTargetClass);
        Objects.requireNonNull(setterName);

        Function<PainlessClass, MethodHandle> objectLookup = targetPainlessClass -> targetPainlessClass.setterMethodHandles.get(setterName);

        return lookupPainlessObject(originalTargetClass, objectLookup);
    }

    private <T> T lookupPainlessObject(Class<?> originalTargetClass, Function<PainlessClass, T> objectLookup) {
        Objects.requireNonNull(originalTargetClass);
        Objects.requireNonNull(objectLookup);

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

        if (originalTargetClass.isInterface()) {
            PainlessClass targetPainlessClass = classesToPainlessClasses.get(Object.class);

            if (targetPainlessClass != null) {
                T painlessObject = objectLookup.apply(targetPainlessClass);

                if (painlessObject != null) {
                    return painlessObject;
                }
            }
        }

        currentTargetClass = originalTargetClass;
        Set<Class<?>> resolvedInterfaces = new HashSet<>();

        while (currentTargetClass != null) {
            Deque<Class<?>> targetInterfaces = new ArrayDeque<>(Arrays.asList(currentTargetClass.getInterfaces()));

            Class<?> targetInterface;
            while ((targetInterface = targetInterfaces.pollFirst()) != null) {

                if (resolvedInterfaces.add(targetInterface)) {
                    PainlessClass targetPainlessClass = classesToPainlessClasses.get(targetInterface);

                    if (targetPainlessClass != null) {
                        T painlessObject = objectLookup.apply(targetPainlessClass);

                        if (painlessObject != null) {
                            return painlessObject;
                        }
                    }
                    targetInterfaces.addAll(Arrays.asList(targetInterface.getInterfaces()));
                }
            }

            currentTargetClass = currentTargetClass.getSuperclass();
        }

        return null;
    }
}
