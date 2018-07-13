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
import org.objectweb.asm.commons.Method;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.CONSTRUCTOR_ANY_NAME;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.DEF_PAINLESS_CLASS_NAME;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.anyTypeNameToPainlessTypeName;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessFieldKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessMethodKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.painlessDefTypeToJavaObjectType;

public final class PainlessLookupBuilder {

    private static class PainlessMethodCacheKey {

        private final Class<?> javaClass;
        private final String methodName;
        private final List<Class<?>> painlessTypeParameters;

        private PainlessMethodCacheKey(Class<?> javaClass, String methodName, List<Class<?>> painlessTypeParameters) {
            this.javaClass = javaClass;
            this.methodName = methodName;
            this.painlessTypeParameters = Collections.unmodifiableList(painlessTypeParameters);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PainlessMethodCacheKey that = (PainlessMethodCacheKey) o;
            return Objects.equals(javaClass, that.javaClass) &&
                Objects.equals(methodName, that.methodName) &&
                Objects.equals(painlessTypeParameters, that.painlessTypeParameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(javaClass, methodName, painlessTypeParameters);
        }
    }

    private static class PainlessFieldCacheKey {

        private final Class<?> javaClass;
        private final String fieldName;
        private final Class<?> painlessType;

        private PainlessFieldCacheKey(Class<?> javaClass, String fieldName, Class<?> painlessType) {
            this.javaClass = javaClass;
            this.fieldName = fieldName;
            this.painlessType = painlessType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PainlessFieldCacheKey that = (PainlessFieldCacheKey) o;
            return Objects.equals(javaClass, that.javaClass) &&
                Objects.equals(fieldName, that.fieldName) &&
                Objects.equals(painlessType, that.painlessType);
        }

        @Override
        public int hashCode() {
            return Objects.hash(javaClass, fieldName, painlessType);
        }
    }

    private static final Map<PainlessMethodCacheKey, PainlessMethod> painlessMethodCache = new HashMap<>();
    private static final Map<PainlessFieldCacheKey,  PainlessField>  painlessFieldCache  = new HashMap<>();

    private static final Pattern CLASS_NAME_PATTERN  = Pattern.compile("^[_a-zA-Z][._a-zA-Z0-9]*$");
    private static final Pattern METHOD_NAME_PATTERN = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");
    private static final Pattern FIELD_NAME_PATTERN  = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");

    private static final String TRANSIENT_JAVA_CLASS_NAME = "transient";

    private static String anyTypesArrayToCanonicalString(Class<?>[] anyTypesArray, boolean toPainlessTypes) {
        return anyTypesListToCanonicalString(Arrays.asList(anyTypesArray), toPainlessTypes);
    }

    private static String anyTypesListToCanonicalString(List<Class<?>> anyTypesList, boolean toPainlessTypes) {
        StringBuilder anyTypesCanonicalStringBuilder = new StringBuilder("[");

        int anyTypesSize = anyTypesList.size();
        int anyTypesIndex = 0;

        for (Class<?> anyType : anyTypesList) {
            String anyTypeCanonicalName = anyType.getCanonicalName();

            if (toPainlessTypes) {
                anyTypeCanonicalName = anyTypeNameToPainlessTypeName(anyTypeCanonicalName);
            }

            anyTypesCanonicalStringBuilder.append(anyTypeCanonicalName);

            if (++anyTypesIndex < anyTypesSize) {
                anyTypesCanonicalStringBuilder.append(",");
            }
        }

        anyTypesCanonicalStringBuilder.append("]");

        return anyTypesCanonicalStringBuilder.toString();
    }

    private final Map<String, Class<?>> painlessClassNamesToJavaClasses;
    private final Map<Class<?>, PainlessClassBuilder> javaClassesToPainlessClassBuilders;

    public PainlessLookupBuilder() {
        painlessClassNamesToJavaClasses = new HashMap<>();
        javaClassesToPainlessClassBuilders = new HashMap<>();

        painlessClassNamesToJavaClasses.put(DEF_PAINLESS_CLASS_NAME, def.class);
        javaClassesToPainlessClassBuilders.put(def.class,
                new PainlessClassBuilder(DEF_PAINLESS_CLASS_NAME, Object.class, Type.getType(Object.class)));
    }

    private Class<?> painlessTypeNameToPainlessType(String painlessTypeName) {
        return PainlessLookupUtility.painlessTypeNameToPainlessType(painlessTypeName, painlessClassNamesToJavaClasses);
    }

    private void validatePainlessType(Class<?> painlessType) {
        PainlessLookupUtility.validatePainlessType(painlessType, javaClassesToPainlessClassBuilders.keySet());
    }

    public void addPainlessClass(ClassLoader classLoader, String javaClassName, boolean importPainlessClassName) {
        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(javaClassName);

        String painlessClassName = anyTypeNameToPainlessTypeName(javaClassName);

        if (CLASS_NAME_PATTERN.matcher(painlessClassName).matches() == false) {
            throw new IllegalArgumentException("invalid painless class name [" + painlessClassName + "]");
        }

        String importedPainlessClassName = anyTypeNameToPainlessTypeName(javaClassName.substring(javaClassName.lastIndexOf('.') + 1));

        Class<?> javaClass;

        if ("void".equals(javaClassName)) javaClass = void.class;
        else if ("boolean".equals(javaClassName)) javaClass = boolean.class;
        else if ("byte".equals(javaClassName)) javaClass = byte.class;
        else if ("short".equals(javaClassName)) javaClass = short.class;
        else if ("char".equals(javaClassName)) javaClass = char.class;
        else if ("int".equals(javaClassName)) javaClass = int.class;
        else if ("long".equals(javaClassName)) javaClass = long.class;
        else if ("float".equals(javaClassName)) javaClass = float.class;
        else if ("double".equals(javaClassName)) javaClass = double.class;
        else {
            try {
                javaClass = Class.forName(javaClassName, true, classLoader);

                if (javaClass == def.class) {
                    throw new IllegalArgumentException(
                            "cannot add reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
                }

                if (javaClass.isArray()) {
                    throw new IllegalArgumentException(
                        "cannot specify an array type java class [" + javaClassName + "] as a painless class");
                }
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException("java class [" + javaClassName + "] not found", cnfe);
            }
        }

        addPainlessClass(painlessClassName, importedPainlessClassName, javaClass, importPainlessClassName);
    }

    public void addPainlessClass(Class<?> javaClass, boolean importPainlessClassName) {
        Objects.requireNonNull(javaClass);

        if (javaClass == def.class) {
            throw new IllegalArgumentException("cannot specify reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

        String javaClassName = javaClass.getCanonicalName();
        String painlessClassName = anyTypeNameToPainlessTypeName(javaClassName);
        String importedPainlessClassName = anyTypeNameToPainlessTypeName(javaClassName.substring(javaClassName.lastIndexOf('.') + 1));

        addPainlessClass(painlessClassName, importedPainlessClassName, javaClass, importPainlessClassName);
    }

    private void addPainlessClass(
            String painlessClassName, String importedPainlessClassName, Class<?> javaClass, boolean importPainlessClassName) {
        PainlessClassBuilder existingPainlessClassBuilder = javaClassesToPainlessClassBuilders.get(javaClass);

        if (existingPainlessClassBuilder == null) {
            PainlessClassBuilder painlessClassBuilder = new PainlessClassBuilder(painlessClassName, javaClass, Type.getType(javaClass));
            painlessClassNamesToJavaClasses.put(painlessClassName, javaClass);
            javaClassesToPainlessClassBuilders.put(javaClass, painlessClassBuilder);
        } else if (existingPainlessClassBuilder.clazz.equals(javaClass) == false) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] illegally represents multiple java classes " +
                    "[" + javaClass.getCanonicalName() + "] and [" + existingPainlessClassBuilder.clazz.getCanonicalName() + "]");
        }

        if (painlessClassName.equals(importedPainlessClassName)) {
            if (importPainlessClassName == true) {
                throw new IllegalArgumentException(
                        "must use only_fqn parameter on painless class [" + painlessClassName + "] with no package");
            }
        } else {
            Class<?> importedJavaClass = painlessClassNamesToJavaClasses.get(importedPainlessClassName);

            if (importedJavaClass == null) {
                if (importPainlessClassName) {
                    if (existingPainlessClassBuilder != null) {
                        throw new IllegalArgumentException(
                                "inconsistent only_fqn parameters found for painless class [" + painlessClassName + "]");
                    }

                    painlessClassNamesToJavaClasses.put(importedPainlessClassName, javaClass);
                }
            } else if (importedJavaClass.equals(javaClass) == false) {
                throw new IllegalArgumentException("painless class [" + importedPainlessClassName + "] illegally represents multiple " +
                        "java classes [" + javaClass.getCanonicalName() + "] and [" + importedJavaClass.getCanonicalName() + "]");
            } else if (importPainlessClassName == false) {
                throw new IllegalArgumentException(
                        "inconsistent only_fqn parameters found for painless class [" + painlessClassName + "]");
            }
        }
    }

    public void addPainlessConstructor(String javaClassName, List<String> anyTypeNameParameters) {
        Objects.requireNonNull(javaClassName);
        Objects.requireNonNull(anyTypeNameParameters);

        String painlessClassName = anyTypeNameToPainlessTypeName(javaClassName);

        if (DEF_PAINLESS_CLASS_NAME.equals(painlessClassName)) {
            throw new IllegalArgumentException("cannot add constructor to reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

        Class<?> javaClass = painlessClassNamesToJavaClasses.get(painlessClassName);

        if (javaClass == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        PainlessClassBuilder painlessClassBuilder = javaClassesToPainlessClassBuilders.get(javaClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        int parametersSize = anyTypeNameParameters.size();
        List<String> painlessTypeNameParameters = new ArrayList<>(parametersSize);

        for (String anyTypeNameParameter : anyTypeNameParameters) {
            String painlessTypeNameParameter = anyTypeNameToPainlessTypeName(anyTypeNameParameter);
            painlessTypeNameParameters.add(painlessTypeNameParameter);
        }

        List<Class<?>> painlessTypeParameters = new ArrayList<>(parametersSize);
        Class<?>[] javaTypeParameters = new Class<?>[parametersSize];

        for (int parameterIndex = 0; parameterIndex < parametersSize; ++parameterIndex) {
            String painlessTypeNameParameter = painlessTypeNameParameters.get(parameterIndex);

            try {
                Class<?> painlessTypeParameter = painlessTypeNameToPainlessType(painlessTypeNameParameter);

                painlessTypeParameters.add(painlessTypeParameter);
                javaTypeParameters[parameterIndex] = painlessDefTypeToJavaObjectType(painlessTypeParameter);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("painless type [" + painlessTypeNameParameter + "] parameter not found " +
                    "for painless constructor [[" + painlessClassName + "], " + painlessTypeNameParameters  + "]", iae);
            }
        }

        addPainlessConstructor(javaClass, painlessClassName, painlessClassBuilder, painlessTypeParameters, javaTypeParameters);
    }

    public void addPainlessConstructor(Class<?> javaClass, List<Class<?>> painlessTypeParameters) {
        Objects.requireNonNull(javaClass);
        Objects.requireNonNull(painlessTypeParameters);

        if (javaClass == def.class) {
            throw new IllegalArgumentException("cannot add constructor to reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

        String javaClassName = javaClass.getCanonicalName();
        String painlessClassName = anyTypeNameToPainlessTypeName(javaClassName);
        PainlessClassBuilder painlessClassBuilder = javaClassesToPainlessClassBuilders.get(javaClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        int parametersSize = painlessTypeParameters.size();
        Class<?>[] javaTypeParameters = new Class<?>[parametersSize];
        int parameterIndex = 0;

        for (Class<?> painlessTypeParameter : painlessTypeParameters) {
            String anyTypeNameParameter = painlessTypeParameter.getCanonicalName();
            String painlessTypeNameParameter = anyTypeNameToPainlessTypeName(anyTypeNameParameter);

            try {
                validatePainlessType(painlessTypeParameter);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(
                        "painless type [" + painlessTypeNameParameter + "] parameter not found for painless constructor " +
                        "[[" + painlessClassName + "], " + anyTypesListToCanonicalString(painlessTypeParameters, true)  + "]", iae);
            }

            javaTypeParameters[parameterIndex++] = painlessDefTypeToJavaObjectType(painlessTypeParameter);
        }

        addPainlessConstructor(javaClass, painlessClassName, painlessClassBuilder, painlessTypeParameters, javaTypeParameters);
    }

    private void addPainlessConstructor(Class<?> javaClass, String painlessClassName, PainlessClassBuilder painlessClassBuilder,
                                        List<Class<?>> painlessTypeParameters, Class<?>[] javaTypeParameters) {
        Constructor<?> javaConstructor;

        try {
            javaConstructor = javaClass.getConstructor(javaTypeParameters);
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException("java constructor [[" + javaClass.getCanonicalName() + "], " +
                    anyTypesArrayToCanonicalString(javaTypeParameters, false) + "] reflection object not found", nsme);
        }

        int parametersSize = painlessTypeParameters.size();
        String painlessMethodKey = buildPainlessMethodKey(CONSTRUCTOR_ANY_NAME, parametersSize);
        PainlessMethod painlessConstructor = painlessClassBuilder.constructors.get(painlessMethodKey);

        if (painlessConstructor == null) {
            Method asmConstructor = Method.getMethod(javaConstructor);
            MethodHandle methodHandle;

            try {
                methodHandle = MethodHandles.publicLookup().in(javaClass).unreflectConstructor(javaConstructor);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException("java constructor [[" + javaClass.getCanonicalName() + "], " +
                        anyTypesArrayToCanonicalString(javaTypeParameters, false) + "] method handle not found");
            }

            painlessConstructor = painlessMethodCache.computeIfAbsent(
                    new PainlessMethodCacheKey(javaClass, CONSTRUCTOR_ANY_NAME, painlessTypeParameters),
                    key -> new PainlessMethod(CONSTRUCTOR_ANY_NAME, javaClass, null, void.class, painlessTypeParameters,
                                              asmConstructor, javaConstructor.getModifiers(), methodHandle)
            );

            painlessClassBuilder.constructors.put(painlessMethodKey, painlessConstructor);
        } else if (painlessConstructor.arguments.equals(painlessTypeParameters) == false){
            throw new IllegalArgumentException(
                    "illegal duplicate constructors for painless class [" + painlessClassName + "] with parameters " +
                    anyTypesListToCanonicalString(painlessTypeParameters, true) + " and " +
                    anyTypesListToCanonicalString(painlessConstructor.arguments, true));
        }
    }

    public void addPainlessMethod(ClassLoader classLoader, String javaClassName, String augmentedJavaClassName, String methodName,
                                  String anyTypeNameReturn, List<String> anyTypeNameParameters) {
        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(javaClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(anyTypeNameReturn);
        Objects.requireNonNull(anyTypeNameParameters);

        String painlessClassName = anyTypeNameToPainlessTypeName(javaClassName);

        if (DEF_PAINLESS_CLASS_NAME.equals(painlessClassName)) {
            throw new IllegalArgumentException("cannot add method to reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

        Class<?> javaClass = painlessClassNamesToJavaClasses.get(painlessClassName);

        if (javaClass == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        PainlessClassBuilder painlessClassBuilder = javaClassesToPainlessClassBuilders.get(javaClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException("invalid method name [" + methodName + "] for painless class [" + painlessClassName + "].");
        }

        int parametersSize = anyTypeNameParameters.size();
        List<String> painlessTypeNameParameters = new ArrayList<>(parametersSize);

        for (String anyTypeNameParameter : anyTypeNameParameters) {
            String painlessTypeNameParameter = anyTypeNameToPainlessTypeName(anyTypeNameParameter);
            painlessTypeNameParameters.add(painlessTypeNameParameter);
        }

        javaClass = painlessDefTypeToJavaObjectType(javaClass);
        Class<?> augmentedJavaClass = null;

        if (augmentedJavaClassName != null) {
            try {
                augmentedJavaClass = Class.forName(augmentedJavaClassName, true, classLoader);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException("augmented java class [" + augmentedJavaClassName + "] not found for painless method " +
                        "[[" + painlessClassName + "], [" + methodName + "], " + painlessTypeNameParameters + "]", cnfe);
            }
        }

        int augmentedParameterOffset = augmentedJavaClass == null ? 0 : 1;

        List<Class<?>> painlessTypeParameters = new ArrayList<>(parametersSize);
        Class<?>[] javaTypeParameters = new Class<?>[parametersSize + augmentedParameterOffset];

        if (augmentedJavaClass != null) {
            javaTypeParameters[0] = javaClass;
        }

        for (int parameterIndex = 0; parameterIndex < parametersSize; ++parameterIndex) {
            String painlessTypeNameParameter = painlessTypeNameParameters.get(parameterIndex);

            try {
                Class<?> painlessTypeParameter = painlessTypeNameToPainlessType(painlessTypeNameParameter);

                painlessTypeParameters.add(painlessTypeParameter);
                javaTypeParameters[parameterIndex + augmentedParameterOffset] = painlessDefTypeToJavaObjectType(painlessTypeParameter);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("painless type [" + painlessTypeNameParameter + "] parameter not found for " +
                        "painless method [[" + painlessClassName + "], [" + methodName + "], " + painlessTypeNameParameters + "]", iae);
            }
        }

        String painlessReturnTypeName = anyTypeNameToPainlessTypeName(anyTypeNameReturn);
        Class<?> painlessTypeReturn;

        try {
            painlessTypeReturn = painlessTypeNameToPainlessType(painlessReturnTypeName);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("painless type [" + painlessReturnTypeName + "] return not found " +
                    "for painless method [[" + painlessClassName + "], [" + methodName + "], " + painlessTypeNameParameters + "]", iae);
        }

        addPainlessMethod(javaClass, augmentedJavaClass, painlessClassName, painlessClassBuilder,
                methodName, painlessTypeReturn, painlessTypeParameters, javaTypeParameters);
    }

    public void addPainlessMethod(Class<?> javaClass, Class<?> augmentedJavaClass, String methodName,
                                  Class<?> painlessTypeReturn, List<Class<?>> painlessTypeParameters) {
        Objects.requireNonNull(javaClass);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(painlessTypeReturn);
        Objects.requireNonNull(painlessTypeParameters);

        if (javaClass == def.class) {
            throw new IllegalArgumentException("cannot add method to reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

        String javaClassName = javaClass.getCanonicalName();
        String painlessClassName = anyTypeNameToPainlessTypeName(javaClassName);
        PainlessClassBuilder painlessClassBuilder = javaClassesToPainlessClassBuilders.get(javaClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException("invalid method name [" + methodName + "] for painless class [" + painlessClassName + "].");
        }

        int parametersSize = painlessTypeParameters.size();
        int augmentedParameterOffset = augmentedJavaClass == null ? 0 : 1;
        Class<?>[] javaTypeParameters = new Class<?>[parametersSize + augmentedParameterOffset];
        int parameterIndex = 0;

        if (augmentedJavaClass != null) {
            javaTypeParameters[0] = javaClass;
        }

        for (Class<?> painlessTypeParameter : painlessTypeParameters) {
            String anyTypeNameParameter = painlessTypeParameter.getCanonicalName();
            String painlessTypeNameParameter = anyTypeNameToPainlessTypeName(anyTypeNameParameter);

            try {
                validatePainlessType(painlessTypeParameter);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(
                        "painless type [" + painlessTypeNameParameter + "] parameter not found for painless constructor " +
                        "[[" + painlessClassName + "], " + anyTypesListToCanonicalString(painlessTypeParameters, true)  + "]", iae);
            }

            javaTypeParameters[parameterIndex + augmentedParameterOffset] = painlessDefTypeToJavaObjectType(painlessTypeParameter);
            ++parameterIndex;
        }

        String anyTypeNameReturn = painlessTypeReturn.getCanonicalName();
        String painlessTypeNameReturn = anyTypeNameToPainlessTypeName(anyTypeNameReturn);

        try {
            validatePainlessType(painlessTypeReturn);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("painless type [" + painlessTypeNameReturn + "] return not found " +
                    "for painless method [[" + painlessClassName + "], [" + methodName + "], " + painlessTypeParameters  + "]", iae);
        }

        addPainlessMethod(javaClass, augmentedJavaClass, painlessClassName, painlessClassBuilder, methodName,
                painlessTypeReturn, painlessTypeParameters, javaTypeParameters);
    }

    private void addPainlessMethod(Class<?> javaClass, Class<?> augmentedJavaClass,
                                   String painlessClassName, PainlessClassBuilder painlessClassBuilder, String methodName,
                                   Class<?> painlessTypeReturn, List<Class<?>> painlessTypeParameters, Class<?>[] javaTypeParameters) {
        java.lang.reflect.Method javaMethod;

        if (augmentedJavaClass == null) {
            try {
                javaMethod = javaClass.getMethod(methodName, javaTypeParameters);
            } catch (NoSuchMethodException nsme) {
                throw new IllegalArgumentException("java method [[" + javaClass.getCanonicalName() + "], [" + methodName + "], " +
                        anyTypesArrayToCanonicalString(javaTypeParameters, false) + "] reflection object not found", nsme);
            }
        } else {
            try {
                javaMethod = augmentedJavaClass.getMethod(methodName, javaTypeParameters);
            } catch (NoSuchMethodException nsme) {
                throw new IllegalArgumentException("java method [[" + augmentedJavaClass.getCanonicalName() + "], [" + methodName + "], " +
                        anyTypesArrayToCanonicalString(javaTypeParameters, false) + "] reflection object not found", nsme);
            }
        }

        if (javaMethod.getReturnType() != painlessDefTypeToJavaObjectType(painlessTypeReturn)) {
            throw new IllegalArgumentException("returned java type [" + javaMethod.getReturnType().getCanonicalName() + "] " +
                    "does not match the specified returned java type [" + painlessTypeReturn.getCanonicalName() + "] " +
                    "for java method [[" + javaClass.getCanonicalName() + "], [" + methodName + "], " +
                    anyTypesArrayToCanonicalString(javaTypeParameters, false) + "]");
        }

        int parametersSize = painlessTypeParameters.size();
        String painlessMethodKey = buildPainlessMethodKey(methodName, parametersSize);

        if (augmentedJavaClass == null && Modifier.isStatic(javaMethod.getModifiers())) {
            PainlessMethod painlessMethod = painlessClassBuilder.staticMethods.get(painlessMethodKey);

            if (painlessMethod == null) {
                Method asmMethod = Method.getMethod(javaMethod);
                MethodHandle javaMethodHandle;

                try {
                    javaMethodHandle = MethodHandles.publicLookup().in(javaClass).unreflect(javaMethod);
                } catch (IllegalAccessException iae) {
                    throw new IllegalArgumentException(
                            "static java method [[" + javaClass.getCanonicalName() + "], [" + methodName + "], " +
                            anyTypesArrayToCanonicalString(javaTypeParameters, false) + "] reflection object not found", iae);
                }

                painlessMethod = painlessMethodCache.computeIfAbsent(
                        new PainlessMethodCacheKey(javaClass, methodName, painlessTypeParameters),
                        key -> new PainlessMethod(methodName, javaClass, null, painlessTypeReturn,
                                                  painlessTypeParameters, asmMethod, javaMethod.getModifiers(), javaMethodHandle));

                painlessClassBuilder.staticMethods.put(painlessMethodKey, painlessMethod);
            } else if ((painlessMethod.name.equals(methodName) && painlessMethod.rtn == painlessTypeReturn &&
                painlessMethod.arguments.equals(painlessTypeParameters)) == false) {
                throw new IllegalArgumentException("illegal duplicate static painless methods [" + painlessMethodKey + "] found " +
                        "[[" + painlessClassName + "], [" + methodName + "], " +
                        anyTypesListToCanonicalString(painlessTypeParameters, true) + "] and " +
                        "[[" + painlessClassName + "], [" + painlessMethod.name + "], " +
                        anyTypesListToCanonicalString(painlessMethod.arguments, true) + "]");
            }
        } else {
            PainlessMethod painlessMethod = painlessClassBuilder.staticMethods.get(painlessMethodKey);

            if (painlessMethod == null) {
                Method asmMethod = Method.getMethod(javaMethod);
                MethodHandle javaMethodHandle;

                try {
                    if (augmentedJavaClass == null) {
                        javaMethodHandle = MethodHandles.publicLookup().in(javaClass).unreflect(javaMethod);
                    } else {
                        javaMethodHandle = MethodHandles.publicLookup().in(augmentedJavaClass).unreflect(javaMethod);
                    }
                } catch (IllegalAccessException iae) {
                    throw new IllegalArgumentException("java method [[" + javaClass.getCanonicalName() + "], [" + methodName + "], " +
                            anyTypesArrayToCanonicalString(javaTypeParameters, false) + "] reflection object not found", iae);
                }

                painlessMethod = painlessMethodCache.computeIfAbsent(
                        new PainlessMethodCacheKey(javaClass, methodName, painlessTypeParameters),
                        key -> new PainlessMethod(methodName, javaClass, augmentedJavaClass, painlessTypeReturn,
                                                  painlessTypeParameters, asmMethod, javaMethod.getModifiers(), javaMethodHandle));

                painlessClassBuilder.methods.put(painlessMethodKey, painlessMethod);
            } else if ((painlessMethod.name.equals(methodName) && painlessMethod.rtn == painlessTypeReturn &&
                painlessMethod.arguments.equals(painlessTypeParameters)) == false) {
                throw new IllegalArgumentException("illegal duplicate painless methods [" + painlessMethodKey + "] found " +
                        "[[" + painlessClassName + "], [" + methodName + "], " +
                        anyTypesListToCanonicalString(painlessTypeParameters, true) + "] and " +
                        "[[" + painlessClassName + "], [" + painlessMethod.name + "], " +
                        anyTypesListToCanonicalString(painlessMethod.arguments, true) + "]");
            }
        }
    }

    public void addPainlessField(String javaClassName, String fieldName, String anyTypeNameField) {
        Objects.requireNonNull(javaClassName);
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(anyTypeNameField);

        String painlessClassName = anyTypeNameToPainlessTypeName(javaClassName);

        if (DEF_PAINLESS_CLASS_NAME.equals(painlessClassName)) {
            throw new IllegalArgumentException("cannot add field to reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

        Class<?> javaClass = painlessClassNamesToJavaClasses.get(painlessClassName);

        if (javaClass == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        PainlessClassBuilder painlessClassBuilder = javaClassesToPainlessClassBuilders.get(javaClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        if (FIELD_NAME_PATTERN.matcher(fieldName).matches() == false) {
            throw new IllegalArgumentException("invalid field name [" + fieldName + "] for painless class [" + painlessClassName + "].");
        }

        String painlessTypeNameField = anyTypeNameToPainlessTypeName(anyTypeNameField);
        Class<?> painlessTypeField = painlessTypeNameToPainlessType(painlessTypeNameField);

        if (painlessTypeField == null) {
            throw new IllegalArgumentException("painless type [" + painlessTypeNameField + "] field not found " +
                    "for painless field [[" + painlessClassName + "], [" + fieldName + "]");
        }

        addPainlessField(javaClass, painlessClassName, painlessClassBuilder, fieldName, painlessTypeField);
    }

    public void addPainlessField(Class<?> javaClass, String fieldName, Class<?> painlessTypeField) {
        Objects.requireNonNull(javaClass);
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(painlessTypeField);

        if (javaClass == def.class) {
            throw new IllegalArgumentException("cannot add field to reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

        String javaClassName = javaClass.getCanonicalName();
        String painlessClassName = anyTypeNameToPainlessTypeName(javaClassName);
        PainlessClassBuilder painlessClassBuilder = javaClassesToPainlessClassBuilders.get(javaClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        if (FIELD_NAME_PATTERN.matcher(fieldName).matches() == false) {
            throw new IllegalArgumentException("invalid field name [" + fieldName + "] for painless class [" + painlessClassName + "].");
        }

        String anyTypeNameField = painlessTypeField.getCanonicalName();
        String painlessTypeNameField = anyTypeNameToPainlessTypeName(anyTypeNameField);

        try {
            validatePainlessType(painlessTypeField);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("painless type [" + painlessTypeNameField + "] field not found " +
                "for painless field [[" + painlessClassName + "], [" + fieldName + "]", iae);
        }

        addPainlessField(javaClass, painlessClassName, painlessClassBuilder, fieldName, painlessTypeField);
    }

    private void addPainlessField(Class<?> javaClass, String painlessClassName,
                                  PainlessClassBuilder painlessClassBuilder, String fieldName, Class<?> painlessTypeField) {
        Field javaField;

        try {
            javaField = javaClass.getField(fieldName);
        } catch (NoSuchFieldException nsme) {
            throw new IllegalArgumentException(
                    "java field [[" + javaClass.getCanonicalName() + "], [" + fieldName + "], reflection object not found", nsme);
        }

        String anyTypeNameField = painlessTypeField.getCanonicalName();
        String painlessFieldKey = buildPainlessFieldKey(fieldName);
        String painlessTypeNameField = anyTypeNameToPainlessTypeName(anyTypeNameField);

        if (javaField.getType() != painlessDefTypeToJavaObjectType(painlessTypeField)) {
            throw new IllegalArgumentException("java type field [" + javaField.getType().getCanonicalName() + "] " +
                    "does not match the specified java type field [" + painlessTypeField.getCanonicalName() + "] " +
                    "for java field [[" + javaClass.getCanonicalName() + "], [" + fieldName + "]]");
        }

        if (Modifier.isStatic(javaField.getModifiers())) {
            if (Modifier.isFinal(javaField.getModifiers()) == false) {
                throw new IllegalArgumentException(
                        "static painless field [[" + painlessClassName + "]. [" + fieldName + "]] must be final");
            }

            PainlessField painlessField = painlessClassBuilder.staticMembers.get(painlessFieldKey);

            if (painlessField == null) {
                painlessField = painlessFieldCache.computeIfAbsent(
                        new PainlessFieldCacheKey(javaClass, fieldName, painlessTypeField),
                        key -> new PainlessField(fieldName, javaField.getName(), javaClass,
                                                 painlessTypeField, javaField.getModifiers(), null, null));

                painlessClassBuilder.staticMembers.put(painlessFieldKey, painlessField);
            } else if (painlessField.clazz != painlessTypeField) {
                throw new IllegalArgumentException("illegal duplicate static painless fields [" + fieldName + "] found " +
                        "[[" + painlessClassName + "], [" + fieldName + "], [" + painlessTypeField.getCanonicalName() + "] and " +
                        "[[" + painlessClassName + "], [" + painlessField.name + "], " + painlessField.javaName + "]");
            }
        } else {
            MethodHandle methodHandleGetter;

            try {
                methodHandleGetter = MethodHandles.publicLookup().unreflectGetter(javaField);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException(
                        "method handle getter not found for painless field [[" + painlessClassName + "], [" + fieldName + "]]");
            }

            MethodHandle methodHandleSetter;

            try {
                methodHandleSetter = MethodHandles.publicLookup().unreflectSetter(javaField);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException(
                        "method handle setter not found for painless field [[" + painlessClassName + "], [" + fieldName + "]]");
            }

            PainlessField painlessField = painlessClassBuilder.members.get(painlessFieldKey);

            if (painlessField == null) {
                painlessField = painlessFieldCache.computeIfAbsent(
                        new PainlessFieldCacheKey(javaClass, painlessFieldKey, painlessTypeField),
                        key -> new PainlessField(fieldName, javaField.getName(), javaClass,
                                                 painlessTypeField, javaField.getModifiers(), methodHandleGetter, methodHandleSetter));

                painlessClassBuilder.members.put(fieldName, painlessField);
            } else if (painlessField.clazz != painlessTypeField) {
                throw new IllegalArgumentException("illegal duplicate painless fields [" + fieldName + "] found " +
                        "[[" + painlessClassName + "], [" + fieldName + "], [" + painlessTypeNameField + "] and " +
                        "[[" + painlessClassName + "], [" + painlessField.name + "], " + painlessField.javaName + "]");
            }
        }
    }

    public PainlessLookup build() {
        copyPainlessClassMembers();
        cacheRuntimeHandles();
        setFunctionalInterfaceMethods();

        Map<Class<?>, PainlessClass> javaClassesToPainlessClasses = new HashMap<>(javaClassesToPainlessClassBuilders.size());

        for (Map.Entry<Class<?>, PainlessClassBuilder> painlessClassBuilderEntry : javaClassesToPainlessClassBuilders.entrySet()) {
            javaClassesToPainlessClasses.put(painlessClassBuilderEntry.getKey(), painlessClassBuilderEntry.getValue().build());
        }

        return new PainlessLookup(painlessClassNamesToJavaClasses, javaClassesToPainlessClasses);
    }

    private void copyPainlessClassMembers() {
        for (Class<?> parentJavaClass : javaClassesToPainlessClassBuilders.keySet()) {
            copyPainlessInterfaceMembers(parentJavaClass, parentJavaClass);

            Class<?> childJavaClass = parentJavaClass.getSuperclass();

            while (childJavaClass != null) {
                if (javaClassesToPainlessClassBuilders.containsKey(childJavaClass)) {
                    copyPainlessClassMembers(childJavaClass, parentJavaClass);
                }

                copyPainlessInterfaceMembers(childJavaClass, parentJavaClass);
                childJavaClass = childJavaClass.getSuperclass();
            }
        }

        for (Class<?> javaClass : javaClassesToPainlessClassBuilders.keySet()) {
            if (javaClass.isInterface()) {
                copyPainlessClassMembers(Object.class, javaClass);
            }
        }
    }

    private void copyPainlessInterfaceMembers(Class<?> parentJavaClass, Class<?> targetJavaClass) {
        for (Class<?> childJavaClass : parentJavaClass.getInterfaces()) {
            if (javaClassesToPainlessClassBuilders.containsKey(childJavaClass)) {
                copyPainlessClassMembers(childJavaClass, targetJavaClass);
            }

            copyPainlessInterfaceMembers(childJavaClass, targetJavaClass);
        }
    }

    private void copyPainlessClassMembers(Class<?> originalJavaClass, Class<?> targetJavaClass) {
        PainlessClassBuilder originalPainlessClassBuilder = javaClassesToPainlessClassBuilders.get(originalJavaClass);
        PainlessClassBuilder targetPainlessClassBuilder = javaClassesToPainlessClassBuilders.get(targetJavaClass);

        Objects.requireNonNull(originalPainlessClassBuilder);
        Objects.requireNonNull(targetPainlessClassBuilder);

        for (Map.Entry<String, PainlessMethod> painlessMethodEntry : originalPainlessClassBuilder.methods.entrySet()) {
            String painlessMethodKey = painlessMethodEntry.getKey();
            PainlessMethod newPainlessMethod = painlessMethodEntry.getValue();
            PainlessMethod existingPainlessMethod = targetPainlessClassBuilder.methods.get(painlessMethodKey);

            if (existingPainlessMethod == null || existingPainlessMethod.target != newPainlessMethod.target &&
                    existingPainlessMethod.target.isAssignableFrom(newPainlessMethod.target)) {
                targetPainlessClassBuilder.methods.put(painlessMethodKey, newPainlessMethod);
            }
        }

        for (Map.Entry<String, PainlessField> painlessFieldEntry : originalPainlessClassBuilder.members.entrySet()) {
            String painlessFieldKey = painlessFieldEntry.getKey();
            PainlessField newPainlessField = painlessFieldEntry.getValue();
            PainlessField existingPainlessField = targetPainlessClassBuilder.members.get(painlessFieldKey);

            if (existingPainlessField == null || existingPainlessField.target != newPainlessField.target &&
                    existingPainlessField.target.isAssignableFrom(newPainlessField.target)) {
                targetPainlessClassBuilder.members.put(painlessFieldKey, newPainlessField);
            }
        }
    }

    private void cacheRuntimeHandles() {
        for (PainlessClassBuilder painlessClassBuilder : javaClassesToPainlessClassBuilders.values()) {
            cacheRuntimeHandles(painlessClassBuilder);
        }
    }

    private void cacheRuntimeHandles(PainlessClassBuilder painlessClassBuilder) {
        for (PainlessMethod painlessMethod : painlessClassBuilder.methods.values()) {
            String methodName = painlessMethod.name;
            int parametersSize = painlessMethod.arguments.size();

            if (parametersSize == 0 && methodName.startsWith("get") && methodName.length() > 3 &&
                    Character.isUpperCase(methodName.charAt(3))) {
                painlessClassBuilder.getters.putIfAbsent(
                        Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4), painlessMethod.handle);
            } else if (parametersSize == 0 && methodName.startsWith("is") && methodName.length() > 2 &&
                    Character.isUpperCase(methodName.charAt(2))) {
                painlessClassBuilder.getters.putIfAbsent(
                    Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3), painlessMethod.handle);
            } else if (parametersSize == 1 && methodName.startsWith("set") && methodName.length() > 3 &&
                Character.isUpperCase(methodName.charAt(3))) {
                painlessClassBuilder.setters.putIfAbsent(
                    Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4), painlessMethod.handle);
            }
        }

        for (PainlessField painlessField : painlessClassBuilder.members.values()) {
            painlessClassBuilder.getters.put(painlessField.name, painlessField.getter);
            painlessClassBuilder.setters.put(painlessField.name, painlessField.setter);
        }
    }

    private void setFunctionalInterfaceMethods() {
        for (Map.Entry<Class<?>, PainlessClassBuilder> painlessClassBuilderEntry : javaClassesToPainlessClassBuilders.entrySet()) {
            setFunctionalInterfaceMethod(painlessClassBuilderEntry.getValue());
        }
    }

    private void setFunctionalInterfaceMethod(PainlessClassBuilder painlessClassBuilder) {
        Class<?> javaClass = painlessClassBuilder.clazz;

        if (javaClass.isInterface()) {
            List<java.lang.reflect.Method> javaMethods = new ArrayList<>();

            for (java.lang.reflect.Method javaMethod : javaClass.getMethods()) {
                if (javaMethod.isDefault() == false && Modifier.isStatic(javaMethod.getModifiers()) == false) {
                    try {
                        Object.class.getMethod(javaMethod.getName(), javaMethod.getParameterTypes());
                    } catch (ReflectiveOperationException roe) {
                        javaMethods.add(javaMethod);
                    }
                }
            }

            if (javaMethods.size() != 1 && javaClass.isAnnotationPresent(FunctionalInterface.class)) {
                throw new IllegalArgumentException("java class [" + javaClass.getCanonicalName() + "] " +
                    "is illegally marked as a FunctionalInterface with java methods " + javaMethods);
            } else if (javaMethods.size() == 1) {
                java.lang.reflect.Method javaMethod = javaMethods.get(0);
                String painlessMethodKey = buildPainlessMethodKey(javaMethod.getName(), javaMethod.getParameterCount());
                painlessClassBuilder.functionalMethod = painlessClassBuilder.methods.get(painlessMethodKey);
            }
        }
    }
}
