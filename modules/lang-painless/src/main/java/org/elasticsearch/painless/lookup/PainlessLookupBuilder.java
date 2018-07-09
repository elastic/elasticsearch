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
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.regex.Pattern;

public final class PainlessLookupBuilder extends PainlessLookupBase {

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

    public PainlessLookupBuilder() {
        super();

        painlessClassNamesToJavaClasses.put(DEF_PAINLESS_CLASS_NAME, def.class);
        javaClassesToPainlessClasses.put(def.class, new PainlessClass(DEF_PAINLESS_CLASS_NAME, Object.class, Type.getType(Object.class)));
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
                    throw new IllegalArgumentException("cannot add reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
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
        PainlessClass existingPainlessClass = javaClassesToPainlessClasses.get(javaClass);

        if (existingPainlessClass == null) {
            PainlessClass painlessClass = new PainlessClass(painlessClassName, javaClass, Type.getType(javaClass));
            painlessClassNamesToJavaClasses.put(painlessClassName, javaClass);
            javaClassesToPainlessClasses.put(javaClass, painlessClass);
        } else if (existingPainlessClass.clazz.equals(javaClass) == false) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] illegally represents multiple java classes " +
                    "[" + javaClass.getCanonicalName() + "] and [" + existingPainlessClass.clazz.getCanonicalName() + "]");
        }

        if (painlessClassName.equals(importedPainlessClassName)) {
            if (importPainlessClassName == false) {
                throw new IllegalArgumentException(
                        "must use only_fqn parameter on painless class [" + painlessClassName + "] with no package");
            }
        } else {
            Class<?> importedJavaClass = painlessClassNamesToJavaClasses.get(importedPainlessClassName);

            if (importedJavaClass == null) {
                if (importPainlessClassName) {
                    if (existingPainlessClass != null) {
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

        PainlessClass painlessClass = javaClassesToPainlessClasses.get(javaClass);

        if (painlessClass == null) {
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

        addPainlessConstructor(javaClass, painlessClassName, painlessClass, painlessTypeParameters, javaTypeParameters);
    }

    public void addPainlessConstructor(Class<?> javaClass, List<Class<?>> painlessTypeParameters) {
        Objects.requireNonNull(javaClass);
        Objects.requireNonNull(painlessTypeParameters);

        if (javaClass == def.class) {
            throw new IllegalArgumentException("cannot add constructor to reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

        String javaClassName = javaClass.getCanonicalName();
        String painlessClassName = anyTypeNameToPainlessTypeName(javaClassName);
        PainlessClass painlessClass = javaClassesToPainlessClasses.get(javaClass);

        if (painlessClass == null) {
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

        addPainlessConstructor(javaClass, painlessClassName, painlessClass, painlessTypeParameters, javaTypeParameters);
    }

    private void addPainlessConstructor(Class<?> javaClass, String painlessClassName, PainlessClass painlessClass,
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
        PainlessMethod painlessConstructor = painlessClass.constructors.get(painlessMethodKey);

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
                    key -> new PainlessMethod(CONSTRUCTOR_ANY_NAME, painlessClass, null, void.class, painlessTypeParameters,
                                              asmConstructor, javaConstructor.getModifiers(), methodHandle)
            );

            painlessClass.constructors.put(painlessMethodKey, painlessConstructor);
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

        PainlessClass painlessClass = javaClassesToPainlessClasses.get(javaClass);

        if (painlessClass == null) {
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

        addPainlessMethod(javaClass, augmentedJavaClass, painlessClassName, painlessClass,
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
        PainlessClass painlessClass = javaClassesToPainlessClasses.get(javaClass);

        if (painlessClass == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException("invalid method name [" + methodName + "] for painless class [" + painlessClassName + "].");
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

        String anyTypeNameReturn = painlessTypeReturn.getCanonicalName();
        String painlessTypeNameReturn = anyTypeNameToPainlessTypeName(anyTypeNameReturn);

        try {
            validatePainlessType(painlessTypeReturn);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("painless type [" + painlessTypeNameReturn + "] return not found " +
                    "for painless method [[" + painlessClassName + "], [" + methodName + "], " + painlessTypeParameters  + "]", iae);
        }

        addPainlessMethod(javaClass, augmentedJavaClass, painlessClassName, painlessClass, methodName,
                painlessTypeReturn, painlessTypeParameters, javaTypeParameters);
    }

    private void addPainlessMethod(Class<?> javaClass, Class<?> augmentedJavaClass,
                                   String painlessClassName, PainlessClass painlessClass, String methodName,
                                   Class<?> painlessTypeReturn, List<Class<?>> painlessTypeParameters, Class<?>[] javaTypeParameters) {
        java.lang.reflect.Method javaMethod;

        try {
            javaMethod = javaClass.getMethod(methodName, javaTypeParameters);
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException("java method [[" + javaClass.getCanonicalName() + "], [" + methodName + "], " +
                    anyTypesArrayToCanonicalString(javaTypeParameters, false) + "] reflection object not found", nsme);
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
            PainlessMethod painlessMethod = painlessClass.staticMethods.get(painlessMethodKey);

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
                        key -> new PainlessMethod(methodName, painlessClass, null, painlessTypeReturn,
                                                  painlessTypeParameters, asmMethod, javaMethod.getModifiers(), javaMethodHandle));

                painlessClass.staticMethods.put(painlessMethodKey, painlessMethod);
            } else if ((painlessMethod.name.equals(methodName) && painlessMethod.rtn == painlessTypeReturn &&
                painlessMethod.arguments.equals(painlessTypeParameters)) == false) {
                throw new IllegalArgumentException("illegal duplicate static painless methods [" + painlessMethodKey + "] found " +
                        "[[" + painlessClassName + "], [" + methodName + "], " +
                        anyTypesListToCanonicalString(painlessTypeParameters, true) + "] and " +
                        "[[" + painlessClassName + "], [" + painlessMethod.name + "], " +
                        anyTypesListToCanonicalString(painlessMethod.arguments, true) + "]");
            }
        } else {
            PainlessMethod painlessMethod = painlessClass.staticMethods.get(painlessMethodKey);

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
                        key -> new PainlessMethod(methodName, painlessClass, augmentedJavaClass, painlessTypeReturn,
                                                  painlessTypeParameters, asmMethod, javaMethod.getModifiers(), javaMethodHandle));

                painlessClass.methods.put(painlessMethodKey, painlessMethod);
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

    private void addPainlessField(String javaClassName, String fieldName, String anyTypeNameField) {
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

        PainlessClass painlessClass = javaClassesToPainlessClasses.get(javaClass);

        if (painlessClass == null) {
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

        addPainlessField(javaClass, painlessClassName, painlessClass, fieldName, painlessTypeField);
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
        PainlessClass painlessClass = javaClassesToPainlessClasses.get(javaClass);

        if (painlessClass == null) {
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

        addPainlessField(javaClass, painlessClassName, painlessClass, fieldName, painlessTypeField);
    }

    private void addPainlessField(
            Class<?> javaClass, String painlessClassName, PainlessClass painlessClass, String fieldName, Class<?> painlessTypeField) {
        Field javaField;

        try {
            javaField = javaClass.getField(fieldName);
        } catch (NoSuchFieldException nsme) {
            throw new IllegalArgumentException(
                    "java field [[" + javaClass.getCanonicalName() + "], [" + fieldName + "], reflection object not found", nsme);
        }

        String anyTypeNameField = painlessTypeField.getCanonicalName();
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

            PainlessField painlessField = painlessClass.staticMembers.get(fieldName);

            if (painlessField == null) {
                painlessField = painlessFieldCache.computeIfAbsent(
                        new PainlessFieldCacheKey(javaClass, fieldName, painlessTypeField),
                        key -> new PainlessField(fieldName, javaField.getName(), painlessClass,
                                                 painlessTypeField, javaField.getModifiers(), null, null));

                painlessClass.staticMembers.put(fieldName, painlessField);
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

            PainlessField painlessField = painlessClass.members.get(fieldName);

            if (painlessField == null) {
                painlessField = painlessFieldCache.computeIfAbsent(
                        new PainlessFieldCacheKey(javaClass, fieldName, painlessTypeField),
                        key -> new PainlessField(fieldName, javaField.getName(), painlessClass,
                                                 painlessTypeField, javaField.getModifiers(), methodHandleGetter, methodHandleSetter));

                painlessClass.staticMembers.put(fieldName, painlessField);
            } else if (painlessField.clazz != painlessTypeField) {
                throw new IllegalArgumentException("illegal duplicate painless fields [" + fieldName + "] found " +
                        "[[" + painlessClassName + "], [" + fieldName + "], [" + painlessTypeNameField + "] and " +
                        "[[" + painlessClassName + "], [" + painlessField.name + "], " + painlessField.javaName + "]");
            }
        }
    }

    public PainlessLookup build() {
        copyChildClassesMembersToParentClasses();

        /*String origin = null;

        try {
            // first iteration collects all the Painless type names that
            // are used for validation during the second iteration
            for (Whitelist whitelist : whitelists) {
                for (WhitelistClass whitelistStruct : whitelist.whitelistStructs) {
                    String painlessTypeName = whitelistStruct.javaClassName.replace('$', '.');
                    PainlessClass painlessStruct = javaClassesToPainlessStructs.get(painlessTypesToJavaClasses.get(painlessTypeName));

                    if (painlessStruct != null && painlessStruct.clazz.getName().equals(whitelistStruct.javaClassName) == false) {
                        throw new IllegalArgumentException("struct [" + painlessStruct.name + "] cannot represent multiple classes " +
                            "[" + painlessStruct.clazz.getName() + "] and [" + whitelistStruct.javaClassName + "]");
                    }

                    origin = whitelistStruct.origin;
                    addStruct(whitelist.javaClassLoader, whitelistStruct);

                    painlessStruct = javaClassesToPainlessStructs.get(painlessTypesToJavaClasses.get(painlessTypeName));
                    javaClassesToPainlessStructs.put(painlessStruct.clazz, painlessStruct);
                }
            }

            // second iteration adds all the constructors, methods, and fields that will
            // be available in Painless along with validating they exist and all their types have
            // been white-listed during the first iteration
            for (Whitelist whitelist : whitelists) {
                for (WhitelistClass whitelistStruct : whitelist.whitelistStructs) {
                    String painlessTypeName = whitelistStruct.javaClassName.replace('$', '.');

                    for (WhitelistConstructor whitelistConstructor : whitelistStruct.whitelistConstructors) {
                        origin = whitelistConstructor.origin;
                        addConstructor(painlessTypeName, whitelistConstructor);
                    }

                    for (WhitelistMethod whitelistMethod : whitelistStruct.whitelistMethods) {
                        origin = whitelistMethod.origin;
                        addMethod(whitelist.javaClassLoader, painlessTypeName, whitelistMethod);
                    }

                    for (WhitelistField whitelistField : whitelistStruct.whitelistFields) {
                        origin = whitelistField.origin;
                        addField(painlessTypeName, whitelistField);
                    }
                }
            }
        } catch (Exception exception) {
            throw new IllegalArgumentException("error loading whitelist(s) " + origin, exception);
        }*/

        // precompute runtime classes
        for (PainlessClass painlessStruct : javaClassesToPainlessStructs.values()) {
            addRuntimeClass(painlessStruct);
        }

        // copy all structs to make them unmodifiable for outside users:
        for (Map.Entry<Class<?>,PainlessClass> entry : javaClassesToPainlessStructs.entrySet()) {
            entry.setValue(entry.getValue().freeze(computeFunctionalInterfaceMethod(entry.getValue())));
        }
    }

    private void copyChildClassesMembersToParentClasses() {
        for (Class<?> parentJavaClass : javaClassesToPainlessClasses.keySet()) {

            List<String> painlessSuperStructs = new ArrayList<>();
            Class<?> javaSuperClass = painlessStruct.clazz.getSuperclass();

            Stack<Class<?>> javaInteraceLookups = new Stack<>();
            javaInteraceLookups.push(painlessStruct.clazz);

            // adds super classes to the inheritance list
            if (javaSuperClass != null && javaSuperClass.isInterface() == false) {
                while (javaSuperClass != null) {
                    PainlessClass painlessSuperStruct = javaClassesToPainlessStructs.get(javaSuperClass);

                    if (painlessSuperStruct != null) {
                        painlessSuperStructs.add(painlessSuperStruct.name);
                    }

                    javaInteraceLookups.push(javaSuperClass);
                    javaSuperClass = javaSuperClass.getSuperclass();
                }
            }

            // adds all super interfaces to the inheritance list
            while (javaInteraceLookups.isEmpty() == false) {
                Class<?> javaInterfaceLookup = javaInteraceLookups.pop();

                for (Class<?> javaSuperInterface : javaInterfaceLookup.getInterfaces()) {
                    PainlessClass painlessInterfaceStruct = javaClassesToPainlessStructs.get(javaSuperInterface);

                    if (painlessInterfaceStruct != null) {
                        String painlessInterfaceStructName = painlessInterfaceStruct.name;

                        if (painlessSuperStructs.contains(painlessInterfaceStructName) == false) {
                            painlessSuperStructs.add(painlessInterfaceStructName);
                        }

                        for (Class<?> javaPushInterface : javaInterfaceLookup.getInterfaces()) {
                            javaInteraceLookups.push(javaPushInterface);
                        }
                    }
                }
            }

            // copies methods and fields from super structs to the parent struct
            copyStruct(painlessStruct.name, painlessSuperStructs);

            // copies methods and fields from Object into interface types
            if (painlessStruct.clazz.isInterface() || (def.class.getSimpleName()).equals(painlessStruct.name)) {
                PainlessClass painlessObjectStruct = javaClassesToPainlessStructs.get(Object.class);

                if (painlessObjectStruct != null) {
                    copyStruct(painlessStruct.name, Collections.singletonList(painlessObjectStruct.name));
                }
            }
        }
    }

    private void copyChildClassMembersToParentClass(Class<?> parentJavaClass, Class<?> childJavaClass) {
        PainlessClass parentPainlessClass = javaClassesToPainlessClasses.get(parentJavaClass);
        PainlessClass childPainlessClass  = javaClassesToPainlessClasses.get(childJavaClass);

        Objects.requireNonNull(parentPainlessClass);
        Objects.requireNonNull(childPainlessClass);

            for (Map.Entry<String, PainlessMethod> painlessMethodEntry : childPainlessClass.methods.entrySet()) {
                String painlessMethodKey = painlessMethodEntry.getKey();
                PainlessMethod painlessMethod = painlessMethodEntry.getValue();

                parentPainlessClass.methods.putIfAbsent(painlessMethodKey, )
                if (parentPainlessClass.methods.get(painlessMethodKey) == null) {
                    // sanity check, look for missing covariant/generic override
                    if (parentJavaClass.isInterface() && childJavaClass == Object.class) {
                        // ok
                    } else if (childJavaClass == Spliterator.OfPrimitive.class || childJavaClass == PrimitiveIterator.class) {
                        // ok, we rely on generics erasure for these (its guaranteed in the javadocs though!!!!)
                    } else if (Constants.JRE_IS_MINIMUM_JAVA9 && owner.clazz == LocalDate.class) {
                        // ok, java 9 added covariant override for LocalDate.getEra() to return IsoEra:
                        // https://bugs.openjdk.java.net/browse/JDK-8072746
                    } else {
                        try {
                            // TODO: we *have* to remove all these public members and use getter methods to encapsulate!
                            final Class<?> impl;
                            final Class<?> arguments[];
                            if (method.augmentation != null) {
                                impl = method.augmentation;
                                arguments = new Class<?>[method.arguments.size() + 1];
                                arguments[0] = method.owner.clazz;
                                for (int i = 0; i < method.arguments.size(); i++) {
                                    arguments[i + 1] = method.arguments.get(i).clazz;
                                }
                            } else {
                                impl = owner.clazz;
                                arguments = new Class<?>[method.arguments.size()];
                                for (int i = 0; i < method.arguments.size(); i++) {
                                    arguments[i] = method.arguments.get(i).clazz;
                                }
                            }
                            java.lang.reflect.Method m = impl.getMethod(method.method.getName(), arguments);
                            if (m.getReturnType() != method.rtn.clazz) {
                                throw new IllegalStateException("missing covariant override for: " + m + " in " + owner.name);
                            }
                            if (m.isBridge() && !Modifier.isVolatile(method.modifiers)) {
                                // its a bridge in the destination, but not in the source, but it might still be ok, check generics:
                                java.lang.reflect.Method source = child.clazz.getMethod(method.method.getName(), arguments);
                                if (!Arrays.equals(source.getGenericParameterTypes(), source.getParameterTypes())) {
                                    throw new IllegalStateException("missing generic override for: " + m + " in " + owner.name);
                                }
                            }
                        } catch (ReflectiveOperationException e) {
                            throw new AssertionError(e);
                        }
                    }*/
                    parentPainlessClass.methods.put(painlessMethodKey, painlessMethod);
                }
            }

            for (PainlessField field : pain.members.values()) {
                if (owner.members.get(field.name) == null) {
                    owner.members.put(field.name,
                        new PainlessField(field.name, field.javaName, owner, field.clazz, field.modifiers, field.getter, field.setter));
                }
            }
    }

    /**
     * Precomputes a more efficient structure for dynamic method/field access.
     */
    private void addRuntimeClass(final PainlessClass struct) {
        // add all getters/setters
        for (Map.Entry<PainlessMethodKey, PainlessMethod> method : struct.methods.entrySet()) {
            String name = method.getKey().name;
            PainlessMethod m = method.getValue();

            if (m.arguments.size() == 0 &&
                name.startsWith("get") &&
                name.length() > 3 &&
                Character.isUpperCase(name.charAt(3))) {
                StringBuilder newName = new StringBuilder();
                newName.append(Character.toLowerCase(name.charAt(3)));
                newName.append(name.substring(4));
                struct.getters.putIfAbsent(newName.toString(), m.handle);
            } else if (m.arguments.size() == 0 &&
                name.startsWith("is") &&
                name.length() > 2 &&
                Character.isUpperCase(name.charAt(2))) {
                StringBuilder newName = new StringBuilder();
                newName.append(Character.toLowerCase(name.charAt(2)));
                newName.append(name.substring(3));
                struct.getters.putIfAbsent(newName.toString(), m.handle);
            }

            if (m.arguments.size() == 1 &&
                name.startsWith("set") &&
                name.length() > 3 &&
                Character.isUpperCase(name.charAt(3))) {
                StringBuilder newName = new StringBuilder();
                newName.append(Character.toLowerCase(name.charAt(3)));
                newName.append(name.substring(4));
                struct.setters.putIfAbsent(newName.toString(), m.handle);
            }
        }

        // add all members
        for (Map.Entry<String, PainlessField> member : struct.members.entrySet()) {
            struct.getters.put(member.getKey(), member.getValue().getter);
            struct.setters.put(member.getKey(), member.getValue().setter);
        }
    }

    /** computes the functional interface method for a class, or returns null */
    private PainlessMethod computeFunctionalInterfaceMethod(PainlessClass clazz) {
        if (!clazz.clazz.isInterface()) {
            return null;
        }
        // if its marked with this annotation, we fail if the conditions don't hold (means whitelist bug)
        // otherwise, this annotation is pretty useless.
        boolean hasAnnotation = clazz.clazz.isAnnotationPresent(FunctionalInterface.class);
        List<java.lang.reflect.Method> methods = new ArrayList<>();
        for (java.lang.reflect.Method m : clazz.clazz.getMethods()) {
            // default interface methods don't count
            if (m.isDefault()) {
                continue;
            }
            // static methods don't count
            if (Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            // if its from Object, it doesn't count
            try {
                Object.class.getMethod(m.getName(), m.getParameterTypes());
                continue;
            } catch (ReflectiveOperationException e) {
                // it counts
            }
            methods.add(m);
        }
        if (methods.size() != 1) {
            if (hasAnnotation) {
                throw new IllegalArgumentException("Class: " + clazz.name +
                    " is marked with FunctionalInterface but doesn't fit the bill: " + methods);
            }
            return null;
        }
        // inspect the one method found from the reflection API, it should match the whitelist!
        java.lang.reflect.Method oneMethod = methods.get(0);
        PainlessMethod painless = clazz.methods.get(new PainlessMethodKey(oneMethod.getName(), oneMethod.getParameterCount()));
        if (painless == null || painless.method.equals(org.objectweb.asm.commons.Method.getMethod(oneMethod)) == false) {
            throw new IllegalArgumentException("Class: " + clazz.name + " is functional but the functional " +
                "method is not whitelisted!");
        }
        return painless;
    }
}
