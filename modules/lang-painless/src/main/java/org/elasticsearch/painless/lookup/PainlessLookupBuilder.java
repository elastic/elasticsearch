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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
        Class<?> javaClass = painlessClassNamesToJavaClasses.get(painlessClassName);

        if (javaClass == def.class) {
            throw new IllegalArgumentException("cannot add constructor for reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

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
                javaTypeParameters[parameterIndex] = painlessTypeToJavaType(painlessTypeParameter);
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
            throw new IllegalArgumentException("cannot add constructor for reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
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

            javaTypeParameters[parameterIndex++] = painlessTypeToJavaType(painlessTypeParameter);
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
        Class<?> javaClass = painlessClassNamesToJavaClasses.get(painlessClassName);

        if (javaClass == def.class) {
            throw new IllegalArgumentException("cannot add method for reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

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

        javaClass = painlessTypeToJavaType(javaClass);
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
                javaTypeParameters[parameterIndex + augmentedParameterOffset] = painlessTypeToJavaType(painlessTypeParameter);
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
            throw new IllegalArgumentException("cannot add constructor for reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
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

            javaTypeParameters[parameterIndex++] = painlessTypeToJavaType(painlessTypeParameter);
        }

        String anyTypeNameParameter = painlessTypeReturn.getCanonicalName();
        String painlessTypeNameParameter = anyTypeNameToPainlessTypeName(anyTypeNameParameter);

        try {
            validatePainlessType(painlessTypeReturn);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("painless type [" + painlessTypeNameParameter + "] return not found " +
                "for painless constructor [[" + painlessClassName + "], " + painlessTypeParameters  + "]", iae);
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

        if (javaMethod.getReturnType() != painlessTypeToJavaType(painlessTypeReturn)) {
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
}
