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

    public PainlessLookupBuilder() {
        super();
    }

    public void addPainlessClass(ClassLoader classLoader, String javaClassName, boolean noImport) {
        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(javaClassName);

        String painlessClassName = anyTypeNametoPainlessTypeName(javaClassName);

        if (DEF_PAINLESS_CLASS_NAME.equals(painlessClassName)) {
            throw new IllegalArgumentException("cannot specify reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

        if (CLASS_NAME_PATTERN.matcher(painlessClassName).matches() == false) {
            throw new IllegalArgumentException("invalid painless class name [" + painlessClassName + "]");
        }

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

                if (javaClass.isArray()) {
                    throw new IllegalArgumentException(
                        "cannot specify an array type java class [" + javaClassName + "] as a painless class");
                }

            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException("java class [" + javaClassName + "] not found", cnfe);
            }
        }

        addPainlessClass(javaClassName, painlessClassName, javaClass, noImport);
    }

    public void addPainlessClass(Class<?> javaClass, boolean noImport) {
        Objects.requireNonNull(javaClass);

        if (javaClass == def.class) {
            throw new IllegalArgumentException("cannot specify reserved painless class [" + DEF_PAINLESS_CLASS_NAME + "]");
        }

        String javaClassName = javaClass.getName();
        String painlessClassName = anyTypeNametoPainlessTypeName(javaClassName);

        addPainlessClass(javaClassName, painlessClassName, javaClass, noImport);
    }

    private void addPainlessClass(String javaClassName, String painlessClassName, Class<?> javaClass, boolean noImport) {
        String importedPainlessClassName = painlessClassName;
        int index = javaClassName.lastIndexOf('.');

        if (index != -1) {
            importedPainlessClassName = anyTypeNametoPainlessTypeName(javaClassName.substring(index + 1));
        }

        PainlessClass existingPainlessClassObject = javaClassesToPainlessClasses.get(javaClass);

        if (existingPainlessClassObject == null) {
            PainlessClass painlessClassObject = new PainlessClass(painlessClassName, javaClass, Type.getType(javaClass));
            painlessClassNamesToJavaClasses.put(painlessClassName, javaClass);
            javaClassesToPainlessClasses.put(javaClass, painlessClassObject);
        } else if (existingPainlessClassObject.clazz.equals(javaClass) == false) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] illegally represents " +
                    "multiple java classes [" + javaClass.getName() + "] and [" + existingPainlessClassObject.clazz.getName() + "]");
        }

        if (painlessClassName.equals(importedPainlessClassName)) {
            if (noImport == false) {
                throw new IllegalArgumentException(
                        "must use only_fqn parameter on painless class [" + painlessClassName + "] with no package");
            }
        } else {
            Class<?> importedJavaClass = painlessClassNamesToJavaClasses.get(importedPainlessClassName);

            if (importedJavaClass == null) {
                if (noImport == false) {
                    if (existingPainlessClassObject != null) {
                        throw new IllegalArgumentException(
                                "inconsistent only_fqn parameters found for painless class [" + painlessClassName + "]");
                    }

                    painlessClassNamesToJavaClasses.put(importedPainlessClassName, javaClass);
                }
            } else if (importedJavaClass.equals(javaClass) == false) {
                throw new IllegalArgumentException("painless class [" + painlessClassName + "] illegally represents " +
                        "multiple java classes [" + javaClass.getName() + "] and [" + importedJavaClass.getName() + "]");
            } else if (noImport) {
                throw new IllegalArgumentException(
                        "inconsistent only_fqn parameters found for painless class [" + painlessClassName + "]");
            }
        }
    }

    public void addPainlessConstructor(String javaClassName, List<String> anyTypeNameParameters) {
        Objects.requireNonNull(javaClassName);
        Objects.requireNonNull(anyTypeNameParameters);

        String painlessClassName = anyTypeNametoPainlessTypeName(javaClassName);
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
            String painlessTypeNameParameter = anyTypeNametoPainlessTypeName(anyTypeNameParameter);
            painlessTypeNameParameters.add(painlessTypeNameParameter);
        }

        List<Class<?>> painlessTypeParameters = new ArrayList<>(parametersSize);
        Class<?>[] javaTypeParameters = new Class<?>[parametersSize];

        for (int parameterIndex = 0; parameterIndex < parametersSize; ++parameterIndex) {
            String painlessTypeNameParameter = painlessTypeNameParameters.get(parameterIndex));

            try {
                Class<?> painlessTypeParameter = painlessTypeNameToPainlessType(painlessTypeNameParameter);

                painlessTypeParameters.add(painlessTypeParameter);
                javaTypeParameters[parameterIndex] = painlessTypeToJavaType(painlessTypeParameter);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("painless type [" + painlessTypeNameParameter + "] not found " +
                    "for painless constructor [[" + painlessClassName + "], " + painlessTypeNameParameters  + "]", iae);
            }
        }

        addPainlessConstructor(painlessClass, painlessTypeParameters, javaTypeParameters);
    }

    public void addPainlessConstructor(Class<?> javaClass, List<Class<?>> painlessTypeParameters) {
        Objects.requireNonNull(javaClass);

        PainlessClass painlessClass = javaClassesToPainlessClasses.get(javaClass);

        if (painlessClass == null) {
            String javaClassName = javaClass.getName();
            String painlessClassName = anyTypeNametoPainlessTypeName(javaClassName);

            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        int parametersSize = painlessTypeParameters.size();
        Class<?>[] javaTypeParameters = new Class<?>[parametersSize];
        int parameterIndex = 0;

        for (Class<?> painlessTypeParameter : painlessTypeParameters) {
            javaTypeParameters[parameterIndex++] = painlessTypeToJavaType(painlessTypeParameter);
        }

        addPainlessConstructor(painlessClass, painlessTypeParameters, javaTypeParameters);
    }

    private void addPainlessConstructor(PainlessClass painlessClass, List<Class<?>> painlessTypeParameters, Class<?>[] javaTypeParameters) {
        Constructor<?> javaConstructor;

        try {
            javaConstructor = painlessClass.clazz.getConstructor(javaTypeParameters);
        } catch (NoSuchMethodException exception) {
            throw new IllegalArgumentException(
                    "java constructor [[" + painlessClass.clazz.getName() + "], " + Arrays.asList(javaTypeParameters) + "] not found");
        }

        int parametersSize = painlessTypeParameters.size();
        String painlessMethodKey = buildPainlessMethodKey(PAINLESS_CONSTRUCTOR_NAME, parametersSize);
        PainlessMethod painlessConstructor = painlessClass.constructors.get(painlessMethodKey);

        if (painlessConstructor == null) {
            Method asmConstructor = Method.getMethod(javaConstructor);
            MethodHandle methodHandle;

            try {
                methodHandle = MethodHandles.publicLookup().in(painlessClass.clazz).unreflectConstructor(javaConstructor);
            } catch (IllegalAccessException exception) {
                throw new IllegalArgumentException(
                        "java constructor [[" + painlessClass.clazz.getName() + "], " + Arrays.asList(javaTypeParameters) + "] not found");
            }

            painlessConstructor = painlessMethodCache.computeIfAbsent(
                    new PainlessMethodCacheKey(painlessClass.clazz, PAINLESS_CONSTRUCTOR_NAME, painlessTypeParameters),
                    key -> new PainlessMethod(PAINLESS_CONSTRUCTOR_NAME, painlessClass, null, void.class, painlessTypeParameters,
                                              asmConstructor, javaConstructor.getModifiers(), methodHandle)
            );

            painlessClass.constructors.put(painlessMethodKey, painlessConstructor);
        } else if (painlessConstructor.arguments.equals(painlessTypeParameters) == false){
            throw new IllegalArgumentException(
                    "illegal duplicate constructors for painless class [" + painlessClass.name + "] " +
                    "with parameters " + painlessTypeParameters + " and " + painlessConstructor.arguments);
        }
    }

    public void addPainlessMethod(ClassLoader classLoader, String javaClassName, WhitelistMethod whitelistMethod) {
        PainlessClass ownerStruct = javaClassesToPainlessStructs.get(painlessTypesToJavaClasses.get(ownerStructName));

        if (ownerStruct == null) {
            throw new IllegalArgumentException("owner struct [" + ownerStructName + "] not defined for method with " +
                "name [" + whitelistMethod.javaMethodName + "] and parameters " + whitelistMethod.painlessParameterTypeNames);
        }

        if (TYPE_NAME_PATTERN.matcher(whitelistMethod.javaMethodName).matches() == false) {
            throw new IllegalArgumentException("invalid method name" +
                " [" + whitelistMethod.javaMethodName + "] for owner struct [" + ownerStructName + "].");
        }

        Class<?> javaAugmentedClass;

        if (whitelistMethod.javaAugmentedClassName != null) {
            try {
                javaAugmentedClass = Class.forName(whitelistMethod.javaAugmentedClassName, true, whitelistClassLoader);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException("augmented class [" + whitelistMethod.javaAugmentedClassName + "] " +
                    "not found for method with name [" + whitelistMethod.javaMethodName + "] " +
                    "and parameters " + whitelistMethod.painlessParameterTypeNames, cnfe);
            }
        } else {
            javaAugmentedClass = null;
        }

        int augmentedOffset = javaAugmentedClass == null ? 0 : 1;

        List<Class<?>> painlessParametersTypes = new ArrayList<>(whitelistMethod.painlessParameterTypeNames.size());
        Class<?>[] javaClassParameters = new Class<?>[whitelistMethod.painlessParameterTypeNames.size() + augmentedOffset];

        if (javaAugmentedClass != null) {
            javaClassParameters[0] = ownerStruct.clazz;
        }

        for (int parameterCount = 0; parameterCount < whitelistMethod.painlessParameterTypeNames.size(); ++parameterCount) {
            String painlessParameterTypeName = whitelistMethod.painlessParameterTypeNames.get(parameterCount);

            try {
                Class<?> painlessParameterClass = getJavaClassFromPainlessType(painlessParameterTypeName);

                painlessParametersTypes.add(painlessParameterClass);
                javaClassParameters[parameterCount + augmentedOffset] = defClassToObjectClass(painlessParameterClass);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("struct not defined for method parameter [" + painlessParameterTypeName + "] " +
                    "with owner struct [" + ownerStructName + "] and method with name [" + whitelistMethod.javaMethodName + "] " +
                    "and parameters " + whitelistMethod.painlessParameterTypeNames, iae);
            }
        }

        Class<?> javaImplClass = javaAugmentedClass == null ? ownerStruct.clazz : javaAugmentedClass;
        java.lang.reflect.Method javaMethod;

        try {
            javaMethod = javaImplClass.getMethod(whitelistMethod.javaMethodName, javaClassParameters);
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException("method with name [" + whitelistMethod.javaMethodName + "] " +
                "and parameters " + whitelistMethod.painlessParameterTypeNames + " not found for class [" +
                javaImplClass.getName() + "]", nsme);
        }

        Class<?> painlessReturnClass;

        try {
            painlessReturnClass = getJavaClassFromPainlessType(whitelistMethod.painlessReturnTypeName);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("struct not defined for return type [" + whitelistMethod.painlessReturnTypeName + "] " +
                "with owner struct [" + ownerStructName + "] and method with name [" + whitelistMethod.javaMethodName + "] " +
                "and parameters " + whitelistMethod.painlessParameterTypeNames, iae);
        }

        if (javaMethod.getReturnType() != defClassToObjectClass(painlessReturnClass)) {
            throw new IllegalArgumentException("specified return type class [" + painlessReturnClass + "] " +
                "does not match the return type class [" + javaMethod.getReturnType() + "] for the " +
                "method with name [" + whitelistMethod.javaMethodName + "] " +
                "and parameters " + whitelistMethod.painlessParameterTypeNames);
        }

        PainlessMethodKey painlessMethodKey =
            new PainlessMethodKey(whitelistMethod.javaMethodName, whitelistMethod.painlessParameterTypeNames.size());

        if (javaAugmentedClass == null && Modifier.isStatic(javaMethod.getModifiers())) {
            PainlessMethod painlessMethod = ownerStruct.staticMethods.get(painlessMethodKey);

            if (painlessMethod == null) {
                org.objectweb.asm.commons.Method asmMethod = org.objectweb.asm.commons.Method.getMethod(javaMethod);
                MethodHandle javaMethodHandle;

                try {
                    javaMethodHandle = MethodHandles.publicLookup().in(javaImplClass).unreflect(javaMethod);
                } catch (IllegalAccessException exception) {
                    throw new IllegalArgumentException("method handle not found for method with name " +
                        "[" + whitelistMethod.javaMethodName + "] and parameters " + whitelistMethod.painlessParameterTypeNames);
                }

                painlessMethod = methodCache.computeIfAbsent(
                    buildMethodCacheKey(ownerStruct.name, whitelistMethod.javaMethodName, painlessParametersTypes),
                    key -> new PainlessMethod(whitelistMethod.javaMethodName, ownerStruct, null, painlessReturnClass,
                        painlessParametersTypes, asmMethod, javaMethod.getModifiers(), javaMethodHandle));
                ownerStruct.staticMethods.put(painlessMethodKey, painlessMethod);
            } else if ((painlessMethod.name.equals(whitelistMethod.javaMethodName) && painlessMethod.rtn == painlessReturnClass &&
                painlessMethod.arguments.equals(painlessParametersTypes)) == false) {
                throw new IllegalArgumentException("illegal duplicate static methods [" + painlessMethodKey + "] " +
                    "found within the struct [" + ownerStruct.name + "] with name [" + whitelistMethod.javaMethodName + "], " +
                    "return types [" + painlessReturnClass + "] and [" + painlessMethod.rtn + "], " +
                    "and parameters " + painlessParametersTypes + " and " + painlessMethod.arguments);
            }
        } else {
            PainlessMethod painlessMethod = ownerStruct.methods.get(painlessMethodKey);

            if (painlessMethod == null) {
                org.objectweb.asm.commons.Method asmMethod = org.objectweb.asm.commons.Method.getMethod(javaMethod);
                MethodHandle javaMethodHandle;

                try {
                    javaMethodHandle = MethodHandles.publicLookup().in(javaImplClass).unreflect(javaMethod);
                } catch (IllegalAccessException exception) {
                    throw new IllegalArgumentException("method handle not found for method with name " +
                        "[" + whitelistMethod.javaMethodName + "] and parameters " + whitelistMethod.painlessParameterTypeNames);
                }

                painlessMethod = methodCache.computeIfAbsent(
                    buildMethodCacheKey(ownerStruct.name, whitelistMethod.javaMethodName, painlessParametersTypes),
                    key -> new PainlessMethod(whitelistMethod.javaMethodName, ownerStruct, javaAugmentedClass, painlessReturnClass,
                        painlessParametersTypes, asmMethod, javaMethod.getModifiers(), javaMethodHandle));
                ownerStruct.methods.put(painlessMethodKey, painlessMethod);
            } else if ((painlessMethod.name.equals(whitelistMethod.javaMethodName) && painlessMethod.rtn.equals(painlessReturnClass) &&
                painlessMethod.arguments.equals(painlessParametersTypes)) == false) {
                throw new IllegalArgumentException("illegal duplicate member methods [" + painlessMethodKey + "] " +
                    "found within the struct [" + ownerStruct.name + "] with name [" + whitelistMethod.javaMethodName + "], " +
                    "return types [" + painlessReturnClass + "] and [" + painlessMethod.rtn + "], " +
                    "and parameters " + painlessParametersTypes + " and " + painlessMethod.arguments);
            }
        }
    }
}
