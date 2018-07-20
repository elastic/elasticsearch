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

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistClass;
import org.elasticsearch.painless.spi.WhitelistConstructor;
import org.elasticsearch.painless.spi.WhitelistField;
import org.elasticsearch.painless.spi.WhitelistMethod;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.regex.Pattern;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.CONSTRUCTOR_NAME;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.DEF_CLASS_NAME;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessFieldKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessMethodKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToJavaType;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typesToCanonicalTypeNames;

public class PainlessLookupBuilder {

    private static class PainlessMethodCacheKey {

        private final Class<?> targetType;
        private final String methodName;
        private final List<Class<?>> typeParameters;

        private PainlessMethodCacheKey(Class<?> targetType, String methodName, List<Class<?>> typeParameters) {
            this.targetType = targetType;
            this.methodName = methodName;
            this.typeParameters = Collections.unmodifiableList(typeParameters);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            PainlessMethodCacheKey that = (PainlessMethodCacheKey)object;

            return Objects.equals(targetType, that.targetType) &&
                   Objects.equals(methodName, that.methodName) &&
                   Objects.equals(typeParameters, that.typeParameters);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetType, methodName, typeParameters);
        }
    }

    private static class PainlessFieldCacheKey {

        private final Class<?> targetType;
        private final String fieldName;
        private final Class<?> typeParameter;

        private PainlessFieldCacheKey(Class<?> targetType, String fieldName, Class<?> typeParameter) {
            this.targetType = targetType;
            this.fieldName = fieldName;
            this.typeParameter = typeParameter;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            PainlessFieldCacheKey that = (PainlessFieldCacheKey) object;

            return Objects.equals(targetType, that.targetType) &&
                   Objects.equals(fieldName, that.fieldName)   &&
                   Objects.equals(typeParameter, that.typeParameter);
        }

        @Override
        public int hashCode() {
            return Objects.hash(targetType, fieldName, typeParameter);
        }
    }

    private static final Map<PainlessMethodCacheKey, PainlessMethod> painlessMethodCache = new HashMap<>();
    private static final Map<PainlessFieldCacheKey,  PainlessField>  painlessFieldCache  = new HashMap<>();

    private static final Pattern CLASS_NAME_PATTERN  = Pattern.compile("^[_a-zA-Z][._a-zA-Z0-9]*$");
    private static final Pattern METHOD_NAME_PATTERN = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");
    private static final Pattern FIELD_NAME_PATTERN  = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");

    private final List<Whitelist> whitelists;

    private final Map<String, Class<?>> canonicalClassNamesToClasses;
    private final Map<Class<?>, PainlessClassBuilder> classesToPainlessClassBuilders;

    public PainlessLookupBuilder(List<Whitelist> whitelists) {
        this.whitelists = whitelists;

        canonicalClassNamesToClasses = new HashMap<>();
        classesToPainlessClassBuilders = new HashMap<>();

        canonicalClassNamesToClasses.put(DEF_CLASS_NAME, def.class);
        classesToPainlessClassBuilders.put(def.class,
                new PainlessClassBuilder(DEF_CLASS_NAME, Object.class, org.objectweb.asm.Type.getType(Object.class)));
    }

    private Class<?> canonicalTypeNameToType(String canonicalTypeName) {
        return PainlessLookupUtility.canonicalTypeNameToType(canonicalTypeName, canonicalClassNamesToClasses);
    }

    private void validateType(Class<?> type) {
        PainlessLookupUtility.validateType(type, classesToPainlessClassBuilders.keySet());
    }

    public void addPainlessClass(ClassLoader classLoader, String javaClassName, boolean importClassName) {
        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(javaClassName);

        Class<?> clazz;

        if      ("void".equals(javaClassName))    clazz = void.class;
        else if ("boolean".equals(javaClassName)) clazz = boolean.class;
        else if ("byte".equals(javaClassName))    clazz = byte.class;
        else if ("short".equals(javaClassName))   clazz = short.class;
        else if ("char".equals(javaClassName))    clazz = char.class;
        else if ("int".equals(javaClassName))     clazz = int.class;
        else if ("long".equals(javaClassName))    clazz = long.class;
        else if ("float".equals(javaClassName))   clazz = float.class;
        else if ("double".equals(javaClassName))  clazz = double.class;
        else {
            try {
                clazz = Class.forName(javaClassName, true, classLoader);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException("class [" + javaClassName + "] not found", cnfe);
            }
        }

        addPainlessClass(clazz, importClassName);
    }

    public void addPainlessClass(Class<?> clazz, boolean importClassName) {
        Objects.requireNonNull(clazz);

        if (clazz == def.class) {
            throw new IllegalArgumentException("cannot add reserved class [" + DEF_CLASS_NAME + "]");
        }

        String canonicalClassName = typeToCanonicalTypeName(clazz);

        if (clazz.isArray()) {
            throw new IllegalArgumentException("cannot add array type [" + canonicalClassName + "] as a class");
        }

        if (CLASS_NAME_PATTERN.matcher(canonicalClassName).matches() == false) {
            throw new IllegalArgumentException("invalid class name [" + canonicalClassName + "]");
        }

        PainlessClassBuilder existingPainlessClassBuilder = classesToPainlessClassBuilders.get(clazz);

        if (existingPainlessClassBuilder == null) {
            PainlessClassBuilder painlessClassBuilder =
                    new PainlessClassBuilder(canonicalClassName, clazz, org.objectweb.asm.Type.getType(clazz));

            canonicalClassNamesToClasses.put(canonicalClassName, clazz);
            classesToPainlessClassBuilders.put(clazz, painlessClassBuilder);
        } else if (existingPainlessClassBuilder.clazz.equals(clazz) == false) {
            throw new IllegalArgumentException("class [" + canonicalClassName + "] " +
                    "cannot represent multiple java classes with the same name from different class loaders");
        }

        String javaClassName = clazz.getName();
        String importedCanonicalClassName = javaClassName.substring(javaClassName.lastIndexOf('.') + 1).replace('$', '.');

        if (canonicalClassName.equals(importedCanonicalClassName)) {
            if (importClassName == true) {
                throw new IllegalArgumentException("must use only_fqn parameter on class [" + canonicalClassName + "] with no package");
            }
        } else {
            Class<?> importedPainlessClass = canonicalClassNamesToClasses.get(importedCanonicalClassName);

            if (importedPainlessClass == null) {
                if (importClassName) {
                    if (existingPainlessClassBuilder != null) {
                        throw new IllegalArgumentException("inconsistent only_fqn parameters found for class [" + canonicalClassName + "]");
                    }

                    canonicalClassNamesToClasses.put(importedCanonicalClassName, clazz);
                }
            } else if (importedPainlessClass.equals(clazz) == false) {
                throw new IllegalArgumentException("imported class [" + importedCanonicalClassName + "] cannot represent multiple " +
                        "classes [" + canonicalClassName + "] and [" + typeToCanonicalTypeName(importedPainlessClass) + "]");
            } else if (importClassName == false) {
                throw new IllegalArgumentException("inconsistent only_fqn parameters found for class [" + canonicalClassName + "]");
            }
        }
    }

    public void addPainlessConstructor(String targetCanonicalClassName, List<String> typeNameParameters) {
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(typeNameParameters);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw new IllegalArgumentException("target class [" + targetCanonicalClassName + "] not found" +
                    "for constructor [[" + targetCanonicalClassName + "], " + typeNameParameters  + "]");
        }

        List<Class<?>> typeParameters = new ArrayList<>(typeNameParameters.size());

        for (String typeNameParameter : typeNameParameters) {
            try {
                Class<?> typeParameter = canonicalTypeNameToType(typeNameParameter);
                typeParameters.add(typeParameter);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("type parameter [" + typeNameParameter + "] not found " +
                        "for constructor [[" + targetCanonicalClassName + "], " + typeNameParameters  + "]", iae);
            }
        }

        addPainlessConstructor(targetClass, typeParameters);
    }

    public void addPainlessConstructor(Class<?> targetClass, List<Class<?>> typeParameters) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add constructor to reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = targetClass.getCanonicalName();
        PainlessClassBuilder painlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException("target class [" + targetCanonicalClassName + "] not found" +
                    "for constructor [[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters)  + "]");
        }

        int typeParametersSize = typeParameters.size();
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize);

        for (Class<?> typeParameter : typeParameters) {
            try {
                validateType(typeParameter);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] not found " +
                        "for constructor [[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "]", iae);
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        Constructor<?> javaConstructor;

        try {
            javaConstructor = targetClass.getConstructor(javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException("constructor reflection object " +
                    "[[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "] not found", nsme);
        }

        String painlessMethodKey = buildPainlessMethodKey(CONSTRUCTOR_NAME, typeParametersSize);
        PainlessMethod painlessConstructor = painlessClassBuilder.constructors.get(painlessMethodKey);

        if (painlessConstructor == null) {
            org.objectweb.asm.commons.Method asmConstructor = org.objectweb.asm.commons.Method.getMethod(javaConstructor);
            MethodHandle methodHandle;

            try {
                methodHandle = MethodHandles.publicLookup().in(targetClass).unreflectConstructor(javaConstructor);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException("constructor method handle " +
                        "[[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "] not found", iae);
            }

            painlessConstructor = painlessMethodCache.computeIfAbsent(
                    new PainlessMethodCacheKey(targetClass, CONSTRUCTOR_NAME, typeParameters),
                    key -> new PainlessMethod(CONSTRUCTOR_NAME, targetClass, null, void.class, typeParameters,
                                              asmConstructor, javaConstructor.getModifiers(), methodHandle)
            );

            painlessClassBuilder.constructors.put(painlessMethodKey, painlessConstructor);
        } else if (painlessConstructor.arguments.equals(typeParameters) == false){
            throw new IllegalArgumentException("cannot have constructors " +
                    "[[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "] and " +
                    "[[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(painlessConstructor.arguments) + "] " +
                    "with the same arity and different type parameters");
        }
    }

    public void addPainlessMethod(ClassLoader classLoader, String targetCanonicalClassName, String augmentedCanonicalClassName,
            String methodName, String returnCanonicalTypeName, List<String> typeNameParameters) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(typeNameParameters);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw new IllegalArgumentException("target class [" + targetCanonicalClassName + "] not found for method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + typeNameParameters + "]");
        }

        Class<?> augmentedClass = null;

        if (augmentedCanonicalClassName != null) {
            try {
                augmentedClass = Class.forName(augmentedCanonicalClassName, true, classLoader);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException("augmented class [" + augmentedCanonicalClassName + "] not found for method " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " + typeNameParameters + "]", cnfe);
            }
        }

        List<Class<?>> typeParameters = new ArrayList<>(typeNameParameters.size());

        for (String typeNameParameter : typeNameParameters) {
            try {
                Class<?> typeParameter = canonicalTypeNameToType(typeNameParameter);
                typeParameters.add(typeParameter);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("parameter type [" + typeNameParameter + "] not found for method " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " + typeNameParameters + "]", iae);
            }
        }

        Class<?> returnType;

        try {
            returnType = canonicalTypeNameToType(returnCanonicalTypeName);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("parameter type [" + returnCanonicalTypeName + "] not found for method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + typeNameParameters + "]", iae);
        }

        addPainlessMethod(targetClass, augmentedClass, methodName, returnType, typeParameters);
    }

    public void addPainlessMethod(Class<?> targetClass, Class<?> augmentedClass, String methodName,
            Class<?> returnType, List<Class<?>> typeParameters) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add method to reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                    "invalid method name [" + methodName + "] for target class [" + targetCanonicalClassName + "].");
        }

        PainlessClassBuilder painlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException("target class [" + targetCanonicalClassName + "] not found for method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
        }

        int typeParametersSize = typeParameters.size();
        int augmentedParameterOffset = augmentedClass == null ? 0 : 1;
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize + augmentedParameterOffset);

        if (augmentedClass != null) {
            javaTypeParameters.add(targetClass);
        }

        for (Class<?> typeParameter : typeParameters) {
            try {
                validateType(typeParameter);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] " +
                        "not found for method [[" + targetCanonicalClassName + "], [" + methodName + "], " +
                        typesToCanonicalTypeNames(typeParameters) + "]", iae);
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        try {
            validateType(returnType);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("return type [" + typeToCanonicalTypeName(returnType) + "] not found for method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "]", iae);
        }

        Method javaMethod;

        if (augmentedClass == null) {
            try {
                javaMethod = targetClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
            } catch (NoSuchMethodException nsme) {
                throw new IllegalArgumentException("method reflection object [[" + targetCanonicalClassName + "], " +
                        "[" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "] not found", nsme);
            }
        } else {
            try {
                javaMethod = augmentedClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
            } catch (NoSuchMethodException nsme) {
                throw new IllegalArgumentException("method reflection object [[" + targetCanonicalClassName + "], " +
                        "[" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "] not found " +
                        "with augmented target class [" + typeToCanonicalTypeName(augmentedClass) + "]", nsme);
            }
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw new IllegalArgumentException("return type [" + typeToCanonicalTypeName(javaMethod.getReturnType()) + "] " +
                    "does not match the specified returned type [" + typeToCanonicalTypeName(returnType) + "] " +
                    "for method [[" + targetClass.getCanonicalName() + "], [" + methodName + "], " +
                    typesToCanonicalTypeNames(typeParameters) + "]");
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, typeParametersSize);

        if (augmentedClass == null && Modifier.isStatic(javaMethod.getModifiers())) {
            PainlessMethod painlessMethod = painlessClassBuilder.staticMethods.get(painlessMethodKey);

            if (painlessMethod == null) {
                org.objectweb.asm.commons.Method asmMethod = org.objectweb.asm.commons.Method.getMethod(javaMethod);
                MethodHandle javaMethodHandle;

                try {
                    javaMethodHandle = MethodHandles.publicLookup().in(targetClass).unreflect(javaMethod);
                } catch (IllegalAccessException iae) {
                    throw new IllegalArgumentException("static method handle [[" + targetClass.getCanonicalName() + "], " +
                            "[" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "] not found", iae);
                }

                painlessMethod = painlessMethodCache.computeIfAbsent(
                        new PainlessMethodCacheKey(targetClass, methodName, typeParameters),
                        key -> new PainlessMethod(methodName, targetClass, null, returnType,
                                typeParameters, asmMethod, javaMethod.getModifiers(), javaMethodHandle));

                painlessClassBuilder.staticMethods.put(painlessMethodKey, painlessMethod);
            } else if ((painlessMethod.name.equals(methodName) && painlessMethod.rtn == returnType &&
                    painlessMethod.arguments.equals(typeParameters)) == false) {
                throw new IllegalArgumentException("cannot have static methods " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " +
                        "[" + typeToCanonicalTypeName(returnType) + "], " +
                        typesToCanonicalTypeNames(typeParameters) + "] and " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " +
                        "[" + typeToCanonicalTypeName(painlessMethod.rtn) + "], " +
                        typesToCanonicalTypeNames(painlessMethod.arguments) + "] " +
                        "with the same arity and different return type or type parameters");
            }
        } else {
            PainlessMethod painlessMethod = painlessClassBuilder.staticMethods.get(painlessMethodKey);

            if (painlessMethod == null) {
                org.objectweb.asm.commons.Method asmMethod = org.objectweb.asm.commons.Method.getMethod(javaMethod);
                MethodHandle javaMethodHandle;

                if (augmentedClass == null) {
                    try {
                        javaMethodHandle = MethodHandles.publicLookup().in(targetClass).unreflect(javaMethod);
                    } catch (IllegalAccessException iae) {
                        throw new IllegalArgumentException("method handle [[" + targetClass.getCanonicalName() + "], " +
                                "[" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "] not found", iae);
                    }
                } else {
                    try {
                        javaMethodHandle = MethodHandles.publicLookup().in(augmentedClass).unreflect(javaMethod);
                    } catch (IllegalAccessException iae) {
                        throw new IllegalArgumentException("method handle [[" + targetClass.getCanonicalName() + "], " +
                                "[" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "] not found " +
                                "with augmented target class [" + typeToCanonicalTypeName(augmentedClass) + "]", iae);
                    }
                }

                painlessMethod = painlessMethodCache.computeIfAbsent(
                        new PainlessMethodCacheKey(targetClass, methodName, typeParameters),
                        key -> new PainlessMethod(methodName, targetClass, augmentedClass, returnType,
                                typeParameters, asmMethod, javaMethod.getModifiers(), javaMethodHandle));

                painlessClassBuilder.methods.put(painlessMethodKey, painlessMethod);
            } else if ((painlessMethod.name.equals(methodName) && painlessMethod.rtn == returnType &&
                    painlessMethod.arguments.equals(typeParameters)) == false) {
                throw new IllegalArgumentException("cannot have methods " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " +
                        "[" + typeToCanonicalTypeName(returnType) + "], " +
                        typesToCanonicalTypeNames(typeParameters) + "] and " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " +
                        "[" + typeToCanonicalTypeName(painlessMethod.rtn) + "], " +
                        typesToCanonicalTypeNames(painlessMethod.arguments) + "] " +
                        "with the same arity and different return type or type parameters");
            }
        }
    }

    public void addPainlessField(String targetCanonicalClassName, String fieldName, String typeNameParameter) {
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(typeNameParameter);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw new IllegalArgumentException("class [" + targetCanonicalClassName + "] not found");
        }

        Class<?> typeParameter;

        try {
            typeParameter = canonicalTypeNameToType(typeNameParameter);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("type parameter [" + typeNameParameter + "] not found " +
                    "for field [[" + targetCanonicalClassName + "], [" + fieldName + "]");
        }


        addPainlessField(targetClass, fieldName, typeParameter);
    }

    public void addPainlessField(Class<?> targetClass, String fieldName, Class<?> typeParameter) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(typeParameter);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add field to reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        if (FIELD_NAME_PATTERN.matcher(fieldName).matches() == false) {
            throw new IllegalArgumentException(
                    "invalid field name [" + fieldName + "] for target class [" + targetCanonicalClassName + "].");
        }


        PainlessClassBuilder painlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException("class [" + targetCanonicalClassName + "] not found");
        }

        try {
            validateType(typeParameter);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] not found " +
                    "for field [[" + targetCanonicalClassName + "], [" + fieldName + "]", iae);
        }

        Field javaField;

        try {
            javaField = targetClass.getField(fieldName);
        } catch (NoSuchFieldException nsme) {
            throw new IllegalArgumentException(
                    "field reflection object [[" + targetCanonicalClassName + "], [" + fieldName + "] not found", nsme);
        }

        if (javaField.getType() != typeToJavaType(typeParameter)) {
            throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(javaField.getType()) + "] " +
                    "does not match the specified type parameter [" + typeToCanonicalTypeName(typeParameter) + "] " +
                    "for field [[" + targetCanonicalClassName + "], [" + fieldName + "]");
        }

        String painlessFieldKey = buildPainlessFieldKey(fieldName);

        if (Modifier.isStatic(javaField.getModifiers())) {
            if (Modifier.isFinal(javaField.getModifiers()) == false) {
                throw new IllegalArgumentException("static field [[" + targetCanonicalClassName + "]. [" + fieldName + "]] must be final");
            }

            PainlessField painlessField = painlessClassBuilder.staticMembers.get(painlessFieldKey);

            if (painlessField == null) {
                painlessField = painlessFieldCache.computeIfAbsent(
                        new PainlessFieldCacheKey(targetClass, fieldName, typeParameter),
                        key -> new PainlessField(fieldName, javaField.getName(), targetClass,
                                typeParameter, javaField.getModifiers(), null, null));

                painlessClassBuilder.staticMembers.put(painlessFieldKey, painlessField);
            } else if (painlessField.clazz != typeParameter) {
                throw new IllegalArgumentException("cannot have static fields " +
                        "[[" + targetCanonicalClassName + "], [" + fieldName + "], [" +
                        typeToCanonicalTypeName(typeParameter) + "] and " +
                        "[[" + targetCanonicalClassName + "], [" + painlessField.name + "], " +
                        typeToCanonicalTypeName(painlessField.clazz) + "] " +
                        "with the same and different type parameters");
            }
        } else {
            MethodHandle methodHandleGetter;

            try {
                methodHandleGetter = MethodHandles.publicLookup().unreflectGetter(javaField);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException(
                        "method handle getter not found for field [[" + targetCanonicalClassName + "], [" + fieldName + "]]");
            }

            MethodHandle methodHandleSetter;

            try {
                methodHandleSetter = MethodHandles.publicLookup().unreflectSetter(javaField);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException(
                        "method handle setter not found for field [[" + targetCanonicalClassName + "], [" + fieldName + "]]");
            }

            PainlessField painlessField = painlessClassBuilder.members.get(painlessFieldKey);

            if (painlessField == null) {
                painlessField = painlessFieldCache.computeIfAbsent(
                        new PainlessFieldCacheKey(targetClass, painlessFieldKey, typeParameter),
                        key -> new PainlessField(fieldName, javaField.getName(), targetClass,
                                typeParameter, javaField.getModifiers(), methodHandleGetter, methodHandleSetter));

                painlessClassBuilder.members.put(fieldName, painlessField);
            } else if (painlessField.clazz != typeParameter) {
                throw new IllegalArgumentException("cannot have fields " +
                        "[[" + targetCanonicalClassName + "], [" + fieldName + "], [" +
                        typeToCanonicalTypeName(typeParameter) + "] and " +
                        "[[" + targetCanonicalClassName + "], [" + painlessField.name + "], " +
                        typeToCanonicalTypeName(painlessField.clazz) + "] " +
                        "with the same and different type parameters");
            }
        }
    }

    private void copyStruct(String struct, List<String> children) {
        final PainlessClassBuilder owner = classesToPainlessClassBuilders.get(canonicalClassNamesToClasses.get(struct));

        if (owner == null) {
            throw new IllegalArgumentException("Owner struct [" + struct + "] not defined for copy.");
        }

        for (int count = 0; count < children.size(); ++count) {
            final PainlessClassBuilder child =
                    classesToPainlessClassBuilders.get(canonicalClassNamesToClasses.get(children.get(count)));

            if (child == null) {
                throw new IllegalArgumentException("Child struct [" + children.get(count) + "]" +
                    " not defined for copy to owner struct [" + owner.name + "].");
            }

            if (!child.clazz.isAssignableFrom(owner.clazz)) {
                throw new ClassCastException("Child struct [" + child.name + "]" +
                    " is not a super type of owner struct [" + owner.name + "] in copy.");
            }

            for (Map.Entry<String,PainlessMethod> kvPair : child.methods.entrySet()) {
                String methodKey = kvPair.getKey();
                PainlessMethod method = kvPair.getValue();
                if (owner.methods.get(methodKey) == null) {
                    // TODO: some of these are no longer valid or outright don't work
                    // TODO: since classes may not come from the Painless classloader
                    // TODO: and it was dependent on the order of the extends which
                    // TODO: which no longer exists since this is generated automatically
                    // sanity check, look for missing covariant/generic override
                    /*if (owner.clazz.isInterface() && child.clazz == Object.class) {
                        // ok
                    } else if (child.clazz == Spliterator.OfPrimitive.class || child.clazz == PrimitiveIterator.class) {
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
                    owner.methods.put(methodKey, method);
                }
            }

            for (PainlessField field : child.members.values()) {
                if (owner.members.get(field.name) == null) {
                    owner.members.put(field.name, new PainlessField(
                            field.name, field.javaName, owner.clazz, field.clazz, field.modifiers, field.getter, field.setter));
                }
            }
        }
    }

    /**
     * Precomputes a more efficient structure for dynamic method/field access.
     */
    private void addRuntimeClass(final PainlessClassBuilder struct) {
        // add all getters/setters
        for (Map.Entry<String, PainlessMethod> method : struct.methods.entrySet()) {
            String name = method.getValue().name;
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
    private PainlessMethod computeFunctionalInterfaceMethod(PainlessClassBuilder clazz) {
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
        PainlessMethod painless = clazz.methods.get(buildPainlessMethodKey(oneMethod.getName(), oneMethod.getParameterCount()));
        if (painless == null || painless.method.equals(org.objectweb.asm.commons.Method.getMethod(oneMethod)) == false) {
            throw new IllegalArgumentException("Class: " + clazz.name + " is functional but the functional " +
                "method is not whitelisted!");
        }
        return painless;
    }

    public PainlessLookup build() {
        String origin = "internal error";

        try {
            // first iteration collects all the Painless type names that
            // are used for validation during the second iteration
            for (Whitelist whitelist : whitelists) {
                for (WhitelistClass whitelistStruct : whitelist.whitelistStructs) {
                    String painlessTypeName = whitelistStruct.javaClassName.replace('$', '.');
                    PainlessClassBuilder painlessStruct =
                        classesToPainlessClassBuilders.get(canonicalClassNamesToClasses.get(painlessTypeName));

                    if (painlessStruct != null && painlessStruct.clazz.getName().equals(whitelistStruct.javaClassName) == false) {
                        throw new IllegalArgumentException("struct [" + painlessStruct.name + "] cannot represent multiple classes " +
                            "[" + painlessStruct.clazz.getName() + "] and [" + whitelistStruct.javaClassName + "]");
                    }

                    origin = whitelistStruct.origin;
                    addPainlessClass(
                            whitelist.javaClassLoader, whitelistStruct.javaClassName, whitelistStruct.onlyFQNJavaClassName == false);

                    painlessStruct = classesToPainlessClassBuilders.get(canonicalClassNamesToClasses.get(painlessTypeName));
                    classesToPainlessClassBuilders.put(painlessStruct.clazz, painlessStruct);
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
                        addPainlessConstructor(painlessTypeName, whitelistConstructor.painlessParameterTypeNames);
                    }

                    for (WhitelistMethod whitelistMethod : whitelistStruct.whitelistMethods) {
                        origin = whitelistMethod.origin;
                        addPainlessMethod(whitelist.javaClassLoader, painlessTypeName, whitelistMethod.javaAugmentedClassName,
                                whitelistMethod.javaMethodName, whitelistMethod.painlessReturnTypeName,
                                whitelistMethod.painlessParameterTypeNames);
                    }

                    for (WhitelistField whitelistField : whitelistStruct.whitelistFields) {
                        origin = whitelistField.origin;
                        addPainlessField(painlessTypeName, whitelistField.javaFieldName, whitelistField.painlessFieldTypeName);
                    }
                }
            }
        } catch (Exception exception) {
            throw new IllegalArgumentException("error loading whitelist(s) " + origin, exception);
        }

        // goes through each Painless struct and determines the inheritance list,
        // and then adds all inherited types to the Painless struct's whitelist
        for (Class<?> javaClass : classesToPainlessClassBuilders.keySet()) {
            PainlessClassBuilder painlessStruct = classesToPainlessClassBuilders.get(javaClass);

            List<String> painlessSuperStructs = new ArrayList<>();
            Class<?> javaSuperClass = painlessStruct.clazz.getSuperclass();

            Stack<Class<?>> javaInteraceLookups = new Stack<>();
            javaInteraceLookups.push(painlessStruct.clazz);

            // adds super classes to the inheritance list
            if (javaSuperClass != null && javaSuperClass.isInterface() == false) {
                while (javaSuperClass != null) {
                    PainlessClassBuilder painlessSuperStruct = classesToPainlessClassBuilders.get(javaSuperClass);

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
                    PainlessClassBuilder painlessInterfaceStruct = classesToPainlessClassBuilders.get(javaSuperInterface);

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
                PainlessClassBuilder painlessObjectStruct = classesToPainlessClassBuilders.get(Object.class);

                if (painlessObjectStruct != null) {
                    copyStruct(painlessStruct.name, Collections.singletonList(painlessObjectStruct.name));
                }
            }
        }

        // precompute runtime classes
        for (PainlessClassBuilder painlessStruct : classesToPainlessClassBuilders.values()) {
            addRuntimeClass(painlessStruct);
        }

        Map<Class<?>, PainlessClass> javaClassesToPainlessClasses = new HashMap<>();

        // copy all structs to make them unmodifiable for outside users:
        for (Map.Entry<Class<?>,PainlessClassBuilder> entry : classesToPainlessClassBuilders.entrySet()) {
            entry.getValue().functionalMethod = computeFunctionalInterfaceMethod(entry.getValue());
            javaClassesToPainlessClasses.put(entry.getKey(), entry.getValue().build());
        }

        return new PainlessLookup(canonicalClassNamesToClasses, javaClassesToPainlessClasses);
    }
}
