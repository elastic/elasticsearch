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

        copyPainlessClassMembers();
        cacheRuntimeHandles();
        setFunctionalInterfaceMethods();

        Map<Class<?>, PainlessClass> classesToPainlessClasses = new HashMap<>(classesToPainlessClassBuilders.size());

        for (Map.Entry<Class<?>, PainlessClassBuilder> painlessClassBuilderEntry : classesToPainlessClassBuilders.entrySet()) {
            classesToPainlessClasses.put(painlessClassBuilderEntry.getKey(), painlessClassBuilderEntry.getValue().build());
        }

        return new PainlessLookup(canonicalClassNamesToClasses, classesToPainlessClasses);
    }

    private void copyPainlessClassMembers() {
        for (Class<?> parentClass : classesToPainlessClassBuilders.keySet()) {
            copyPainlessInterfaceMembers(parentClass, parentClass);

            Class<?> childClass = parentClass.getSuperclass();

            while (childClass != null) {
                if (classesToPainlessClassBuilders.containsKey(childClass)) {
                    copyPainlessClassMembers(childClass, parentClass);
                }

                copyPainlessInterfaceMembers(childClass, parentClass);
                childClass = childClass.getSuperclass();
            }
        }

        for (Class<?> javaClass : classesToPainlessClassBuilders.keySet()) {
            if (javaClass.isInterface()) {
                copyPainlessClassMembers(Object.class, javaClass);
            }
        }
    }

    private void copyPainlessInterfaceMembers(Class<?> parentClass, Class<?> targetClass) {
        for (Class<?> childClass : parentClass.getInterfaces()) {
            if (classesToPainlessClassBuilders.containsKey(childClass)) {
                copyPainlessClassMembers(childClass, targetClass);
            }

            copyPainlessInterfaceMembers(childClass, targetClass);
        }
    }

    private void copyPainlessClassMembers(Class<?> originalClass, Class<?> targetClass) {
        PainlessClassBuilder originalPainlessClassBuilder = classesToPainlessClassBuilders.get(originalClass);
        PainlessClassBuilder targetPainlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

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
        for (PainlessClassBuilder painlessClassBuilder : classesToPainlessClassBuilders.values()) {
            cacheRuntimeHandles(painlessClassBuilder);
        }
    }

    private void cacheRuntimeHandles(PainlessClassBuilder painlessClassBuilder) {
        for (PainlessMethod painlessMethod : painlessClassBuilder.methods.values()) {
            String methodName = painlessMethod.name;
            int typeParametersSize = painlessMethod.arguments.size();

            if (typeParametersSize == 0 && methodName.startsWith("get") && methodName.length() > 3 &&
                    Character.isUpperCase(methodName.charAt(3))) {
                painlessClassBuilder.getters.putIfAbsent(
                        Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4), painlessMethod.handle);
            } else if (typeParametersSize == 0 && methodName.startsWith("is") && methodName.length() > 2 &&
                    Character.isUpperCase(methodName.charAt(2))) {
                painlessClassBuilder.getters.putIfAbsent(
                        Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3), painlessMethod.handle);
            } else if (typeParametersSize == 1 && methodName.startsWith("set") && methodName.length() > 3 &&
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
        for (Map.Entry<Class<?>, PainlessClassBuilder> painlessClassBuilderEntry : classesToPainlessClassBuilders.entrySet()) {
            setFunctionalInterfaceMethod(painlessClassBuilderEntry.getValue());
        }
    }

    private void setFunctionalInterfaceMethod(PainlessClassBuilder painlessClassBuilder) {
        Class<?> targetClass = painlessClassBuilder.clazz;

        if (targetClass.isInterface()) {
            List<java.lang.reflect.Method> javaMethods = new ArrayList<>();

            for (java.lang.reflect.Method javaMethod : targetClass.getMethods()) {
                if (javaMethod.isDefault() == false && Modifier.isStatic(javaMethod.getModifiers()) == false) {
                    try {
                        Object.class.getMethod(javaMethod.getName(), javaMethod.getParameterTypes());
                    } catch (ReflectiveOperationException roe) {
                        javaMethods.add(javaMethod);
                    }
                }
            }

            if (javaMethods.size() != 1 && targetClass.isAnnotationPresent(FunctionalInterface.class)) {
                throw new IllegalArgumentException("class [" + typeToCanonicalTypeName(targetClass) + "] " +
                        "is illegally marked as a FunctionalInterface with java methods " + javaMethods);
            } else if (javaMethods.size() == 1) {
                java.lang.reflect.Method javaMethod = javaMethods.get(0);
                String painlessMethodKey = buildPainlessMethodKey(javaMethod.getName(), javaMethod.getParameterCount());
                painlessClassBuilder.functionalMethod = painlessClassBuilder.methods.get(painlessMethodKey);
            }
        }
    }
}
