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
import org.elasticsearch.painless.spi.WhitelistClassBinding;
import org.elasticsearch.painless.spi.WhitelistConstructor;
import org.elasticsearch.painless.spi.WhitelistField;
import org.elasticsearch.painless.spi.WhitelistMethod;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.DEF_CLASS_NAME;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessConstructorKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessFieldKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessMethodKey;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToJavaType;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typesToCanonicalTypeNames;

public final class PainlessLookupBuilder {

    private static final Map<PainlessConstructor , PainlessConstructor>  painlessConstructorCache  = new HashMap<>();
    private static final Map<PainlessMethod      , PainlessMethod>       painlessMethodCache       = new HashMap<>();
    private static final Map<PainlessField       , PainlessField>        painlessFieldCache        = new HashMap<>();
    private static final Map<PainlessClassBinding, PainlessClassBinding> painlessClassBindingCache = new HashMap<>();

    private static final Pattern CLASS_NAME_PATTERN  = Pattern.compile("^[_a-zA-Z][._a-zA-Z0-9]*$");
    private static final Pattern METHOD_NAME_PATTERN = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");
    private static final Pattern FIELD_NAME_PATTERN  = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$");

    public static PainlessLookup buildFromWhitelists(List<Whitelist> whitelists) {
        PainlessLookupBuilder painlessLookupBuilder = new PainlessLookupBuilder();
        String origin = "internal error";

        try {
            for (Whitelist whitelist : whitelists) {
                for (WhitelistClass whitelistClass : whitelist.whitelistClasses) {
                    origin = whitelistClass.origin;
                    painlessLookupBuilder.addPainlessClass(
                            whitelist.classLoader, whitelistClass.javaClassName, whitelistClass.noImport == false);
                }
            }

            for (Whitelist whitelist : whitelists) {
                for (WhitelistClass whitelistClass : whitelist.whitelistClasses) {
                    String targetCanonicalClassName = whitelistClass.javaClassName.replace('$', '.');

                    for (WhitelistConstructor whitelistConstructor : whitelistClass.whitelistConstructors) {
                        origin = whitelistConstructor.origin;
                        painlessLookupBuilder.addPainlessConstructor(
                                targetCanonicalClassName, whitelistConstructor.canonicalTypeNameParameters);
                    }

                    for (WhitelistMethod whitelistMethod : whitelistClass.whitelistMethods) {
                        origin = whitelistMethod.origin;
                        painlessLookupBuilder.addPainlessMethod(
                                whitelist.classLoader, targetCanonicalClassName, whitelistMethod.augmentedCanonicalClassName,
                                whitelistMethod.methodName, whitelistMethod.returnCanonicalTypeName,
                                whitelistMethod.canonicalTypeNameParameters);
                    }

                    for (WhitelistField whitelistField : whitelistClass.whitelistFields) {
                        origin = whitelistField.origin;
                        painlessLookupBuilder.addPainlessField(
                                targetCanonicalClassName, whitelistField.fieldName, whitelistField.canonicalTypeNameParameter);
                    }
                }

                for (WhitelistMethod whitelistStatic : whitelist.whitelistImportedMethods) {
                    origin = whitelistStatic.origin;
                    painlessLookupBuilder.addImportedPainlessMethod(
                            whitelist.classLoader, whitelistStatic.augmentedCanonicalClassName,
                            whitelistStatic.methodName, whitelistStatic.returnCanonicalTypeName,
                            whitelistStatic.canonicalTypeNameParameters);
                }

                for (WhitelistClassBinding whitelistClassBinding : whitelist.whitelistClassBindings) {
                    origin = whitelistClassBinding.origin;
                    painlessLookupBuilder.addPainlessClassBinding(
                            whitelist.classLoader, whitelistClassBinding.targetJavaClassName,
                            whitelistClassBinding.methodName, whitelistClassBinding.returnCanonicalTypeName,
                            whitelistClassBinding.canonicalTypeNameParameters);
                }
            }
        } catch (Exception exception) {
            throw new IllegalArgumentException("error loading whitelist(s) " + origin, exception);
        }

        return painlessLookupBuilder.build();
    }

    private final Map<String, Class<?>> canonicalClassNamesToClasses;
    private final Map<Class<?>, PainlessClassBuilder> classesToPainlessClassBuilders;

    private final Map<String, PainlessMethod> painlessMethodKeysToImportedPainlessMethods;
    private final Map<String, PainlessClassBinding> painlessMethodKeysToPainlessClassBindings;

    public PainlessLookupBuilder() {
        canonicalClassNamesToClasses = new HashMap<>();
        classesToPainlessClassBuilders = new HashMap<>();

        painlessMethodKeysToImportedPainlessMethods = new HashMap<>();
        painlessMethodKeysToPainlessClassBindings = new HashMap<>();
    }

    private Class<?> canonicalTypeNameToType(String canonicalTypeName) {
        return PainlessLookupUtility.canonicalTypeNameToType(canonicalTypeName, canonicalClassNamesToClasses);
    }

    private boolean isValidType(Class<?> type) {
        while (type.getComponentType() != null) {
            type = type.getComponentType();
        }

        return type == def.class || classesToPainlessClassBuilders.containsKey(type);
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

        Class<?> existingClass = canonicalClassNamesToClasses.get(canonicalClassName);

        if (existingClass != null && existingClass != clazz) {
            throw new IllegalArgumentException("class [" + canonicalClassName + "] " +
                    "cannot represent multiple java classes with the same name from different class loaders");
        }

        PainlessClassBuilder existingPainlessClassBuilder = classesToPainlessClassBuilders.get(clazz);

        if (existingPainlessClassBuilder == null) {
            PainlessClassBuilder painlessClassBuilder = new PainlessClassBuilder();

            canonicalClassNamesToClasses.put(canonicalClassName, clazz);
            classesToPainlessClassBuilders.put(clazz, painlessClassBuilder);
        }

        String javaClassName = clazz.getName();
        String importedCanonicalClassName = javaClassName.substring(javaClassName.lastIndexOf('.') + 1).replace('$', '.');

        if (canonicalClassName.equals(importedCanonicalClassName)) {
            if (importClassName == true) {
                throw new IllegalArgumentException("must use no_import parameter on class [" + canonicalClassName + "] with no package");
            }
        } else {
            Class<?> importedClass = canonicalClassNamesToClasses.get(importedCanonicalClassName);

            if (importedClass == null) {
                if (importClassName) {
                    if (existingPainlessClassBuilder != null) {
                        throw new IllegalArgumentException(
                                "inconsistent no_import parameter found for class [" + canonicalClassName + "]");
                    }

                    canonicalClassNamesToClasses.put(importedCanonicalClassName, clazz);
                }
            } else if (importedClass != clazz) {
                throw new IllegalArgumentException("imported class [" + importedCanonicalClassName + "] cannot represent multiple " +
                        "classes [" + canonicalClassName + "] and [" + typeToCanonicalTypeName(importedClass) + "]");
            } else if (importClassName == false) {
                throw new IllegalArgumentException("inconsistent no_import parameter found for class [" + canonicalClassName + "]");
            }
        }
    }

    public void addPainlessConstructor(String targetCanonicalClassName, List<String> canonicalTypeNameParameters) {
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw new IllegalArgumentException("target class [" + targetCanonicalClassName + "] not found" +
                    "for constructor [[" + targetCanonicalClassName + "], " + canonicalTypeNameParameters  + "]");
        }

        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw new IllegalArgumentException("type parameter [" + canonicalTypeNameParameter + "] not found " +
                        "for constructor [[" + targetCanonicalClassName + "], " + canonicalTypeNameParameters  + "]");
            }

            typeParameters.add(typeParameter);
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
            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] not found " +
                        "for constructor [[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        Constructor<?> javaConstructor;

        try {
            javaConstructor = targetClass.getConstructor(javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException("reflection object not found for constructor " +
                    "[[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "]", nsme);
        }

        MethodHandle methodHandle;

        try {
            methodHandle = MethodHandles.publicLookup().in(targetClass).unreflectConstructor(javaConstructor);
        } catch (IllegalAccessException iae) {
            throw new IllegalArgumentException("method handle not found for constructor " +
                    "[[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "]", iae);
        }

        MethodType methodType = methodHandle.type();

        String painlessConstructorKey = buildPainlessConstructorKey(typeParametersSize);
        PainlessConstructor existingPainlessConstructor = painlessClassBuilder.constructors.get(painlessConstructorKey);
        PainlessConstructor newPainlessConstructor = new PainlessConstructor(javaConstructor, typeParameters, methodHandle, methodType);

        if (existingPainlessConstructor == null) {
            newPainlessConstructor = painlessConstructorCache.computeIfAbsent(newPainlessConstructor, key -> key);
            painlessClassBuilder.constructors.put(painlessConstructorKey, newPainlessConstructor);
        } else if (newPainlessConstructor.equals(existingPainlessConstructor) == false){
            throw new IllegalArgumentException("cannot add constructors with the same arity but are not equivalent for constructors " +
                    "[[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "] and " +
                    "[[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(existingPainlessConstructor.typeParameters) + "]");
        }
    }

    public void addPainlessMethod(ClassLoader classLoader, String targetCanonicalClassName, String augmentedCanonicalClassName,
            String methodName, String returnCanonicalTypeName, List<String> canonicalTypeNameParameters) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw new IllegalArgumentException("target class [" + targetCanonicalClassName + "] not found for method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + canonicalTypeNameParameters + "]");
        }

        Class<?> augmentedClass = null;

        if (augmentedCanonicalClassName != null) {
            try {
                augmentedClass = Class.forName(augmentedCanonicalClassName, true, classLoader);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException("augmented class [" + augmentedCanonicalClassName + "] not found for method " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " + canonicalTypeNameParameters + "]", cnfe);
            }
        }

        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw new IllegalArgumentException("type parameter [" + canonicalTypeNameParameter + "] not found for method " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " + canonicalTypeNameParameters + "]");
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw new IllegalArgumentException("return type [" + returnCanonicalTypeName + "] not found for method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + canonicalTypeNameParameters + "]");
        }

        addPainlessMethod(targetClass, augmentedClass, methodName, returnType, typeParameters);
    }

    public void addPainlessMethod(Class<?> targetClass, Class<?> augmentedClass,
            String methodName, Class<?> returnType, List<Class<?>> typeParameters) {

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
            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] " +
                        "not found for method [[" + targetCanonicalClassName + "], [" + methodName + "], " +
                        typesToCanonicalTypeNames(typeParameters) + "]");
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        if (isValidType(returnType) == false) {
            throw new IllegalArgumentException("return type [" + typeToCanonicalTypeName(returnType) + "] not found for method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
        }

        Method javaMethod;

        if (augmentedClass == null) {
            try {
                javaMethod = targetClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
            } catch (NoSuchMethodException nsme) {
                throw new IllegalArgumentException("reflection object not found for method [[" + targetCanonicalClassName + "], " +
                        "[" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "]", nsme);
            }
        } else {
            try {
                javaMethod = augmentedClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));

                if (Modifier.isStatic(javaMethod.getModifiers()) == false) {
                    throw new IllegalArgumentException("method [[" + targetCanonicalClassName + "], [" + methodName + "], " +
                            typesToCanonicalTypeNames(typeParameters) + "] with augmented class " +
                            "[" + typeToCanonicalTypeName(augmentedClass) + "] must be static");
                }
            } catch (NoSuchMethodException nsme) {
                throw new IllegalArgumentException("reflection object not found for method " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "] " +
                        "with augmented class [" + typeToCanonicalTypeName(augmentedClass) + "]", nsme);
            }
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw new IllegalArgumentException("return type [" + typeToCanonicalTypeName(javaMethod.getReturnType()) + "] " +
                    "does not match the specified returned type [" + typeToCanonicalTypeName(returnType) + "] " +
                    "for method [[" + targetClass.getCanonicalName() + "], [" + methodName + "], " +
                    typesToCanonicalTypeNames(typeParameters) + "]");
        }

        MethodHandle methodHandle;

        if (augmentedClass == null) {
            try {
                methodHandle = MethodHandles.publicLookup().in(targetClass).unreflect(javaMethod);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException("method handle not found for method " +
                        "[[" + targetClass.getCanonicalName() + "], [" + methodName + "], " +
                        typesToCanonicalTypeNames(typeParameters) + "]", iae);
            }
        } else {
            try {
                methodHandle = MethodHandles.publicLookup().in(augmentedClass).unreflect(javaMethod);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException("method handle not found for method " +
                        "[[" + targetClass.getCanonicalName() + "], [" + methodName + "], " +
                        typesToCanonicalTypeNames(typeParameters) + "]" +
                        "with augmented class [" + typeToCanonicalTypeName(augmentedClass) + "]", iae);
            }
        }

        MethodType methodType = methodHandle.type();

        boolean isStatic = augmentedClass == null && Modifier.isStatic(javaMethod.getModifiers());
        String painlessMethodKey = buildPainlessMethodKey(methodName, typeParametersSize);
        PainlessMethod existingPainlessMethod = isStatic ?
                painlessClassBuilder.staticMethods.get(painlessMethodKey) :
                painlessClassBuilder.methods.get(painlessMethodKey);
        PainlessMethod newPainlessMethod =
                new PainlessMethod(javaMethod, targetClass, returnType, typeParameters, methodHandle, methodType);

        if (existingPainlessMethod == null) {
            newPainlessMethod = painlessMethodCache.computeIfAbsent(newPainlessMethod, key -> key);

            if (isStatic) {
                painlessClassBuilder.staticMethods.put(painlessMethodKey, newPainlessMethod);
            } else {
                painlessClassBuilder.methods.put(painlessMethodKey, newPainlessMethod);
            }
        } else if (newPainlessMethod.equals(existingPainlessMethod) == false) {
            throw new IllegalArgumentException("cannot add methods with the same name and arity but are not equivalent for methods " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " +
                    "[" + typeToCanonicalTypeName(returnType) + "], " +
                    typesToCanonicalTypeNames(typeParameters) + "] and " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " +
                    "[" + typeToCanonicalTypeName(existingPainlessMethod.returnType) + "], " +
                    typesToCanonicalTypeNames(existingPainlessMethod.typeParameters) + "]");
        }
    }

    public void addPainlessField(String targetCanonicalClassName, String fieldName, String canonicalTypeNameParameter) {
        Objects.requireNonNull(targetCanonicalClassName);
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(canonicalTypeNameParameter);

        Class<?> targetClass = canonicalClassNamesToClasses.get(targetCanonicalClassName);

        if (targetClass == null) {
            throw new IllegalArgumentException("target class [" + targetCanonicalClassName + "] not found for field " +
                    "[[" + targetCanonicalClassName + "], [" + fieldName + "], [" + canonicalTypeNameParameter + "]]");
        }

        Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

        if (typeParameter == null) {
            throw new IllegalArgumentException("type parameter [" + canonicalTypeNameParameter + "] not found " +
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
            throw new IllegalArgumentException("target class [" + targetCanonicalClassName + "] not found for field " +
                    "[[" + targetCanonicalClassName + "], [" + fieldName + "], [" + typeToCanonicalTypeName(typeParameter) + "]]");
        }

        if (isValidType(typeParameter) == false) {
            throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] not found " +
                    "for field [[" + targetCanonicalClassName + "], [" + fieldName + "]");
        }

        Field javaField;

        try {
            javaField = targetClass.getField(fieldName);
        } catch (NoSuchFieldException nsme) {
            throw new IllegalArgumentException(
                    "reflection object not found for field [[" + targetCanonicalClassName + "], [" + fieldName + "]", nsme);
        }

        if (javaField.getType() != typeToJavaType(typeParameter)) {
            throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(javaField.getType()) + "] " +
                    "does not match the specified type parameter [" + typeToCanonicalTypeName(typeParameter) + "] " +
                    "for field [[" + targetCanonicalClassName + "], [" + fieldName + "]");
        }

        MethodHandle methodHandleGetter;

        try {
            methodHandleGetter = MethodHandles.publicLookup().unreflectGetter(javaField);
        } catch (IllegalAccessException iae) {
            throw new IllegalArgumentException(
                    "getter method handle not found for field [[" + targetCanonicalClassName + "], [" + fieldName + "]]");
        }

        String painlessFieldKey = buildPainlessFieldKey(fieldName);

        if (Modifier.isStatic(javaField.getModifiers())) {
            if (Modifier.isFinal(javaField.getModifiers()) == false) {
                throw new IllegalArgumentException("static field [[" + targetCanonicalClassName + "], [" + fieldName + "]] must be final");
            }

            PainlessField existingPainlessField = painlessClassBuilder.staticFields.get(painlessFieldKey);
            PainlessField newPainlessField = new PainlessField(javaField, typeParameter, methodHandleGetter, null);

            if (existingPainlessField == null) {
                newPainlessField = painlessFieldCache.computeIfAbsent(newPainlessField, key -> key);
                painlessClassBuilder.staticFields.put(painlessFieldKey, newPainlessField);
            } else if (newPainlessField.equals(existingPainlessField) == false) {
                throw new IllegalArgumentException("cannot add fields with the same name but are not equivalent for fields " +
                        "[[" + targetCanonicalClassName + "], [" + fieldName + "], [" +
                        typeToCanonicalTypeName(typeParameter) + "] and " +
                        "[[" + targetCanonicalClassName + "], [" + existingPainlessField.javaField.getName() + "], " +
                        typeToCanonicalTypeName(existingPainlessField.typeParameter) + "] " +
                        "with the same name and different type parameters");
            }
        } else {
            MethodHandle methodHandleSetter;

            try {
                methodHandleSetter = MethodHandles.publicLookup().unreflectSetter(javaField);
            } catch (IllegalAccessException iae) {
                throw new IllegalArgumentException(
                        "setter method handle not found for field [[" + targetCanonicalClassName + "], [" + fieldName + "]]");
            }

            PainlessField existingPainlessField = painlessClassBuilder.fields.get(painlessFieldKey);
            PainlessField newPainlessField = new PainlessField(javaField, typeParameter, methodHandleGetter, methodHandleSetter);

            if (existingPainlessField == null) {
                newPainlessField = painlessFieldCache.computeIfAbsent(newPainlessField, key -> key);
                painlessClassBuilder.fields.put(painlessFieldKey, newPainlessField);
            } else if (newPainlessField.equals(existingPainlessField) == false) {
                throw new IllegalArgumentException("cannot add fields with the same name but are not equivalent for fields " +
                        "[[" + targetCanonicalClassName + "], [" + fieldName + "], [" +
                        typeToCanonicalTypeName(typeParameter) + "] and " +
                        "[[" + targetCanonicalClassName + "], [" + existingPainlessField.javaField.getName() + "], " +
                        typeToCanonicalTypeName(existingPainlessField.typeParameter) + "] " +
                        "with the same name and different type parameters");
            }
        }
    }

    public void addImportedPainlessMethod(ClassLoader classLoader, String targetJavaClassName,
            String methodName, String returnCanonicalTypeName, List<String> canonicalTypeNameParameters) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetJavaClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass;

        try {
            targetClass = Class.forName(targetJavaClassName, true, classLoader);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("class [" + targetJavaClassName + "] not found", cnfe);
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        if (targetClass == null) {
            throw new IllegalArgumentException("target class [" + targetCanonicalClassName + "] not found for imported method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + canonicalTypeNameParameters + "]");
        }

        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw new IllegalArgumentException("type parameter [" + canonicalTypeNameParameter + "] not found for imported method " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " + canonicalTypeNameParameters + "]");
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw new IllegalArgumentException("return type [" + returnCanonicalTypeName + "] not found for imported method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + canonicalTypeNameParameters + "]");
        }

        addImportedPainlessMethod(targetClass, methodName, returnType, typeParameters);
    }

    public void addImportedPainlessMethod(Class<?> targetClass, String methodName, Class<?> returnType, List<Class<?>> typeParameters) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add imported method from reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                    "invalid imported method name [" + methodName + "] for target class [" + targetCanonicalClassName + "].");
        }

        PainlessClassBuilder painlessClassBuilder = classesToPainlessClassBuilders.get(targetClass);

        if (painlessClassBuilder == null) {
            throw new IllegalArgumentException("target class [" + targetCanonicalClassName + "] not found for imported method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
        }

        int typeParametersSize = typeParameters.size();
        List<Class<?>> javaTypeParameters = new ArrayList<>(typeParametersSize);

        for (Class<?> typeParameter : typeParameters) {
            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] " +
                        "not found for imported method [[" + targetCanonicalClassName + "], [" + methodName + "], " +
                        typesToCanonicalTypeNames(typeParameters) + "]");
            }

            javaTypeParameters.add(typeToJavaType(typeParameter));
        }

        if (isValidType(returnType) == false) {
            throw new IllegalArgumentException("return type [" + typeToCanonicalTypeName(returnType) + "] not found for imported method " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
        }

        Method javaMethod;

        try {
            javaMethod = targetClass.getMethod(methodName, javaTypeParameters.toArray(new Class<?>[typeParametersSize]));
        } catch (NoSuchMethodException nsme) {
            throw new IllegalArgumentException("imported method reflection object [[" + targetCanonicalClassName + "], " +
                    "[" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "] not found", nsme);
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw new IllegalArgumentException("return type [" + typeToCanonicalTypeName(javaMethod.getReturnType()) + "] " +
                    "does not match the specified returned type [" + typeToCanonicalTypeName(returnType) + "] " +
                    "for imported method [[" + targetClass.getCanonicalName() + "], [" + methodName + "], " +
                    typesToCanonicalTypeNames(typeParameters) + "]");
        }

        if (Modifier.isStatic(javaMethod.getModifiers()) == false) {
            throw new IllegalArgumentException("imported method [[" + targetClass.getCanonicalName() + "], [" + methodName + "], " +
                    typesToCanonicalTypeNames(typeParameters) + "] must be static");
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, typeParametersSize);

        if (painlessMethodKeysToPainlessClassBindings.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("imported method and class binding cannot have the same name [" + methodName + "]");
        }

        MethodHandle methodHandle;

        try {
            methodHandle = MethodHandles.publicLookup().in(targetClass).unreflect(javaMethod);
        } catch (IllegalAccessException iae) {
            throw new IllegalArgumentException("imported method handle [[" + targetClass.getCanonicalName() + "], " +
                    "[" + methodName + "], " + typesToCanonicalTypeNames(typeParameters) + "] not found", iae);
        }

        MethodType methodType = methodHandle.type();

        PainlessMethod existingImportedPainlessMethod = painlessMethodKeysToImportedPainlessMethods.get(painlessMethodKey);
        PainlessMethod newImportedPainlessMethod =
                new PainlessMethod(javaMethod, targetClass, returnType, typeParameters, methodHandle, methodType);

        if (existingImportedPainlessMethod == null) {
            newImportedPainlessMethod = painlessMethodCache.computeIfAbsent(newImportedPainlessMethod, key -> key);
            painlessMethodKeysToImportedPainlessMethods.put(painlessMethodKey, newImportedPainlessMethod);
        } else if (newImportedPainlessMethod.equals(existingImportedPainlessMethod) == false) {
            throw new IllegalArgumentException("cannot add imported methods with the same name and arity " +
                    "but are not equivalent for methods " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " +
                    "[" + typeToCanonicalTypeName(returnType) + "], " +
                    typesToCanonicalTypeNames(typeParameters) + "] and " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " +
                    "[" + typeToCanonicalTypeName(existingImportedPainlessMethod.returnType) + "], " +
                    typesToCanonicalTypeNames(existingImportedPainlessMethod.typeParameters) + "]");
        }
    }

    public void addPainlessClassBinding(ClassLoader classLoader, String targetJavaClassName,
            String methodName, String returnCanonicalTypeName, List<String> canonicalTypeNameParameters) {

        Objects.requireNonNull(classLoader);
        Objects.requireNonNull(targetJavaClassName);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnCanonicalTypeName);
        Objects.requireNonNull(canonicalTypeNameParameters);

        Class<?> targetClass;

        try {
            targetClass = Class.forName(targetJavaClassName, true, classLoader);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("class [" + targetJavaClassName + "] not found", cnfe);
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);
        List<Class<?>> typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());

        for (String canonicalTypeNameParameter : canonicalTypeNameParameters) {
            Class<?> typeParameter = canonicalTypeNameToType(canonicalTypeNameParameter);

            if (typeParameter == null) {
                throw new IllegalArgumentException("type parameter [" + canonicalTypeNameParameter + "] not found for class binding " +
                        "[[" + targetCanonicalClassName + "], [" + methodName + "], " + canonicalTypeNameParameters + "]");
            }

            typeParameters.add(typeParameter);
        }

        Class<?> returnType = canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw new IllegalArgumentException("return type [" + returnCanonicalTypeName + "] not found for class binding " +
                    "[[" + targetCanonicalClassName + "], [" + methodName + "], " + canonicalTypeNameParameters + "]");
        }

        addPainlessClassBinding(targetClass, methodName, returnType, typeParameters);
    }

    public void addPainlessClassBinding(Class<?> targetClass, String methodName, Class<?> returnType, List<Class<?>> typeParameters) {
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(methodName);
        Objects.requireNonNull(returnType);
        Objects.requireNonNull(typeParameters);

        if (targetClass == def.class) {
            throw new IllegalArgumentException("cannot add class binding as reserved class [" + DEF_CLASS_NAME + "]");
        }

        String targetCanonicalClassName = typeToCanonicalTypeName(targetClass);

        Constructor<?>[] javaConstructors = targetClass.getConstructors();
        Constructor<?> javaConstructor = null;

        for (Constructor<?> eachJavaConstructor : javaConstructors) {
            if (eachJavaConstructor.getDeclaringClass() == targetClass) {
                if (javaConstructor != null) {
                    throw new IllegalArgumentException(
                            "class binding [" + targetCanonicalClassName + "] cannot have multiple constructors");
                }

                javaConstructor = eachJavaConstructor;
            }
        }

        if (javaConstructor == null) {
            throw new IllegalArgumentException("class binding [" + targetCanonicalClassName + "] must have exactly one constructor");
        }

        int constructorTypeParametersSize = javaConstructor.getParameterCount();

        for (int typeParameterIndex = 0; typeParameterIndex < constructorTypeParametersSize; ++typeParameterIndex) {
            Class<?> typeParameter = typeParameters.get(typeParameterIndex);

            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] not found " +
                        "for class binding [[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
            }

            Class<?> javaTypeParameter = javaConstructor.getParameterTypes()[typeParameterIndex];

            if (isValidType(javaTypeParameter) == false) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] not found " +
                        "for class binding [[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
            }

            if (javaTypeParameter != typeToJavaType(typeParameter)) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(javaTypeParameter) + "] " +
                        "does not match the specified type parameter [" + typeToCanonicalTypeName(typeParameter) + "] " +
                        "for class binding [[" + targetClass.getCanonicalName() + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
            }
        }

        if (METHOD_NAME_PATTERN.matcher(methodName).matches() == false) {
            throw new IllegalArgumentException(
                    "invalid method name [" + methodName + "] for class binding [" + targetCanonicalClassName + "].");
        }

        Method[] javaMethods = targetClass.getMethods();
        Method javaMethod = null;

        for (Method eachJavaMethod : javaMethods) {
            if (eachJavaMethod.getDeclaringClass() == targetClass) {
                if (javaMethod != null) {
                    throw new IllegalArgumentException("class binding [" + targetCanonicalClassName + "] cannot have multiple methods");
                }

                javaMethod = eachJavaMethod;
            }
        }

        if (javaMethod == null) {
            throw new IllegalArgumentException("class binding [" + targetCanonicalClassName + "] must have exactly one method");
        }

        int methodTypeParametersSize = javaMethod.getParameterCount();

        for (int typeParameterIndex = 0; typeParameterIndex < methodTypeParametersSize; ++typeParameterIndex) {
            Class<?> typeParameter = typeParameters.get(constructorTypeParametersSize + typeParameterIndex);

            if (isValidType(typeParameter) == false) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] not found " +
                        "for class binding [[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
            }

            Class<?> javaTypeParameter = javaMethod.getParameterTypes()[typeParameterIndex];

            if (isValidType(javaTypeParameter) == false) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(typeParameter) + "] not found " +
                        "for class binding [[" + targetCanonicalClassName + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
            }

            if (javaTypeParameter != typeToJavaType(typeParameter)) {
                throw new IllegalArgumentException("type parameter [" + typeToCanonicalTypeName(javaTypeParameter) + "] " +
                        "does not match the specified type parameter [" + typeToCanonicalTypeName(typeParameter) + "] " +
                        "for class binding [[" + targetClass.getCanonicalName() + "], " + typesToCanonicalTypeNames(typeParameters) + "]");
            }
        }

        if (javaMethod.getReturnType() != typeToJavaType(returnType)) {
            throw new IllegalArgumentException("return type [" + typeToCanonicalTypeName(javaMethod.getReturnType()) + "] " +
                    "does not match the specified returned type [" + typeToCanonicalTypeName(returnType) + "] " +
                    "for class binding [[" + targetClass.getCanonicalName() + "], [" + methodName + "], " +
                    typesToCanonicalTypeNames(typeParameters) + "]");
        }

        String painlessMethodKey = buildPainlessMethodKey(methodName, constructorTypeParametersSize + methodTypeParametersSize);

        if (painlessMethodKeysToImportedPainlessMethods.containsKey(painlessMethodKey)) {
            throw new IllegalArgumentException("class binding and imported method cannot have the same name [" + methodName + "]");
        }

        PainlessClassBinding existingPainlessClassBinding = painlessMethodKeysToPainlessClassBindings.get(painlessMethodKey);
        PainlessClassBinding newPainlessClassBinding =
                new PainlessClassBinding(javaConstructor, javaMethod, returnType, typeParameters);

        if (existingPainlessClassBinding == null) {
            newPainlessClassBinding = painlessClassBindingCache.computeIfAbsent(newPainlessClassBinding, key -> key);
            painlessMethodKeysToPainlessClassBindings.put(painlessMethodKey, newPainlessClassBinding);
        } else if (newPainlessClassBinding.equals(existingPainlessClassBinding)) {
            throw new IllegalArgumentException("cannot add class bindings with the same name and arity " +
                    "but are not equivalent for methods " +
                    "[[" + targetCanonicalClassName + "], " +
                    "[" + methodName + "], " +
                    "[" + typeToCanonicalTypeName(returnType) + "], " +
                    typesToCanonicalTypeNames(typeParameters) + "] and " +
                    "[[" + targetCanonicalClassName + "], " +
                    "[" + methodName + "], " +
                    "[" + typeToCanonicalTypeName(existingPainlessClassBinding.returnType) + "], " +
                    typesToCanonicalTypeNames(existingPainlessClassBinding.typeParameters) + "]");
        }
    }

    public PainlessLookup build() {
        copyPainlessClassMembers();
        cacheRuntimeHandles();
        setFunctionalInterfaceMethods();

        Map<Class<?>, PainlessClass> classesToPainlessClasses = new HashMap<>(classesToPainlessClassBuilders.size());

        for (Map.Entry<Class<?>, PainlessClassBuilder> painlessClassBuilderEntry : classesToPainlessClassBuilders.entrySet()) {
            classesToPainlessClasses.put(painlessClassBuilderEntry.getKey(), painlessClassBuilderEntry.getValue().build());
        }

        return new PainlessLookup(canonicalClassNamesToClasses, classesToPainlessClasses,
                painlessMethodKeysToImportedPainlessMethods, painlessMethodKeysToPainlessClassBindings);
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

            if (existingPainlessMethod == null || existingPainlessMethod.targetClass != newPainlessMethod.targetClass &&
                    existingPainlessMethod.targetClass.isAssignableFrom(newPainlessMethod.targetClass)) {
                targetPainlessClassBuilder.methods.put(painlessMethodKey, newPainlessMethod);
            }
        }

        for (Map.Entry<String, PainlessField> painlessFieldEntry : originalPainlessClassBuilder.fields.entrySet()) {
            String painlessFieldKey = painlessFieldEntry.getKey();
            PainlessField newPainlessField = painlessFieldEntry.getValue();
            PainlessField existingPainlessField = targetPainlessClassBuilder.fields.get(painlessFieldKey);

            if (existingPainlessField == null ||
                    existingPainlessField.javaField.getDeclaringClass() != newPainlessField.javaField.getDeclaringClass() &&
                    existingPainlessField.javaField.getDeclaringClass().isAssignableFrom(newPainlessField.javaField.getDeclaringClass())) {
                targetPainlessClassBuilder.fields.put(painlessFieldKey, newPainlessField);
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
            String methodName = painlessMethod.javaMethod.getName();
            int typeParametersSize = painlessMethod.typeParameters.size();

            if (typeParametersSize == 0 && methodName.startsWith("get") && methodName.length() > 3 &&
                    Character.isUpperCase(methodName.charAt(3))) {
                painlessClassBuilder.getterMethodHandles.putIfAbsent(
                        Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4), painlessMethod.methodHandle);
            } else if (typeParametersSize == 0 && methodName.startsWith("is") && methodName.length() > 2 &&
                    Character.isUpperCase(methodName.charAt(2))) {
                painlessClassBuilder.getterMethodHandles.putIfAbsent(
                        Character.toLowerCase(methodName.charAt(2)) + methodName.substring(3), painlessMethod.methodHandle);
            } else if (typeParametersSize == 1 && methodName.startsWith("set") && methodName.length() > 3 &&
                    Character.isUpperCase(methodName.charAt(3))) {
                painlessClassBuilder.setterMethodHandles.putIfAbsent(
                        Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4), painlessMethod.methodHandle);
            }
        }

        for (PainlessField painlessField : painlessClassBuilder.fields.values()) {
            painlessClassBuilder.getterMethodHandles.put(painlessField.javaField.getName(), painlessField.getterMethodHandle);
            painlessClassBuilder.setterMethodHandles.put(painlessField.javaField.getName(), painlessField.setterMethodHandle);
        }
    }

    private void setFunctionalInterfaceMethods() {
        for (Map.Entry<Class<?>, PainlessClassBuilder> painlessClassBuilderEntry : classesToPainlessClassBuilders.entrySet()) {
            setFunctionalInterfaceMethod(painlessClassBuilderEntry.getKey(), painlessClassBuilderEntry.getValue());
        }
    }

    private void setFunctionalInterfaceMethod(Class<?> targetClass, PainlessClassBuilder painlessClassBuilder) {
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
                painlessClassBuilder.functionalInterfaceMethod = painlessClassBuilder.methods.get(painlessMethodKey);
            }
        }
    }
}
