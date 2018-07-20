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
import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.regex.Pattern;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.DEF_TYPE_NAME;
import static org.elasticsearch.painless.lookup.PainlessLookupUtility.buildPainlessMethodKey;

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
    private final Map<Class<?>, PainlessClassBuilder> classesToPainlessClasses;

    public PainlessLookupBuilder(List<Whitelist> whitelists) {
        this.whitelists = whitelists;

        canonicalClassNamesToClasses = new HashMap<>();
        classesToPainlessClasses = new HashMap<>();

        canonicalClassNamesToClasses.put(DEF_TYPE_NAME, def.class);
        classesToPainlessClasses.put(def.class,
                new PainlessClassBuilder(DEF_TYPE_NAME, Object.class, Type.getType(Object.class)));
    }

    private Class<?> canonicalTypeNameToType(String canonicalTypeName) {
        return PainlessLookupUtility.canonicalTypeNameToType(canonicalTypeName, canonicalClassNamesToClasses);
    }

    private void validateType(Class<?> type) {
        PainlessLookupUtility.validateType(type, classesToPainlessClasses.keySet());
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
            throw new IllegalArgumentException("cannot add reserved class [" + DEF_TYPE_NAME + "]");
        }

        String canonicalClassName = clazz.getCanonicalName();

        if (clazz.isArray()) {
            throw new IllegalArgumentException("cannot add array type [" + canonicalClassName + "] as a class");
        }

        if (CLASS_NAME_PATTERN.matcher(canonicalClassName).matches() == false) {
            throw new IllegalArgumentException("invalid class name [" + canonicalClassName + "]");
        }

        PainlessClassBuilder existingPainlessClassBuilder = classesToPainlessClasses.get(clazz);

        if (existingPainlessClassBuilder == null) {
            PainlessClassBuilder painlessClassBuilder = new PainlessClassBuilder(canonicalClassName, clazz, Type.getType(clazz));

            canonicalClassNamesToClasses.put(canonicalClassName, clazz);
            classesToPainlessClasses.put(clazz, painlessClassBuilder);
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
            Class<?> importedPainlessType = canonicalClassNamesToClasses.get(importedCanonicalClassName);

            if (importedPainlessType == null) {
                if (importClassName) {
                    if (existingPainlessClassBuilder != null) {
                        throw new IllegalArgumentException(
                                "inconsistent only_fqn parameters found for painless type [" + canonicalClassName + "]");
                    }

                    canonicalClassNamesToClasses.put(importedCanonicalClassName, clazz);
                }
            } else if (importedPainlessType.equals(clazz) == false) {
                throw new IllegalArgumentException("painless type [" + importedCanonicalClassName + "] illegally represents multiple " +
                        "java types [" + clazz.getCanonicalName() + "] and [" + importedPainlessType.getCanonicalName() + "]");
            } else if (importClassName == false) {
                throw new IllegalArgumentException("inconsistent only_fqn parameters found for painless type [" + canonicalClassName + "]");
            }
        }
    }

    private void addConstructor(String ownerStructName, WhitelistConstructor whitelistConstructor) {
        PainlessClassBuilder ownerStruct = classesToPainlessClasses.get(canonicalClassNamesToClasses.get(ownerStructName));

        if (ownerStruct == null) {
            throw new IllegalArgumentException("owner struct [" + ownerStructName + "] not defined for constructor with " +
                "parameters " + whitelistConstructor.painlessParameterTypeNames);
        }

        List<Class<?>> painlessParametersTypes = new ArrayList<>(whitelistConstructor.painlessParameterTypeNames.size());
        Class<?>[] javaClassParameters = new Class<?>[whitelistConstructor.painlessParameterTypeNames.size()];

        for (int parameterCount = 0; parameterCount < whitelistConstructor.painlessParameterTypeNames.size(); ++parameterCount) {
            String painlessParameterTypeName = whitelistConstructor.painlessParameterTypeNames.get(parameterCount);

            try {
                Class<?> painlessParameterClass = canonicalTypeNameToType(painlessParameterTypeName);

                painlessParametersTypes.add(painlessParameterClass);
                javaClassParameters[parameterCount] = PainlessLookupUtility.typeToJavaType(painlessParameterClass);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("struct not defined for constructor parameter [" + painlessParameterTypeName + "] " +
                    "with owner struct [" + ownerStructName + "] and constructor parameters " +
                    whitelistConstructor.painlessParameterTypeNames, iae);
            }
        }

        java.lang.reflect.Constructor<?> javaConstructor;

        try {
            javaConstructor = ownerStruct.clazz.getConstructor(javaClassParameters);
        } catch (NoSuchMethodException exception) {
            throw new IllegalArgumentException("constructor not defined for owner struct [" + ownerStructName + "] " +
                " with constructor parameters " + whitelistConstructor.painlessParameterTypeNames, exception);
        }

        String painlessMethodKey = buildPainlessMethodKey("<init>", whitelistConstructor.painlessParameterTypeNames.size());
        PainlessMethod painlessConstructor = ownerStruct.constructors.get(painlessMethodKey);

        if (painlessConstructor == null) {
            org.objectweb.asm.commons.Method asmConstructor = org.objectweb.asm.commons.Method.getMethod(javaConstructor);
            MethodHandle javaHandle;

            try {
                javaHandle = MethodHandles.publicLookup().in(ownerStruct.clazz).unreflectConstructor(javaConstructor);
            } catch (IllegalAccessException exception) {
                throw new IllegalArgumentException("constructor not defined for owner struct [" + ownerStructName + "] " +
                    " with constructor parameters " + whitelistConstructor.painlessParameterTypeNames);
            }

            painlessConstructor = painlessMethodCache.computeIfAbsent(
                new PainlessMethodCacheKey(ownerStruct.clazz, "<init>", painlessParametersTypes),
                key -> new PainlessMethod("<init>", ownerStruct.clazz, null, void.class, painlessParametersTypes,
                    asmConstructor, javaConstructor.getModifiers(), javaHandle));
            ownerStruct.constructors.put(painlessMethodKey, painlessConstructor);
        } else if (painlessConstructor.arguments.equals(painlessParametersTypes) == false){
            throw new IllegalArgumentException(
                "illegal duplicate constructors [" + painlessMethodKey + "] found within the struct [" + ownerStruct.name + "] " +
                    "with parameters " + painlessParametersTypes + " and " + painlessConstructor.arguments);
        }
    }

    private void addMethod(ClassLoader whitelistClassLoader, String ownerStructName, WhitelistMethod whitelistMethod) {
        PainlessClassBuilder ownerStruct = classesToPainlessClasses.get(canonicalClassNamesToClasses.get(ownerStructName));

        if (ownerStruct == null) {
            throw new IllegalArgumentException("owner struct [" + ownerStructName + "] not defined for method with " +
                "name [" + whitelistMethod.javaMethodName + "] and parameters " + whitelistMethod.painlessParameterTypeNames);
        }

        if (METHOD_NAME_PATTERN.matcher(whitelistMethod.javaMethodName).matches() == false) {
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
                Class<?> painlessParameterClass = canonicalTypeNameToType(painlessParameterTypeName);

                painlessParametersTypes.add(painlessParameterClass);
                javaClassParameters[parameterCount + augmentedOffset] =
                        PainlessLookupUtility.typeToJavaType(painlessParameterClass);
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
            painlessReturnClass = canonicalTypeNameToType(whitelistMethod.painlessReturnTypeName);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("struct not defined for return type [" + whitelistMethod.painlessReturnTypeName + "] " +
                "with owner struct [" + ownerStructName + "] and method with name [" + whitelistMethod.javaMethodName + "] " +
                "and parameters " + whitelistMethod.painlessParameterTypeNames, iae);
        }

        if (javaMethod.getReturnType() != PainlessLookupUtility.typeToJavaType(painlessReturnClass)) {
            throw new IllegalArgumentException("specified return type class [" + painlessReturnClass + "] " +
                "does not match the return type class [" + javaMethod.getReturnType() + "] for the " +
                "method with name [" + whitelistMethod.javaMethodName + "] " +
                "and parameters " + whitelistMethod.painlessParameterTypeNames);
        }

        String painlessMethodKey =
            buildPainlessMethodKey(whitelistMethod.javaMethodName, whitelistMethod.painlessParameterTypeNames.size());

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

                painlessMethod = painlessMethodCache.computeIfAbsent(
                    new PainlessMethodCacheKey(ownerStruct.clazz, whitelistMethod.javaMethodName, painlessParametersTypes),
                    key -> new PainlessMethod(whitelistMethod.javaMethodName, ownerStruct.clazz, null, painlessReturnClass,
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

                painlessMethod = painlessMethodCache.computeIfAbsent(
                    new PainlessMethodCacheKey(ownerStruct.clazz, whitelistMethod.javaMethodName, painlessParametersTypes),
                    key -> new PainlessMethod(whitelistMethod.javaMethodName, ownerStruct.clazz, javaAugmentedClass, painlessReturnClass,
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

    private void addField(String ownerStructName, WhitelistField whitelistField) {
        PainlessClassBuilder ownerStruct = classesToPainlessClasses.get(canonicalClassNamesToClasses.get(ownerStructName));

        if (ownerStruct == null) {
            throw new IllegalArgumentException("owner struct [" + ownerStructName + "] not defined for method with " +
                "name [" + whitelistField.javaFieldName + "] and type " + whitelistField.painlessFieldTypeName);
        }

        if (FIELD_NAME_PATTERN.matcher(whitelistField.javaFieldName).matches() == false) {
            throw new IllegalArgumentException("invalid field name " +
                "[" + whitelistField.painlessFieldTypeName + "] for owner struct [" + ownerStructName + "].");
        }

        java.lang.reflect.Field javaField;

        try {
            javaField = ownerStruct.clazz.getField(whitelistField.javaFieldName);
        } catch (NoSuchFieldException exception) {
            throw new IllegalArgumentException("field [" + whitelistField.javaFieldName + "] " +
                "not found for class [" + ownerStruct.clazz.getName() + "].");
        }

        Class<?> painlessFieldClass;

        try {
            painlessFieldClass = canonicalTypeNameToType(whitelistField.painlessFieldTypeName);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("struct not defined for return type [" + whitelistField.painlessFieldTypeName + "] " +
                "with owner struct [" + ownerStructName + "] and field with name [" + whitelistField.javaFieldName + "]", iae);
        }

        if (Modifier.isStatic(javaField.getModifiers())) {
            if (Modifier.isFinal(javaField.getModifiers()) == false) {
                throw new IllegalArgumentException("static [" + whitelistField.javaFieldName + "] " +
                    "with owner struct [" + ownerStruct.name + "] is not final");
            }

            PainlessField painlessField = ownerStruct.staticMembers.get(whitelistField.javaFieldName);

            if (painlessField == null) {
                painlessField = painlessFieldCache.computeIfAbsent(
                    new PainlessFieldCacheKey(ownerStruct.clazz, whitelistField.javaFieldName, painlessFieldClass),
                    key -> new PainlessField(whitelistField.javaFieldName, javaField.getName(),
                        ownerStruct.clazz, painlessFieldClass, javaField.getModifiers(), null, null));
                ownerStruct.staticMembers.put(whitelistField.javaFieldName, painlessField);
            } else if (painlessField.clazz != painlessFieldClass) {
                throw new IllegalArgumentException("illegal duplicate static fields [" + whitelistField.javaFieldName + "] " +
                    "found within the struct [" + ownerStruct.name + "] with type [" + whitelistField.painlessFieldTypeName + "]");
            }
        } else {
            MethodHandle javaMethodHandleGetter;
            MethodHandle javaMethodHandleSetter;

            try {
                if (Modifier.isStatic(javaField.getModifiers()) == false) {
                    javaMethodHandleGetter = MethodHandles.publicLookup().unreflectGetter(javaField);
                    javaMethodHandleSetter = MethodHandles.publicLookup().unreflectSetter(javaField);
                } else {
                    javaMethodHandleGetter = null;
                    javaMethodHandleSetter = null;
                }
            } catch (IllegalAccessException exception) {
                throw new IllegalArgumentException("getter/setter [" + whitelistField.javaFieldName + "]" +
                    " not found for class [" + ownerStruct.clazz.getName() + "].");
            }

            PainlessField painlessField = ownerStruct.members.get(whitelistField.javaFieldName);

            if (painlessField == null) {
                painlessField = painlessFieldCache.computeIfAbsent(
                    new PainlessFieldCacheKey(ownerStruct.clazz, whitelistField.javaFieldName, painlessFieldClass),
                    key -> new PainlessField(whitelistField.javaFieldName, javaField.getName(),
                        ownerStruct.clazz, painlessFieldClass, javaField.getModifiers(), javaMethodHandleGetter, javaMethodHandleSetter));
                ownerStruct.members.put(whitelistField.javaFieldName, painlessField);
            } else if (painlessField.clazz != painlessFieldClass) {
                throw new IllegalArgumentException("illegal duplicate member fields [" + whitelistField.javaFieldName + "] " +
                    "found within the struct [" + ownerStruct.name + "] with type [" + whitelistField.painlessFieldTypeName + "]");
            }
        }
    }

    private void copyStruct(String struct, List<String> children) {
        final PainlessClassBuilder owner = classesToPainlessClasses.get(canonicalClassNamesToClasses.get(struct));

        if (owner == null) {
            throw new IllegalArgumentException("Owner struct [" + struct + "] not defined for copy.");
        }

        for (int count = 0; count < children.size(); ++count) {
            final PainlessClassBuilder child =
                    classesToPainlessClasses.get(canonicalClassNamesToClasses.get(children.get(count)));

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
                        classesToPainlessClasses.get(canonicalClassNamesToClasses.get(painlessTypeName));

                    if (painlessStruct != null && painlessStruct.clazz.getName().equals(whitelistStruct.javaClassName) == false) {
                        throw new IllegalArgumentException("struct [" + painlessStruct.name + "] cannot represent multiple classes " +
                            "[" + painlessStruct.clazz.getName() + "] and [" + whitelistStruct.javaClassName + "]");
                    }

                    origin = whitelistStruct.origin;
                    addPainlessClass(
                            whitelist.javaClassLoader, whitelistStruct.javaClassName, whitelistStruct.onlyFQNJavaClassName == false);

                    painlessStruct = classesToPainlessClasses.get(canonicalClassNamesToClasses.get(painlessTypeName));
                    classesToPainlessClasses.put(painlessStruct.clazz, painlessStruct);
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
        }

        // goes through each Painless struct and determines the inheritance list,
        // and then adds all inherited types to the Painless struct's whitelist
        for (Class<?> javaClass : classesToPainlessClasses.keySet()) {
            PainlessClassBuilder painlessStruct = classesToPainlessClasses.get(javaClass);

            List<String> painlessSuperStructs = new ArrayList<>();
            Class<?> javaSuperClass = painlessStruct.clazz.getSuperclass();

            Stack<Class<?>> javaInteraceLookups = new Stack<>();
            javaInteraceLookups.push(painlessStruct.clazz);

            // adds super classes to the inheritance list
            if (javaSuperClass != null && javaSuperClass.isInterface() == false) {
                while (javaSuperClass != null) {
                    PainlessClassBuilder painlessSuperStruct = classesToPainlessClasses.get(javaSuperClass);

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
                    PainlessClassBuilder painlessInterfaceStruct = classesToPainlessClasses.get(javaSuperInterface);

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
                PainlessClassBuilder painlessObjectStruct = classesToPainlessClasses.get(Object.class);

                if (painlessObjectStruct != null) {
                    copyStruct(painlessStruct.name, Collections.singletonList(painlessObjectStruct.name));
                }
            }
        }

        // precompute runtime classes
        for (PainlessClassBuilder painlessStruct : classesToPainlessClasses.values()) {
            addRuntimeClass(painlessStruct);
        }

        Map<Class<?>, PainlessClass> javaClassesToPainlessClasses = new HashMap<>();

        // copy all structs to make them unmodifiable for outside users:
        for (Map.Entry<Class<?>,PainlessClassBuilder> entry : classesToPainlessClasses.entrySet()) {
            entry.getValue().functionalMethod = computeFunctionalInterfaceMethod(entry.getValue());
            javaClassesToPainlessClasses.put(entry.getKey(), entry.getValue().build());
        }

        return new PainlessLookup(canonicalClassNamesToClasses, javaClassesToPainlessClasses);
    }
}
