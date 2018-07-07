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

    public void addClass(ClassLoader classLoader, String javaClassName, boolean noImport) {
        String painlessClassName = javaClassName.replace('$', '.');
        String importedPainlessClassName = painlessClassName;

        if (CLASS_NAME_PATTERN.matcher(painlessClassName).matches() == false) {
            throw new IllegalArgumentException("invalid painless class name [" + painlessClassName + "]");
        }

        int index = javaClassName.lastIndexOf('.');

        if (index != -1) {
            importedPainlessClassName = javaClassName.substring(index + 1).replace('$', '.');
        }

        Class<?> javaClass;

        if      ("void"   .equals(javaClassName)) javaClass = void.class;
        else if ("boolean".equals(javaClassName)) javaClass = boolean.class;
        else if ("byte"   .equals(javaClassName)) javaClass = byte.class;
        else if ("short"  .equals(javaClassName)) javaClass = short.class;
        else if ("char"   .equals(javaClassName)) javaClass = char.class;
        else if ("int"    .equals(javaClassName)) javaClass = int.class;
        else if ("long"   .equals(javaClassName)) javaClass = long.class;
        else if ("float"  .equals(javaClassName)) javaClass = float.class;
        else if ("double" .equals(javaClassName)) javaClass = double.class;
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

        PainlessClass existingPainlessClass = javaClassesToPainlessClasses.get(javaClass);

        if (existingPainlessClass == null) {
            PainlessClass painlessClass = new PainlessClass(painlessClassName, javaClass, Type.getType(javaClass));
            painlessClassNamesToJavaClasses.put(painlessClassName, javaClass);
            javaClassesToPainlessClasses.put(javaClass, painlessClass);
        } else if (existingPainlessClass.clazz.equals(javaClass) == false) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] illegally represents " +
                    "multiple java classes [" + javaClass.getName() + "] and [" + existingPainlessClass.clazz.getName() + "]");
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
                    if (existingPainlessClass != null) {
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

    public void addConstructor(String painlessClassName, List<String> painlessTypeNameParameters) {
        Class<?> javaClass = painlessClassNamesToJavaClasses.get(painlessClassName);

        if (javaClass == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        PainlessClass painlessClass = javaClassesToPainlessClasses.get(javaClass);

        if (painlessClass == null) {
            throw new IllegalArgumentException("painless class [" + painlessClassName + "] not found");
        }

        int painlessTypeParametersSize = painlessTypeNameParameters.size();
        List<Class<?>> painlessTypeParameters = new ArrayList<>(painlessTypeParametersSize);
        Class<?>[] javaTypeParameters = new Class<?>[painlessTypeParametersSize];

        for (int parameterIndex = 0; parameterIndex < painlessTypeNameParameters.size(); ++parameterIndex) {
            String painlessTypeNameParameter = painlessTypeNameParameters.get(parameterIndex);

            try {
                Class<?> painlessTypeParameter = painlessTypeNameToPainlessType(painlessTypeNameParameter);

                painlessTypeParameters.add(painlessTypeParameter);
                javaTypeParameters[parameterIndex] = painlessTypeToJavaType(painlessTypeParameter);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException("painless type [" + painlessTypeNameParameter + "] not found " +
                    "for constructor [[" + painlessClassName + "], " + painlessTypeNameParameters + "]", iae);
            }
        }

        Constructor<?> javaConstructor;

        try {
            javaConstructor = painlessClass.clazz.getConstructor(javaTypeParameters);
        } catch (NoSuchMethodException exception) {
            throw new IllegalArgumentException(
                    "java constructor [[" + painlessClass.clazz.getName() + "], " + Arrays.asList(javaTypeParameters) + "] not found");
        }

        String painlessMethodKey = buildPainlessMethodKey("<init>", painlessTypeParametersSize);
        PainlessMethod painlessConstructor = painlessClass.constructors.get(painlessMethodKey);

        if (painlessConstructor == null) {
            Method asmConstructor = Method.getMethod(javaConstructor);
            MethodHandle javaMethodHandle;

            try {
                javaMethodHandle = MethodHandles.publicLookup().in(painlessClass.clazz).unreflectConstructor(javaConstructor);
            } catch (IllegalAccessException exception) {
                throw new IllegalArgumentException(
                        "java constructor [[" + painlessClass.clazz.getName() + "], " + Arrays.asList(javaTypeParameters) + "] not found");
            }

            painlessConstructor = painlessMethodCache.computeIfAbsent(
                    new PainlessMethodCacheKey(javaClass, "<init>", painlessTypeParameters),
                    key -> new PainlessMethod("<init>", painlessClass, null, void.class, painlessTypeParameters,
                                              asmConstructor, javaConstructor.getModifiers(), javaMethodHandle)
            );

            painlessClass.constructors.put(painlessMethodKey, painlessConstructor);
        } else if (painlessConstructor.arguments.equals(painlessTypeParameters) == false){
            throw new IllegalArgumentException(
                    "illegal duplicate constructors for painless class [" + painlessClassName + "] " +
                    "with parameters " + painlessTypeParameters + " and " + painlessConstructor.arguments);
        }
    }
}
