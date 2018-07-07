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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class PainlessLookupBase {

    static String buildPainlessMethodKey(String methodName, int methodArity) {
        return methodName + "/" + methodArity;
    }

    static Class<?> javaTypeToPainlessType(Class<?> clazz) {
        if (clazz.isArray()) {
            Class<?> component = clazz.getComponentType();
            int dimensions = 1;

            while (component.isArray()) {
                component = component.getComponentType();
                ++dimensions;
            }

            if (component == Object.class) {
                char[] braces = new char[dimensions];
                Arrays.fill(braces, '[');

                String descriptor = new String(braces) + org.objectweb.asm.Type.getType(def.class).getDescriptor();
                org.objectweb.asm.Type type = org.objectweb.asm.Type.getType(descriptor);

                try {
                    return Class.forName(type.getInternalName().replace('/', '.'));
                } catch (ClassNotFoundException exception) {
                    throw new IllegalStateException("internal error", exception);
                }
            }
        } else if (clazz == Object.class) {
            return def.class;
        }

        return clazz;
    }

    static Class<?> painlessTypeToJavaType(Class<?> clazz) {
        if (clazz.isArray()) {
            Class<?> component = clazz.getComponentType();
            int dimensions = 1;

            while (component.isArray()) {
                component = component.getComponentType();
                ++dimensions;
            }

            if (component == def.class) {
                char[] braces = new char[dimensions];
                Arrays.fill(braces, '[');

                String descriptor = new String(braces) + org.objectweb.asm.Type.getType(Object.class).getDescriptor();
                org.objectweb.asm.Type type = org.objectweb.asm.Type.getType(descriptor);

                try {
                    return Class.forName(type.getInternalName().replace('/', '.'));
                } catch (ClassNotFoundException exception) {
                    throw new IllegalStateException("internal error", exception);
                }
            }
        } else if (clazz == def.class) {
            return Object.class;
        }

        return clazz;
    }

    final Map<String, Class<?>> painlessClassNamesToJavaClasses;
    final Map<Class<?>, PainlessClass> javaClassesToPainlessClasses;

    PainlessLookupBase() {
        painlessClassNamesToJavaClasses = new HashMap<>();
        javaClassesToPainlessClasses = new HashMap<>();
    }

    Class<?> painlessTypeNameToPainlessType(String painlessTypeName) {
        Class<?> javaClass = painlessClassNamesToJavaClasses.get(painlessTypeName);

        if (javaClass != null) {
            return javaClass;
        }

        int arrayDimensions = 0;
        int arrayIndex = painlessTypeName.indexOf('[');

        if (arrayIndex != -1) {
            int length = painlessTypeName.length();

            while (arrayIndex < length) {
                if (painlessTypeName.charAt(arrayIndex) == '[' && ++arrayIndex < length && painlessTypeName.charAt(arrayIndex++) == ']') {
                    ++arrayDimensions;
                } else {
                    throw new IllegalArgumentException("invalid painless type [" + painlessTypeName + "].");
                }
            }

            painlessTypeName = painlessTypeName.substring(0, painlessTypeName.indexOf('['));
            javaClass = painlessClassNamesToJavaClasses.get(painlessTypeName);

            char braces[] = new char[arrayDimensions];
            Arrays.fill(braces, '[');
            String descriptor = new String(braces);

            if (javaClass == boolean.class) {
                descriptor += "Z";
            } else if (javaClass == byte.class) {
                descriptor += "B";
            } else if (javaClass == short.class) {
                descriptor += "S";
            } else if (javaClass == char.class) {
                descriptor += "C";
            } else if (javaClass == int.class) {
                descriptor += "I";
            } else if (javaClass == long.class) {
                descriptor += "J";
            } else if (javaClass == float.class) {
                descriptor += "F";
            } else if (javaClass == double.class) {
                descriptor += "D";
            } else {
                descriptor += "L" + javaClass.getName() + ";";
            }

            try {
                return Class.forName(descriptor);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalStateException("painless type [" + painlessTypeName + "] not found", cnfe);
            }
        }

        throw new IllegalArgumentException("painless type [" + painlessTypeName + "] not found");
    }
}
