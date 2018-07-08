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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PainlessLookupBase {

    // The following terminology is used for variable names throughout the lookup package:
    //
    // - javaClass           (Class)         - a java class including def and excluding array type java classes
    // - javaClassName       (String)        - the fully qualified java class name for a javaClass
    // - painlessClassName   (String)        - the fully qualified painless name or imported painless name for a painlessClass
    // - anyClassName        (String)        - either a javaClassName or a painlessClassName
    // - javaType            (Class)         - a java class excluding def and array type java classes
    // - painlessType        (Class)         - a java class including def and array type java classes
    // - javaTypeName        (String)        - the fully qualified java Type name for a javaType
    // - painlessTypeName    (String)        - the fully qualified painless name or imported painless name for a painlessType
    // - anyTypeName         (String)        - either a javaTypeName or a painlessTypeName
    // - painlessClass       (PainlessClass) - a painless class object
    //
    // Under ambiguous circumstances most variable names are prefixed with asm, java, or painless.
    // If the variable name is the same for asm, java, and painless, no prefix is used.

    public static Class<?> javaTypeToPainlessType(Class<?> javaType) {
        if (javaType.isArray()) {
            Class<?> javaTypeComponent = javaType.getComponentType();
            int arrayDimensions = 1;

            while (javaTypeComponent.isArray()) {
                javaTypeComponent = javaTypeComponent.getComponentType();
                ++arrayDimensions;
            }

            if (javaTypeComponent == Object.class) {
                char[] asmDescriptorBraces = new char[arrayDimensions];
                Arrays.fill(asmDescriptorBraces, '[');

                String asmDescriptor = new String(asmDescriptorBraces) + Type.getType(def.class).getDescriptor();
                Type asmType = Type.getType(asmDescriptor);

                try {
                    return Class.forName(asmType.getInternalName().replace('/', '.'));
                } catch (ClassNotFoundException cnfe) {
                    throw new IllegalStateException("internal error", cnfe);
                }
            }
        } else if (javaType == Object.class) {
            return def.class;
        }

        return javaType;
    }

    public static Class<?> painlessTypeToJavaType(Class<?> painlessType) {
        if (painlessType.isArray()) {
            Class<?> painlessTypeComponent = painlessType.getComponentType();
            int arrayDimensions = 1;

            while (painlessTypeComponent.isArray()) {
                painlessTypeComponent = painlessTypeComponent.getComponentType();
                ++arrayDimensions;
            }

            if (painlessTypeComponent == def.class) {
                char[] asmDescriptorBraces = new char[arrayDimensions];
                Arrays.fill(asmDescriptorBraces, '[');

                String asmDescriptor = new String(asmDescriptorBraces) + Type.getType(Object.class).getDescriptor();
                Type asmType = Type.getType(asmDescriptor);

                try {
                    return Class.forName(asmType.getInternalName().replace('/', '.'));
                } catch (ClassNotFoundException exception) {
                    throw new IllegalStateException("internal error", exception);
                }
            }
        } else if (painlessType == def.class) {
            return Object.class;
        }

        return painlessType;
    }

    public static String anyTypeNameToPainlessTypeName(String anyTypeName) {
        if (anyTypeName.startsWith(def.class.getName())) {
            anyTypeName = anyTypeName.replace(def.class.getName(), DEF_PAINLESS_CLASS_NAME);
        }

        return anyTypeName.replace('$', '.');
    }

    public static String buildPainlessMethodKey(String methodName, int methodArity) {
        return methodName + "/" + methodArity;
    }

    public static final String DEF_PAINLESS_CLASS_NAME = "def";
    public static final String PAINLESS_CONSTRUCTOR_NAME = "<init>";

    final Map<String, Class<?>> painlessClassNamesToJavaClasses;
    final Map<Class<?>, PainlessClass> javaClassesToPainlessClasses;

    PainlessLookupBase() {
        painlessClassNamesToJavaClasses = new HashMap<>();
        javaClassesToPainlessClasses = new HashMap<>();
    }

    public Class<?> painlessTypeNameToPainlessType(String painlessTypeName) {
        Class<?> javaClass = painlessClassNamesToJavaClasses.get(painlessTypeName);

        if (javaClass != null) {
            return javaClass;
        }

        int arrayDimensions = 0;
        int arrayIndex = painlessTypeName.indexOf('[');

        if (arrayIndex != -1) {
            int painlessTypeNameLength = painlessTypeName.length();

            while (arrayIndex < painlessTypeNameLength) {
                if (painlessTypeName.charAt(arrayIndex) == '[' &&
                        ++arrayIndex < painlessTypeNameLength  &&
                        painlessTypeName.charAt(arrayIndex++) == ']') {
                    ++arrayDimensions;
                } else {
                    throw new IllegalArgumentException("invalid painless type [" + painlessTypeName + "].");
                }
            }

            painlessTypeName = painlessTypeName.substring(0, painlessTypeName.indexOf('['));
            javaClass = painlessClassNamesToJavaClasses.get(painlessTypeName);

            char javaDescriptorBraces[] = new char[arrayDimensions];
            Arrays.fill(javaDescriptorBraces, '[');
            String javaDescriptor = new String(javaDescriptorBraces);

            if (javaClass == boolean.class) {
                javaDescriptor += "Z";
            } else if (javaClass == byte.class) {
                javaDescriptor += "B";
            } else if (javaClass == short.class) {
                javaDescriptor += "S";
            } else if (javaClass == char.class) {
                javaDescriptor += "C";
            } else if (javaClass == int.class) {
                javaDescriptor += "I";
            } else if (javaClass == long.class) {
                javaDescriptor += "J";
            } else if (javaClass == float.class) {
                javaDescriptor += "F";
            } else if (javaClass == double.class) {
                javaDescriptor += "D";
            } else {
                javaDescriptor += "L" + javaClass.getName() + ";";
            }

            try {
                return Class.forName(javaDescriptor);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalStateException("painless type [" + painlessTypeName + "] not found", cnfe);
            }
        }

        throw new IllegalArgumentException("painless type [" + painlessTypeName + "] not found");
    }

    public void validatePainlessType(Class<?> painlessType) {
        String painlessTypeName = anyTypeNameToPainlessTypeName(painlessType.getName());

        while (painlessType.getComponentType() != null) {
            painlessType = painlessType.getComponentType();
        }

        if (javaClassesToPainlessClasses.containsKey(painlessType) == false) {
            throw new IllegalStateException("painless type [" + painlessTypeName + "] not found");
        }
    }
}
