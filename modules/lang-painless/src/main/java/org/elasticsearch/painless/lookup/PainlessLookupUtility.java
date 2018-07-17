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
import java.util.Collection;
import java.util.Map;

/**
 * This class contains methods shared by {@link PainlessLookupBuilder}, {@link PainlessLookup}, and other classes within
 * Painless for conversion between type names and types along with some other various utility methods.
 *
 * The following terminology is used for variable names throughout the lookup package:
 *
 * - javaClass           (Class)         - a java class including def and excluding array type java classes
 * - javaClassName       (String)        - the fully qualified java class name for a javaClass
 * - painlessClassName   (String)        - the fully qualified painless name or imported painless name for a painlessClass
 * - anyClassName        (String)        - either a javaClassName or a painlessClassName
 * - javaType            (Class)         - a java class excluding def and array type java classes
 * - painlessType        (Class)         - a java class including def and array type java classes
 * - javaTypeName        (String)        - the fully qualified java Type name for a javaType
 * - painlessTypeName    (String)        - the fully qualified painless name or imported painless name for a painlessType
 * - anyTypeName         (String)        - either a javaTypeName or a painlessTypeName
 * - painlessClass       (PainlessClass) - a painless class object
 *
 * Under ambiguous circumstances most variable names are prefixed with asm, java, or painless.
 * If the variable name is the same for asm, java, and painless, no prefix is used.
 */
public final class PainlessLookupUtility {

    public static Class<?> javaObjectTypeToPainlessDefType(Class<?> javaType) {
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

    public static Class<?> painlessDefTypeToJavaObjectType(Class<?> painlessType) {
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
        return anyTypeName.replace(def.class.getName(), DEF_PAINLESS_CLASS_NAME).replace('$', '.');
    }

    public static String anyTypeToPainlessTypeName(Class<?> anyType) {
        if (anyType.isLocalClass() || anyType.isAnonymousClass()) {
            return null;
        } else if (anyType.isArray()) {
            Class<?> anyTypeComponent = anyType.getComponentType();
            int arrayDimensions = 1;

            while (anyTypeComponent.isArray()) {
                anyTypeComponent = anyTypeComponent.getComponentType();
                ++arrayDimensions;
            }

            if (anyTypeComponent == def.class) {
                StringBuilder painlessDefTypeNameArrayBuilder = new StringBuilder(DEF_PAINLESS_CLASS_NAME);

                for (int dimension = 0; dimension < arrayDimensions; dimension++) {
                    painlessDefTypeNameArrayBuilder.append("[]");
                }

                return painlessDefTypeNameArrayBuilder.toString();
            }
        } else if (anyType == def.class) {
            return DEF_PAINLESS_CLASS_NAME;
        }

        return anyType.getCanonicalName().replace('$', '.');
    }

    public static Class<?> painlessTypeNameToPainlessType(String painlessTypeName, Map<String, Class<?>> painlessClassNamesToJavaClasses) {
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

    public static void validatePainlessType(Class<?> painlessType, Collection<Class<?>> javaClasses) {
        String painlessTypeName = anyTypeNameToPainlessTypeName(painlessType.getName());

        while (painlessType.getComponentType() != null) {
            painlessType = painlessType.getComponentType();
        }

        if (javaClasses.contains(painlessType) == false) {
            throw new IllegalStateException("painless type [" + painlessTypeName + "] not found");
        }
    }

    public static String buildPainlessMethodKey(String methodName, int methodArity) {
        return methodName + "/" + methodArity;
    }

    public static String buildPainlessFieldKey(String fieldName) {
        return fieldName;
    }

    public static Class<?> getBoxedAnyType(Class<?> anyType) {
        if (anyType == boolean.class) {
            return Boolean.class;
        } else if (anyType == byte.class) {
            return Byte.class;
        } else if (anyType == short.class) {
            return Short.class;
        } else if (anyType == char.class) {
            return Character.class;
        } else if (anyType == int.class) {
            return Integer.class;
        } else if (anyType == long.class) {
            return Long.class;
        } else if (anyType == float.class) {
            return Float.class;
        } else if (anyType == double.class) {
            return Double.class;
        }

        return anyType;
    }

    public static Class<?> getUnboxedAnyType(Class<?> anyType) {
        if (anyType == Boolean.class) {
            return boolean.class;
        } else if (anyType == Byte.class) {
            return byte.class;
        } else if (anyType == Short.class) {
            return short.class;
        } else if (anyType == Character.class) {
            return char.class;
        } else if (anyType == Integer.class) {
            return int.class;
        } else if (anyType == Long.class) {
            return long.class;
        } else if (anyType == Float.class) {
            return float.class;
        } else if (anyType == Double.class) {
            return double.class;
        }

        return anyType;
    }

    public static boolean isAnyTypeConstant(Class<?> anyType) {
        return anyType == boolean.class ||
               anyType == byte.class    ||
               anyType == short.class   ||
               anyType == char.class    ||
               anyType == int.class     ||
               anyType == long.class    ||
               anyType == float.class   ||
               anyType == double.class  ||
               anyType == String.class;
    }

    public static final String DEF_PAINLESS_CLASS_NAME = def.class.getSimpleName();
    public static final String CONSTRUCTOR_ANY_NAME = "<init>";

    private PainlessLookupUtility() {

    }
}
