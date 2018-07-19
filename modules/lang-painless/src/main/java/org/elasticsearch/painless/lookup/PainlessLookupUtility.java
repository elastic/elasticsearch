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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class contains methods shared by {@link PainlessLookupBuilder}, {@link PainlessLookup}, and other
 * classes within Painless for conversion between type names and types along with some other various utility
 * methods.
 *
 * The following terminology is used for variable names throughout the lookup package:
 * <ul>
 *     <li> - javaClassName     (String)         - the fully qualified java class name where '$' tokens represent inner classes excluding
 *                                                 def and array types </li>
 *
 *     <li> - javaClass          (Class)         - a java class excluding def and array types </li>
 *
 *     <li> - javaType           (Class)         - a java class excluding def and including array types </li>
 *
 *     <li> - canonicalClassName (String)        - the fully qualified painless class name equivalent to the fully
 *                                                 qualified java canonical class name or imported painless type name for a type
 *                                                 including def and excluding array types where '.' tokens represent inner classes <li>
 *
 *     <li> - canonicalTypeName (String)         - the fully qualified painless type name equivalent to the fully
 *                                                 qualified java canonical type name or imported painless type name for a type
 *                                                 including def where '.' tokens represent inner classes and each set of '[]' tokens
 *                                                 at the end of the type name represent a single dimension for an array type <li>
 *
 *     <li> - class/clazz       (Class)          - a painless class represented by a java class including def and excluding array
 *                                                 types </li>
 *
 *     <li> - type              (Class)          - a painless type represented by a java class including def and array types </li>
 *
 *     <li> - painlessClass     (PainlessClass)  - a painless class object </li>
 *
 *     <li> - painlessMethod    (PainlessMethod) - a painless method object </li>
 *
 *     <li> - painlessField     (PainlessField)  - a painless field object </li>
 * </ul>
 *
 * Under ambiguous circumstances most variable names are prefixed with asm, java, or painless.
 * If the variable value is the same for asm, java, and painless, no prefix is used.
 */
public final class PainlessLookupUtility {

    public static Class<?> canonicalTypeNameToType(String canonicalTypeName, Map<String, Class<?>> canonicalClassNamesToClasses) {
        Objects.requireNonNull(canonicalTypeName);
        Objects.requireNonNull(canonicalClassNamesToClasses);

        Class<?> type = canonicalClassNamesToClasses.get(canonicalTypeName);

        if (type != null) {
            return type;
        }

        int arrayDimensions = 0;
        int arrayIndex = canonicalTypeName.indexOf('[');

        if (arrayIndex != -1) {
            int typeNameLength = canonicalTypeName.length();

            while (arrayIndex < typeNameLength) {
                if (canonicalTypeName.charAt(arrayIndex) == '[' &&
                    ++arrayIndex < typeNameLength  &&
                    canonicalTypeName.charAt(arrayIndex++) == ']') {
                    ++arrayDimensions;
                } else {
                    throw new IllegalArgumentException("type [" + canonicalTypeName + "] not found");
                }
            }

            canonicalTypeName = canonicalTypeName.substring(0, canonicalTypeName.indexOf('['));
            type = canonicalClassNamesToClasses.get(canonicalTypeName);

            char arrayBraces[] = new char[arrayDimensions];
            Arrays.fill(arrayBraces, '[');
            String javaTypeName = new String(arrayBraces);

            if (type == boolean.class) {
                javaTypeName += "Z";
            } else if (type == byte.class) {
                javaTypeName += "B";
            } else if (type == short.class) {
                javaTypeName += "S";
            } else if (type == char.class) {
                javaTypeName += "C";
            } else if (type == int.class) {
                javaTypeName += "I";
            } else if (type == long.class) {
                javaTypeName += "J";
            } else if (type == float.class) {
                javaTypeName += "F";
            } else if (type == double.class) {
                javaTypeName += "D";
            } else {
                javaTypeName += "L" + type.getName() + ";";
            }

            try {
                return Class.forName(javaTypeName);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException("type [" + canonicalTypeName + "] not found", cnfe);
            }
        }

        throw new IllegalArgumentException("type [" + canonicalTypeName + "] not found");
    }

    public static String typeToCanonicalTypeName(Class<?> type) {
        Objects.requireNonNull(type);

        String canonicalTypeName = type.getCanonicalName();

        if (canonicalTypeName.startsWith(def.class.getName())) {
            canonicalTypeName = canonicalTypeName.replace(def.class.getName(), DEF_TYPE_NAME);
        }

        return canonicalTypeName;
    }

    public static String typesToCanonicalTypeNames(List<Class<?>> types) {
        StringBuilder typesStringBuilder = new StringBuilder("[");

        int anyTypesSize = types.size();
        int anyTypesIndex = 0;

        for (Class<?> painlessType : types) {
            String canonicalTypeName = typeToCanonicalTypeName(painlessType);

            typesStringBuilder.append(canonicalTypeName);

            if (++anyTypesIndex < anyTypesSize) {
                typesStringBuilder.append(",");
            }
        }

        typesStringBuilder.append("]");

        return typesStringBuilder.toString();
    }

    public static Class<?> javaTypeToType(Class<?> javaType) {
        Objects.requireNonNull(javaType);

        if (javaType.isArray()) {
            Class<?> javaTypeComponent = javaType.getComponentType();
            int arrayDimensions = 1;

            while (javaTypeComponent.isArray()) {
                javaTypeComponent = javaTypeComponent.getComponentType();
                ++arrayDimensions;
            }

            if (javaTypeComponent == Object.class) {
                char[] arrayBraces = new char[arrayDimensions];
                Arrays.fill(arrayBraces, '[');

                try {
                    return Class.forName(new String(arrayBraces) + "L" + def.class.getName() + ";");
                } catch (ClassNotFoundException cnfe) {
                    throw new IllegalStateException("internal error", cnfe);
                }
            }
        } else if (javaType == Object.class) {
            return def.class;
        }

        return javaType;
    }

    public static Class<?> typeToJavaType(Class<?> type) {
        Objects.requireNonNull(type);

        if (type.isArray()) {
            Class<?> typeComponent = type.getComponentType();
            int arrayDimensions = 1;

            while (typeComponent.isArray()) {
                typeComponent = typeComponent.getComponentType();
                ++arrayDimensions;
            }

            if (typeComponent == def.class) {
                char[] arrayBraces = new char[arrayDimensions];
                Arrays.fill(arrayBraces, '[');

                try {
                    return Class.forName(new String(arrayBraces) + "L" + Object.class.getName() + ";");
                } catch (ClassNotFoundException cnfe) {
                    throw new IllegalStateException("internal error", cnfe);
                }
            }
        } else if (type == def.class) {
            return Object.class;
        }

        return type;
    }

    public static void validateType(Class<?> type, Collection<Class<?>> classes) {
        String canonicalTypeName = typeToCanonicalTypeName(type);

        while (type.getComponentType() != null) {
            type = type.getComponentType();
        }

        if (classes.contains(type) == false) {
            throw new IllegalArgumentException("type [" + canonicalTypeName + "] not found");
        }
    }

    public static Class<?> typeToBoxedType(Class<?> type) {
        if (type == boolean.class) {
            return Boolean.class;
        } else if (type == byte.class) {
            return Byte.class;
        } else if (type == short.class) {
            return Short.class;
        } else if (type == char.class) {
            return Character.class;
        } else if (type == int.class) {
            return Integer.class;
        } else if (type == long.class) {
            return Long.class;
        } else if (type == float.class) {
            return Float.class;
        } else if (type == double.class) {
            return Double.class;
        }

        return type;
    }

    public static Class<?> typeToUnboxedType(Class<?> type) {
        if (type == Boolean.class) {
            return boolean.class;
        } else if (type == Byte.class) {
            return byte.class;
        } else if (type == Short.class) {
            return short.class;
        } else if (type == Character.class) {
            return char.class;
        } else if (type == Integer.class) {
            return int.class;
        } else if (type == Long.class) {
            return long.class;
        } else if (type == Float.class) {
            return float.class;
        } else if (type == Double.class) {
            return double.class;
        }

        return type;
    }

    public static boolean isConstantType(Class<?> type) {
        return type == boolean.class ||
               type == byte.class    ||
               type == short.class   ||
               type == char.class    ||
               type == int.class     ||
               type == long.class    ||
               type == float.class   ||
               type == double.class  ||
               type == String.class;
    }

    public static String buildPainlessMethodKey(String methodName, int methodArity) {
        return methodName + "/" + methodArity;
    }

    public static String buildPainlessFieldKey(String fieldName) {
        return fieldName;
    }

    public static final String DEF_TYPE_NAME = "def";
    public static final String CONSTRUCTOR_NAME = "<init>";

    private PainlessLookupUtility() {

    }
}
