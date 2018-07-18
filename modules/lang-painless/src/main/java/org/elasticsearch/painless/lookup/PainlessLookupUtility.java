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
 *
 * - javaTypeName        (String)        - the fully qualified java type name for a painless type
 *                                         where '$' tokens are used to represent inner classes
 * - painlessTypeName    (String)        - the fully qualified painless type name or imported painless
 *                                         type name for a painless type where '.' tokens replace '$' tokens
 * - painlessType        (Class)         - a painless type represented by a java class including def
 *                                         and including array type java classes
 * - painlessClass       (PainlessClass) - a painless class object
 *
 * Under ambiguous circumstances most variable names are prefixed with asm, java, or painless.
 * If the variable value is the same for asm, java, and painless, no prefix is used.
 */
public final class PainlessLookupUtility {

    public static String javaTypeNameToPainlessTypeName(String javaTypeName) {
        Objects.requireNonNull(javaTypeName);
        assert(javaTypeName.startsWith("[") == false);

        if (javaTypeName.startsWith(def.class.getName())) {
            return javaTypeName.replace(def.class.getName(), DEF_PAINLESS_TYPE_NAME).replace('$', '.');
        } else {
            return javaTypeName.replace('$', '.');
        }
    }

    public static Class<?> painlessTypeNameToPainlessType(String painlessTypeName, Map<String, Class<?>> painlessTypeNamesToPainlessTypes) {
        Objects.requireNonNull(painlessTypeName);
        Objects.requireNonNull(painlessTypeNamesToPainlessTypes);

        Class<?> painlessType = painlessTypeNamesToPainlessTypes.get(painlessTypeName);

        if (painlessType != null) {
            return painlessType;
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
                    throw new IllegalArgumentException("painless type [" + painlessTypeName + "] not found");
                }
            }

            painlessTypeName = painlessTypeName.substring(0, painlessTypeName.indexOf('['));
            painlessType = painlessTypeNamesToPainlessTypes.get(painlessTypeName);

            char javaDescriptorBraces[] = new char[arrayDimensions];
            Arrays.fill(javaDescriptorBraces, '[');
            String javaDescriptor = new String(javaDescriptorBraces);

            if (painlessType == boolean.class) {
                javaDescriptor += "Z";
            } else if (painlessType == byte.class) {
                javaDescriptor += "B";
            } else if (painlessType == short.class) {
                javaDescriptor += "S";
            } else if (painlessType == char.class) {
                javaDescriptor += "C";
            } else if (painlessType == int.class) {
                javaDescriptor += "I";
            } else if (painlessType == long.class) {
                javaDescriptor += "J";
            } else if (painlessType == float.class) {
                javaDescriptor += "F";
            } else if (painlessType == double.class) {
                javaDescriptor += "D";
            } else {
                javaDescriptor += "L" + painlessType.getName() + ";";
            }

            try {
                return Class.forName(javaDescriptor);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalArgumentException("painless type [" + painlessTypeName + "] not found", cnfe);
            }
        }

        throw new IllegalArgumentException("painless type [" + painlessTypeName + "] not found");
    }

    public static String painlessTypeToPainlessTypeName(Class<?> painlessType) {
        Objects.requireNonNull(painlessType);

        if (painlessType.getCanonicalName().startsWith(def.class.getName())) {
            return painlessType.getCanonicalName().replace(def.class.getName(), DEF_PAINLESS_TYPE_NAME).replace('$', '.');
        } else {
            return painlessType.getCanonicalName().replace('$', '.');
        }
    }

    public static String painlessTypesToPainlessTypeNames(List<Class<?>> painlessTypes) {
        StringBuilder painlessTypesStringBuilder = new StringBuilder("[");

        int anyTypesSize = painlessTypes.size();
        int anyTypesIndex = 0;

        for (Class<?> painlessType : painlessTypes) {
            String anyTypeCanonicalName = javaTypeNameToPainlessTypeName(painlessType.getCanonicalName());

            painlessTypesStringBuilder.append(anyTypeCanonicalName);

            if (++anyTypesIndex < anyTypesSize) {
                painlessTypesStringBuilder.append(",");
            }
        }

        painlessTypesStringBuilder.append("]");

        return painlessTypesStringBuilder.toString();
    }

    public static Class<?> ObjectTypeTodefType(Class<?> painlessType) {
        Objects.requireNonNull(painlessType);

        if (painlessType.isArray()) {
            Class<?> painlessTypeComponent = painlessType.getComponentType();
            int arrayDimensions = 1;

            while (painlessTypeComponent.isArray()) {
                painlessTypeComponent = painlessTypeComponent.getComponentType();
                ++arrayDimensions;
            }

            if (painlessTypeComponent == Object.class) {
                char[] arrayBraces = new char[arrayDimensions];
                Arrays.fill(arrayBraces, '[');

                try {
                    return Class.forName(new String(arrayBraces) + def.class.getName() + ";");
                } catch (ClassNotFoundException cnfe) {
                    throw new IllegalStateException("internal error", cnfe);
                }
            }
        } else if (painlessType == Object.class) {
            return def.class;
        }

        return painlessType;
    }

    public static Class<?> defTypeToObjectType(Class<?> painlessType) {
        Objects.requireNonNull(painlessType);

        if (painlessType.isArray()) {
            Class<?> painlessTypeComponent = painlessType.getComponentType();
            int arrayDimensions = 1;

            while (painlessTypeComponent.isArray()) {
                painlessTypeComponent = painlessTypeComponent.getComponentType();
                ++arrayDimensions;
            }

            if (painlessTypeComponent == def.class) {
                char[] arrayBraces = new char[arrayDimensions];
                Arrays.fill(arrayBraces, '[');

                try {
                    return Class.forName(new String(arrayBraces) + Object.class.getName() + ";");
                } catch (ClassNotFoundException cnfe) {
                    throw new IllegalStateException("internal error", cnfe);
                }
            }
        } else if (painlessType == def.class) {
            return Object.class;
        }

        return painlessType;
    }

    public static void validatePainlessType(Class<?> painlessType, Collection<Class<?>> painlessTypes) {
        String painlessTypeName = javaTypeNameToPainlessTypeName(painlessType.getCanonicalName());

        while (painlessType.getComponentType() != null) {
            painlessType = painlessType.getComponentType();
        }

        if (painlessTypes.contains(painlessType) == false) {
            throw new IllegalArgumentException("painless type [" + painlessTypeName + "] not found");
        }
    }

    public static Class<?> toBoxedPainlessType(Class<?> painlessType) {
        if (painlessType == boolean.class) {
            return Boolean.class;
        } else if (painlessType == byte.class) {
            return Byte.class;
        } else if (painlessType == short.class) {
            return Short.class;
        } else if (painlessType == char.class) {
            return Character.class;
        } else if (painlessType == int.class) {
            return Integer.class;
        } else if (painlessType == long.class) {
            return Long.class;
        } else if (painlessType == float.class) {
            return Float.class;
        } else if (painlessType == double.class) {
            return Double.class;
        }

        return painlessType;
    }

    public static Class<?> toUnboxedPainlessType(Class<?> painlessType) {
        if (painlessType == Boolean.class) {
            return boolean.class;
        } else if (painlessType == Byte.class) {
            return byte.class;
        } else if (painlessType == Short.class) {
            return short.class;
        } else if (painlessType == Character.class) {
            return char.class;
        } else if (painlessType == Integer.class) {
            return int.class;
        } else if (painlessType == Long.class) {
            return long.class;
        } else if (painlessType == Float.class) {
            return float.class;
        } else if (painlessType == Double.class) {
            return double.class;
        }

        return painlessType;
    }

    public static boolean isConstantPainlessType(Class<?> painlessType) {
        return painlessType == boolean.class ||
               painlessType == byte.class    ||
               painlessType == short.class   ||
               painlessType == char.class    ||
               painlessType == int.class     ||
               painlessType == long.class    ||
               painlessType == float.class   ||
               painlessType == double.class  ||
               painlessType == String.class;
    }

    public static String buildPainlessMethodKey(String methodName, int methodArity) {
        return methodName + "/" + methodArity;
    }

    public static String buildPainlessFieldKey(String fieldName) {
        return fieldName;
    }

    public static final String DEF_PAINLESS_TYPE_NAME = "def";
    public static final String CONSTRUCTOR_ANY_NAME = "<init>";

    private PainlessLookupUtility() {

    }
}
