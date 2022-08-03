/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.lookup;

import org.elasticsearch.painless.spi.annotation.InjectConstantAnnotation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * PainlessLookupUtility contains methods shared by {@link PainlessLookupBuilder}, {@link PainlessLookup}, and other classes within
 * Painless for conversion between type names and types along with some other various utility methods.
 *
 * The following terminology is used for variable names throughout the lookup package:
 *
 * A class is a set of methods and fields under a specific class name. A type is either a class or an array under a specific type name.
 * Note the distinction between class versus type is class means that no array classes will be be represented whereas type allows array
 * classes to be represented. The set of available classes will always be a subset of the available types.
 *
 * Under ambiguous circumstances most variable names are prefixed with asm, java, or painless. If the variable value is the same for asm,
 * java, and painless, no prefix is used. Target is used as a prefix to represent if a constructor, method, or field is being
 * called/accessed on that specific class.  Parameter is often a postfix used to represent if a type is used as a parameter to a
 * constructor, method, or field.
 *
 * <ul>
 *     <li> - javaClassName     (String)         - the fully qualified java class name where '$' tokens represent inner classes excluding
 *                                                 def and array types </li>
 *
 *     <li> - javaClass          (Class)         - a java class excluding def and array types </li>
 *
 *     <li> - javaType           (Class)         - a java class excluding def and including array types </li>
 *
 *     <li> - importedClassName (String)         - the imported painless class name where the java canonical class name is used without
 *                                                 the package qualifier
 *
 *     <li> - canonicalClassName (String)        - the fully qualified painless class name equivalent to the fully
 *                                                 qualified java canonical class name or imported painless class name for a class
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
 */
public final class PainlessLookupUtility {

    /**
     * The name for an anonymous class.
     */
    public static final String ANONYMOUS_CLASS_NAME = "$anonymous";

    /**
     * The def type name as specified in the source for a script.
     */
    public static final String DEF_CLASS_NAME = "def";

    /**
     * The method name for all constructors.
     */
    public static final String CONSTRUCTOR_NAME = "<init>";

    /**
     * Converts a canonical type name to a type based on the terminology specified as part of the documentation for
     * {@link PainlessLookupUtility}. Since canonical class names are a subset of canonical type names, this method will
     * safely convert a canonical class name to a class as well.
     */
    public static Class<?> canonicalTypeNameToType(String canonicalTypeName, Map<String, Class<?>> canonicalClassNamesToClasses) {
        Objects.requireNonNull(canonicalTypeName);
        Objects.requireNonNull(canonicalClassNamesToClasses);

        Class<?> type = DEF_CLASS_NAME.equals(canonicalTypeName) ? def.class : canonicalClassNamesToClasses.get(canonicalTypeName);

        if (type != null) {
            return type;
        }

        int arrayDimensions = 0;
        int arrayIndex = canonicalTypeName.indexOf('[');

        if (arrayIndex != -1) {
            int typeNameLength = canonicalTypeName.length();

            while (arrayIndex < typeNameLength) {
                if (canonicalTypeName.charAt(arrayIndex) == '['
                    && ++arrayIndex < typeNameLength
                    && canonicalTypeName.charAt(arrayIndex++) == ']') {
                    ++arrayDimensions;
                } else {
                    return null;
                }
            }

            canonicalTypeName = canonicalTypeName.substring(0, canonicalTypeName.indexOf('['));
            type = DEF_CLASS_NAME.equals(canonicalTypeName) ? def.class : canonicalClassNamesToClasses.get(canonicalTypeName);

            if (type != null) {
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
                    throw new IllegalStateException("internal error", cnfe);
                }
            }
        }

        return null;
    }

    /**
     * Converts a type to a canonical type name based on the terminology specified as part of the documentation for
     * {@link PainlessLookupUtility}. Since classes are a subset of types, this method will safely convert a class
     * to a canonical class name as well.
     */
    public static String typeToCanonicalTypeName(Class<?> type) {
        Objects.requireNonNull(type);

        String canonicalTypeName = type.getCanonicalName();

        if (canonicalTypeName == null) {
            canonicalTypeName = ANONYMOUS_CLASS_NAME;
        } else if (canonicalTypeName.startsWith(def.class.getCanonicalName())) {
            canonicalTypeName = canonicalTypeName.replace(def.class.getCanonicalName(), DEF_CLASS_NAME);
        }

        return canonicalTypeName;
    }

    /**
     * Converts a list of types to a list of canonical type names as a string based on the terminology specified as part of the
     * documentation for {@link PainlessLookupUtility}. Since classes are a subset of types, this method will safely convert a list
     * of classes or a mixed list of classes and types to a list of canonical type names as a string as well.
     */
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

    /**
     * Converts a java type to a type based on the terminology specified as part of {@link PainlessLookupUtility} where if a type is an
     * object class or object array, the returned type will be the equivalent def class or def array. Otherwise, this behaves as an
     * identity function.
     */
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

    /**
     * Converts a type to a java type based on the terminology specified as part of {@link PainlessLookupUtility} where if a type is a
     * def class or def array, the returned type will be the equivalent object class or object array. Otherwise, this behaves as an
     * identity function.
     */
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

    /**
     * Converts a type  to its boxed type equivalent if one exists based on the terminology specified as part of
     * {@link PainlessLookupUtility}. Otherwise, this behaves as an identity function.
     */
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

    /**
     * Converts a type to its unboxed type equivalent if one exists based on the terminology specified as part of
     * {@link PainlessLookupUtility}. Otherwise, this behaves as an identity function.
     */
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

    /**
     * Checks if a type based on the terminology specified as part of {@link PainlessLookupUtility} is available as a constant type
     * where {@code true} is returned if the type is a constant type and {@code false} otherwise.
     */
    public static boolean isConstantType(Class<?> type) {
        return type == boolean.class
            || type == byte.class
            || type == short.class
            || type == char.class
            || type == int.class
            || type == long.class
            || type == float.class
            || type == double.class
            || type == String.class;
    }

    /**
     * Constructs a painless constructor key used to lookup painless constructors from a painless class.
     */
    public static String buildPainlessConstructorKey(int constructorArity) {
        return CONSTRUCTOR_NAME + "/" + constructorArity;
    }

    /**
     * Constructs a painless method key used to lookup painless methods from a painless class.
     */
    public static String buildPainlessMethodKey(String methodName, int methodArity) {
        return methodName + "/" + methodArity;
    }

    /**
     * Constructs a painless field key used to lookup painless fields from a painless class.
     */
    public static String buildPainlessFieldKey(String fieldName) {
        return fieldName;
    }

    /**
     * Constructs an array of injectable constants for a specific {@link PainlessMethod}
     * derived from an {@link org.elasticsearch.painless.spi.annotation.InjectConstantAnnotation}.
     */
    public static Object[] buildInjections(PainlessMethod painlessMethod, Map<String, Object> constants) {
        if (painlessMethod.annotations().containsKey(InjectConstantAnnotation.class) == false) {
            return new Object[0];
        }

        List<String> names = ((InjectConstantAnnotation) painlessMethod.annotations().get(InjectConstantAnnotation.class)).injects();
        Object[] injections = new Object[names.size()];

        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            Object constant = constants.get(name);

            if (constant == null) {
                throw new IllegalStateException(
                    "constant ["
                        + name
                        + "] not found for injection into method "
                        + "["
                        + buildPainlessMethodKey(painlessMethod.javaMethod().getName(), painlessMethod.typeParameters().size())
                        + "]"
                );
            }

            injections[i] = constant;
        }

        return injections;
    }

    private PainlessLookupUtility() {

    }
}
