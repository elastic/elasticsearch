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
import java.util.Collections;
import java.util.Map;

/**
 * The entire API for Painless.  Also used as a whitelist for checking for legal
 * methods and fields during at both compile-time and runtime.
 */
public final class PainlessLookup {

    public static Class<?> getBoxedType(Class<?> clazz) {
        if (clazz == boolean.class) {
            return Boolean.class;
        } else if (clazz == byte.class) {
            return Byte.class;
        } else if (clazz == short.class) {
            return Short.class;
        } else if (clazz == char.class) {
            return Character.class;
        } else if (clazz == int.class) {
            return Integer.class;
        } else if (clazz == long.class) {
            return Long.class;
        } else if (clazz == float.class) {
            return Float.class;
        } else if (clazz == double.class) {
            return Double.class;
        }

        return clazz;
    }

    public static Class<?> getUnboxedype(Class<?> clazz) {
        if (clazz == Boolean.class) {
            return boolean.class;
        } else if (clazz == Byte.class) {
            return byte.class;
        } else if (clazz == Short.class) {
            return short.class;
        } else if (clazz == Character.class) {
            return char.class;
        } else if (clazz == Integer.class) {
            return int.class;
        } else if (clazz == Long.class) {
            return long.class;
        } else if (clazz == Float.class) {
            return float.class;
        } else if (clazz == Double.class) {
            return double.class;
        }

        return clazz;
    }

    public static boolean isConstantType(Class<?> clazz) {
        return clazz == boolean.class ||
               clazz == byte.class    ||
               clazz == short.class   ||
               clazz == char.class    ||
               clazz == int.class     ||
               clazz == long.class    ||
               clazz == float.class   ||
               clazz == double.class  ||
               clazz == String.class;
    }

    public Class<?> getClassFromBinaryName(String painlessType) {
        return painlessTypesToJavaClasses.get(painlessType.replace('$', '.'));
    }

    public static Class<?> ObjectClassTodefClass(Class<?> clazz) {
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

    public static Class<?> defClassToObjectClass(Class<?> clazz) {
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

    public static String ClassToName(Class<?> clazz) {
        if (clazz.isLocalClass() || clazz.isAnonymousClass()) {
            return null;
        } else if (clazz.isArray()) {
            Class<?> component = clazz.getComponentType();
            int dimensions = 1;

            while (component.isArray()) {
                component = component.getComponentType();
                ++dimensions;
            }

            if (component == def.class) {
                StringBuilder builder = new StringBuilder(def.class.getSimpleName());

                for (int dimension = 0; dimension < dimensions; dimension++) {
                    builder.append("[]");
                }

                return builder.toString();
            }
        } else if (clazz == def.class) {
            return def.class.getSimpleName();
        }

        return clazz.getCanonicalName().replace('$', '.');
    }

    public Collection<PainlessClass> getStructs() {
        return javaClassesToPainlessStructs.values();
    }

    private final Map<String, Class<?>> painlessTypesToJavaClasses;
    private final Map<Class<?>, PainlessClass> javaClassesToPainlessStructs;

    PainlessLookup(Map<String, Class<?>> painlessTypesToJavaClasses, Map<Class<?>, PainlessClass> javaClassesToPainlessStructs) {
        this.painlessTypesToJavaClasses = Collections.unmodifiableMap(painlessTypesToJavaClasses);
        this.javaClassesToPainlessStructs = Collections.unmodifiableMap(javaClassesToPainlessStructs);
    }

    public boolean isSimplePainlessType(String painlessType) {
        return painlessTypesToJavaClasses.containsKey(painlessType);
    }

    public PainlessClass getPainlessStructFromJavaClass(Class<?> clazz) {
        return javaClassesToPainlessStructs.get(clazz);
    }

    public Class<?> getJavaClassFromPainlessType(String painlessType) {
        Class<?> javaClass = painlessTypesToJavaClasses.get(painlessType);

        if (javaClass != null) {
            return javaClass;
        }
        int arrayDimensions = 0;
        int arrayIndex = painlessType.indexOf('[');

        if (arrayIndex != -1) {
            int length = painlessType.length();

            while (arrayIndex < length) {
                if (painlessType.charAt(arrayIndex) == '[' && ++arrayIndex < length && painlessType.charAt(arrayIndex++) == ']') {
                    ++arrayDimensions;
                } else {
                    throw new IllegalArgumentException("invalid painless type [" + painlessType + "].");
                }
            }

            painlessType = painlessType.substring(0, painlessType.indexOf('['));
            javaClass = painlessTypesToJavaClasses.get(painlessType);

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
                throw new IllegalStateException("invalid painless type [" + painlessType + "]", cnfe);
            }
        }

        throw new IllegalArgumentException("invalid painless type [" + painlessType + "]");
    }
}
