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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

/**
 * The entire API for Painless.  Also used as a whitelist for checking for legal
 * methods and fields during at both compile-time and runtime.
 */
public final class PainlessLookup extends PainlessLookupBase {

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

    PainlessLookup(Map<String, Class<?>> painlessClassNamesToJavaClasses, Map<Class<?>, PainlessClass> javaClassesToPainlessClasses) {
        super(painlessClassNamesToJavaClasses, javaClassesToPainlessClasses);
    }

    private void copyStruct(String struct, List<String> children) {
        final PainlessClass owner = javaClassesToPainlessStructs.get(painlessTypesToJavaClasses.get(struct));

        if (owner == null) {
            throw new IllegalArgumentException("Owner struct [" + struct + "] not defined for copy.");
        }

        for (int count = 0; count < children.size(); ++count) {
            final PainlessClass child = javaClassesToPainlessStructs.get(painlessTypesToJavaClasses.get(children.get(count)));

            if (child == null) {
                throw new IllegalArgumentException("Child struct [" + children.get(count) + "]" +
                    " not defined for copy to owner struct [" + owner.name + "].");
            }

            if (!child.clazz.isAssignableFrom(owner.clazz)) {
                throw new ClassCastException("Child struct [" + child.name + "]" +
                    " is not a super type of owner struct [" + owner.name + "] in copy.");
            }

            for (Map.Entry<PainlessMethodKey,PainlessMethod> kvPair : child.methods.entrySet()) {
                PainlessMethodKey methodKey = kvPair.getKey();
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
                    owner.members.put(field.name,
                        new PainlessField(field.name, field.javaName, owner, field.clazz, field.modifiers, field.getter, field.setter));
                }
            }
        }
    }

    /**
     * Precomputes a more efficient structure for dynamic method/field access.
     */
    private void addRuntimeClass(final PainlessClass struct) {
        // add all getters/setters
        for (Map.Entry<PainlessMethodKey, PainlessMethod> method : struct.methods.entrySet()) {
            String name = method.getKey().name;
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
    private PainlessMethod computeFunctionalInterfaceMethod(PainlessClass clazz) {
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
        PainlessMethod painless = clazz.methods.get(new PainlessMethodKey(oneMethod.getName(), oneMethod.getParameterCount()));
        if (painless == null || painless.method.equals(org.objectweb.asm.commons.Method.getMethod(oneMethod)) == false) {
            throw new IllegalArgumentException("Class: " + clazz.name + " is functional but the functional " +
                "method is not whitelisted!");
        }
        return painless;
    }

    public boolean isSimplePainlessType(String painlessType) {
        return painlessTypesToJavaClasses.containsKey(painlessType);
    }

    public PainlessClass getPainlessStructFromJavaClass(Class<?> clazz) {
        return javaClassesToPainlessStructs.get(clazz);
    }
}
