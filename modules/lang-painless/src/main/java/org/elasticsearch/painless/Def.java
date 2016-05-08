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

package org.elasticsearch.painless;

import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.RuntimeClass;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

public class Def {

    /** Looks up handle for a method call from whitelist */
    static MethodHandle methodHandle(Class<?> ownerClass, String name, Definition definition) {
        for (Class<?> clazz = ownerClass; clazz != null; clazz = clazz.getSuperclass()) {
            RuntimeClass struct = definition.runtimeMap.get(clazz);

            if (struct != null) {
                Method method = struct.methods.get(name);

                if (method != null) {
                    return method.handle;
                }
            }

            for (final Class<?> iface : clazz.getInterfaces()) {
                struct = definition.runtimeMap.get(iface);

                if (struct != null) {
                    Method method = struct.methods.get(name);

                    if (method != null) {
                        return method.handle;
                    }
                }
            }
        }
      
        throw new IllegalArgumentException("Unable to find dynamic method [" + name + "] " +
                                           "for class [" + ownerClass.getCanonicalName() + "].");
    }
    
    private static final MethodHandle ARRAY_LENGTH;
    private static final MethodHandle MAP_GET;
    private static final MethodHandle MAP_PUT;
    private static final MethodHandle LIST_GET;
    private static final MethodHandle LIST_SET;
    static {
        Lookup lookup = MethodHandles.lookup();
        try {
            ARRAY_LENGTH = lookup.findStatic(Array.class, "getLength",
                                             MethodType.methodType(int.class, Object.class));
            MAP_GET      = lookup.findVirtual(Map.class, "get",
                                             MethodType.methodType(Object.class, Object.class));
            MAP_PUT      = lookup.findVirtual(Map.class, "put",
                                             MethodType.methodType(Object.class, Object.class, Object.class));
            LIST_GET     = lookup.findVirtual(List.class, "get",
                                             MethodType.methodType(Object.class, int.class));
            LIST_SET     = lookup.findVirtual(List.class, "set",
                                             MethodType.methodType(Object.class, int.class, Object.class));
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }
    
    /** Looks up handle for a field load from whitelist */
    static MethodHandle loadHandle(Class<?> ownerClass, String name, Definition definition) {
        // first try whitelist
        for (Class<?> clazz = ownerClass; clazz != null; clazz = clazz.getSuperclass()) {
            RuntimeClass struct = definition.runtimeMap.get(clazz);

            if (struct != null) {
                MethodHandle handle = struct.getters.get(name);
                if (handle != null) {
                    return handle;
                }
            }

            for (final Class<?> iface : clazz.getInterfaces()) {
                struct = definition.runtimeMap.get(iface);

                if (struct != null) {
                    MethodHandle handle = struct.getters.get(name);
                    if (handle != null) {
                        return handle;
                    }
                }
            }
        }
        // special case: arrays, maps, and lists
        if (ownerClass.isArray() && "length".equals(name)) {
            return ARRAY_LENGTH;
        } else if (Map.class.isAssignableFrom(ownerClass)) {
            return MethodHandles.insertArguments(MAP_GET, 1, name);
        } else if (List.class.isAssignableFrom(ownerClass)) {
            try {
                int index = Integer.parseInt(name);
                return MethodHandles.insertArguments(LIST_GET, 1, index);            
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException( "Illegal list shortcut value [" + name + "].");
            }
        }
        
        throw new IllegalArgumentException("Unable to find dynamic field [" + name + "] " +
                                           "for class [" + ownerClass.getCanonicalName() + "].");
    }
    
    /** Looks up handle for a field store from whitelist */
    static MethodHandle storeHandle(Class<?> ownerClass, String name, Definition definition) {
        // first try whitelist
        for (Class<?> clazz = ownerClass; clazz != null; clazz = clazz.getSuperclass()) {
            RuntimeClass struct = definition.runtimeMap.get(clazz);

            if (struct != null) {
                MethodHandle handle = struct.setters.get(name);
                if (handle != null) {
                    return handle;
                }
            }

            for (final Class<?> iface : clazz.getInterfaces()) {
                struct = definition.runtimeMap.get(iface);

                if (struct != null) {
                    MethodHandle handle = struct.setters.get(name);
                    if (handle != null) {
                        return handle;
                    }
                }
            }
        }
        // special case: maps, and lists
        if (Map.class.isAssignableFrom(ownerClass)) {
            return MethodHandles.insertArguments(MAP_PUT, 1, name);
        } else if (List.class.isAssignableFrom(ownerClass)) {
            try {
                int index = Integer.parseInt(name);
                return MethodHandles.insertArguments(LIST_SET, 1, index);            
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException( "Illegal list shortcut value [" + name + "].");
            }
        }
        
        throw new IllegalArgumentException("Unable to find dynamic field [" + name + "] " +
                                           "for class [" + ownerClass.getCanonicalName() + "].");
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static void arrayStore(final Object array, Object index, Object value) {
        if (array instanceof Map) {
            ((Map)array).put(index, value);
        } else if (array.getClass().isArray()) {
            try {
                Array.set(array, (int)index, value);
            } catch (final Throwable throwable) {
                throw new IllegalArgumentException("Error storing value [" + value + "] " +
                                                   "in array class [" + array.getClass().getCanonicalName() + "].", throwable);
            }
        } else if (array instanceof List) {
            ((List)array).set((int)index, value);
        } else {
            throw new IllegalArgumentException("Attempting to address a non-array type " +
                                               "[" + array.getClass().getCanonicalName() + "] as an array.");
        }
    }
    
    @SuppressWarnings("rawtypes")
    public static Object arrayLoad(final Object array, Object index) {
        if (array instanceof Map) {
            return ((Map)array).get(index);
        } else if (array.getClass().isArray()) {
            try {
                return Array.get(array, (int)index);
            } catch (final Throwable throwable) {
                throw new IllegalArgumentException("Error loading value from " +
                                                   "array class [" + array.getClass().getCanonicalName() + "].", throwable);
            }
        } else if (array instanceof List) {
            return ((List)array).get((int)index);
        } else {
            throw new IllegalArgumentException("Attempting to address a non-array type " +
                                               "[" + array.getClass().getCanonicalName() + "] as an array.");
        }
    }

    public static Object not(final Object unary) {
        if (unary instanceof Double || unary instanceof Float || unary instanceof Long) {
            return ~((Number)unary).longValue();
        } else if (unary instanceof Number) {
            return ~((Number)unary).intValue();
        } else if (unary instanceof Character) {
            return ~(int)(char)unary;
        }

        throw new ClassCastException("Cannot apply [~] operation to type " +
                "[" + unary.getClass().getCanonicalName() + "].");
    }

    public static Object neg(final Object unary) {
        if (unary instanceof Double) {
            return -(double)unary;
        } else if (unary instanceof Float) {
            return -(float)unary;
        } else if (unary instanceof Long) {
            return -(long)unary;
        } else if (unary instanceof Number) {
            return -((Number)unary).intValue();
        } else if (unary instanceof Character) {
            return -(char)unary;
        }

        throw new ClassCastException("Cannot apply [-] operation to type " +
                "[" + unary.getClass().getCanonicalName() + "].");
    }

    public static Object mul(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number)left).doubleValue() * ((Number)right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number)left).floatValue() * ((Number)right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() * ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() * ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number)left).doubleValue() * (char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() * (char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() * (char)right;
                } else {
                    return ((Number)left).intValue() * (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char)left * ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (char)left * ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (char)left * ((Number)right).longValue();
                } else {
                    return (char)left * ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left * (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [*] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static Object div(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number)left).doubleValue() / ((Number)right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number)left).floatValue() / ((Number)right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() / ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() / ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number)left).doubleValue() / (char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() / (char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() / (char)right;
                } else {
                    return ((Number)left).intValue() / (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char)left / ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (char)left / ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (char)left / ((Number)right).longValue();
                } else {
                    return (char)left / ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left / (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [/] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static Object rem(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number)left).doubleValue() % ((Number)right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number)left).floatValue() % ((Number)right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() % ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() % ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number)left).doubleValue() % (char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() % (char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() % (char)right;
                } else {
                    return ((Number)left).intValue() % (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char)left % ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (char)left % ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (char)left % ((Number)right).longValue();
                } else {
                    return (char)left % ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left % (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [%] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static Object add(final Object left, final Object right) {
        if (left instanceof String || right instanceof String) {
            return "" + left + right;
        } else if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number)left).doubleValue() + ((Number)right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number)left).floatValue() + ((Number)right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() + ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() + ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number)left).doubleValue() + (char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() + (char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() + (char)right;
                } else {
                    return ((Number)left).intValue() + (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char)left + ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (char)left + ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (char)left + ((Number)right).longValue();
                } else {
                    return (char)left + ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left + (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [+] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static Object sub(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number)left).doubleValue() - ((Number)right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number)left).floatValue() - ((Number)right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() - ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() - ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number)left).doubleValue() - (char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() - (char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() - (char)right;
                } else {
                    return ((Number)left).intValue() - (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char)left - ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (char)left - ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (char)left - ((Number)right).longValue();
                } else {
                    return (char)left - ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left - (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [-] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static Object lsh(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double ||
                        left instanceof Float || right instanceof Float ||
                        left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() << ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() << ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double || left instanceof Float || left instanceof Long) {
                    return ((Number)left).longValue() << (char)right;
                } else {
                    return ((Number)left).intValue() << (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (long)(char)left << ((Number)right).longValue();
                } else {
                    return (char)left << ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left << (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [<<] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static Object rsh(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double ||
                        left instanceof Float || right instanceof Float ||
                        left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() >> ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() >> ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double || left instanceof Float || left instanceof Long) {
                    return ((Number)left).longValue() >> (char)right;
                } else {
                    return ((Number)left).intValue() >> (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (long)(char)left >> ((Number)right).longValue();
                } else {
                    return (char)left >> ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left >> (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [>>] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static Object ush(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double ||
                        left instanceof Float || right instanceof Float ||
                        left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() >>> ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() >>> ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double || left instanceof Float || left instanceof Long) {
                    return ((Number)left).longValue() >>> (char)right;
                } else {
                    return ((Number)left).intValue() >>> (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (long)(char)left >>> ((Number)right).longValue();
                } else {
                    return (char)left >>> ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left >>> (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [>>>] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static Object and(final Object left, final Object right) {
        if (left instanceof Boolean && right instanceof Boolean) {
            return (boolean)left && (boolean)right;
        } else if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double ||
                        left instanceof Float || right instanceof Float ||
                        left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() & ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() & ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double || left instanceof Float || left instanceof Long) {
                    return ((Number)left).longValue() & (char)right;
                } else {
                    return ((Number)left).intValue() & (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (char)left & ((Number)right).longValue();
                } else {
                    return (char)left & ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left & (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [&] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static Object xor(final Object left, final Object right) {
        if (left instanceof Boolean && right instanceof Boolean) {
            return (boolean)left ^ (boolean)right;
        } else if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double ||
                        left instanceof Float || right instanceof Float ||
                        left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() ^ ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() ^ ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double || left instanceof Float || left instanceof Long) {
                    return ((Number)left).longValue() ^ (char)right;
                } else {
                    return ((Number)left).intValue() ^ (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (char)left ^ ((Number)right).longValue();
                } else {
                    return (char)left ^ ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left ^ (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [^] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static Object or(final Object left, final Object right) {
        if (left instanceof Boolean && right instanceof Boolean) {
            return (boolean)left || (boolean)right;
        } else if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double ||
                        left instanceof Float || right instanceof Float ||
                        left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() | ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() | ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double || left instanceof Float || left instanceof Long) {
                    return ((Number)left).longValue() | (char)right;
                } else {
                    return ((Number)left).intValue() | (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (char)left | ((Number)right).longValue();
                } else {
                    return (char)left | ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left | (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [|] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static boolean eq(final Object left, final Object right) {
        if (left != null && right != null) {
            if (left instanceof Double) {
                if (right instanceof Number) {
                    return (double)left == ((Number)right).doubleValue();
                } else if (right instanceof Character) {
                    return (double)left == (char)right;
                }
            } else if (right instanceof Double) {
                if (left instanceof Number) {
                    return ((Number)left).doubleValue() == (double)right;
                } else if (left instanceof Character) {
                    return (char)left == ((Number)right).doubleValue();
                }
            } else if (left instanceof Float) {
                if (right instanceof Number) {
                    return (float)left == ((Number)right).floatValue();
                } else if (right instanceof Character) {
                    return (float)left == (char)right;
                }
            } else if (right instanceof Float) {
                if (left instanceof Number) {
                    return ((Number)left).floatValue() == (float)right;
                } else if (left instanceof Character) {
                    return (char)left == ((Number)right).floatValue();
                }
            } else if (left instanceof Long) {
                if (right instanceof Number) {
                    return (long)left == ((Number)right).longValue();
                } else if (right instanceof Character) {
                    return (long)left == (char)right;
                }
            } else if (right instanceof Long) {
                if (left instanceof Number) {
                    return ((Number)left).longValue() == (long)right;
                } else if (left instanceof Character) {
                    return (char)left == ((Number)right).longValue();
                }
            } else if (left instanceof Number) {
                if (right instanceof Number) {
                    return ((Number)left).intValue() == ((Number)right).intValue();
                } else if (right instanceof Character) {
                    return ((Number)left).intValue() == (char)right;
                }
            } else if (right instanceof Number && left instanceof Character) {
                return (char)left == ((Number)right).intValue();
            } else if (left instanceof Character && right instanceof Character) {
                return (char)left == (char)right;
            }

            return left.equals(right);
        }

        return left == null && right == null;
    }

    public static boolean lt(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number)left).doubleValue() < ((Number)right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number)left).floatValue() < ((Number)right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() < ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() < ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number)left).doubleValue() < (char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() < (char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() < (char)right;
                } else {
                    return ((Number)left).intValue() < (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char)left < ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (char)left < ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (char)left < ((Number)right).longValue();
                } else {
                    return (char)left < ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left < (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [<] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static boolean lte(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number)left).doubleValue() <= ((Number)right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number)left).floatValue() <= ((Number)right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() <= ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() <= ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number)left).doubleValue() <= (char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() <= (char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() <= (char)right;
                } else {
                    return ((Number)left).intValue() <= (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char)left <= ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (char)left <= ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (char)left <= ((Number)right).longValue();
                } else {
                    return (char)left <= ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left <= (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [<=] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static boolean gt(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number)left).doubleValue() > ((Number)right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number)left).floatValue() > ((Number)right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() > ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() > ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number)left).doubleValue() > (char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() > (char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() > (char)right;
                } else {
                    return ((Number)left).intValue() > (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char)left > ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (char)left > ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (char)left > ((Number)right).longValue();
                } else {
                    return (char)left > ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left > (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [>] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static boolean gte(final Object left, final Object right) {
        if (left instanceof Number) {
            if (right instanceof Number) {
                if (left instanceof Double || right instanceof Double) {
                    return ((Number)left).doubleValue() >= ((Number)right).doubleValue();
                } else if (left instanceof Float || right instanceof Float) {
                    return ((Number)left).floatValue() >= ((Number)right).floatValue();
                } else if (left instanceof Long || right instanceof Long) {
                    return ((Number)left).longValue() >= ((Number)right).longValue();
                } else {
                    return ((Number)left).intValue() >= ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                if (left instanceof Double) {
                    return ((Number)left).doubleValue() >= (char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() >= (char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() >= (char)right;
                } else {
                    return ((Number)left).intValue() >= (char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (char)left >= ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (char)left >= ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (char)left >= ((Number)right).longValue();
                } else {
                    return (char)left >= ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (char)left >= (char)right;
            }
        }

        throw new ClassCastException("Cannot apply [>] operation to types " +
                "[" + left.getClass().getCanonicalName() + "] and [" + right.getClass().getCanonicalName() + "].");
    }

    public static boolean DefToboolean(final Object value) {
        if (value instanceof Boolean) {
            return (boolean)value;
        } else if (value instanceof Character) {
            return ((char)value) != 0;
        } else {
            return ((Number)value).intValue() != 0;
        }
    }

    public static byte DefTobyte(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean)value) ? (byte)1 : 0;
        } else if (value instanceof Character) {
            return (byte)(char)value;
        } else {
            return ((Number)value).byteValue();
        }
    }

    public static short DefToshort(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean)value) ? (short)1 : 0;
        } else if (value instanceof Character) {
            return (short)(char)value;
        } else {
            return ((Number)value).shortValue();
        }
    }

    public static char DefTochar(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean)value) ? (char)1 : 0;
        } else if (value instanceof Character) {
            return ((Character)value);
        } else {
            return (char)((Number)value).intValue();
        }
    }

    public static int DefToint(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean)value) ? 1 : 0;
        } else if (value instanceof Character) {
            return (char)value;
        } else {
            return ((Number)value).intValue();
        }
    }

    public static long DefTolong(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean)value) ? 1L : 0;
        } else if (value instanceof Character) {
            return (char)value;
        } else {
            return ((Number)value).longValue();
        }
    }

    public static float DefTofloat(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean)value) ? (float)1 : 0;
        } else if (value instanceof Character) {
            return (char)value;
        } else {
            return ((Number)value).floatValue();
        }
    }

    public static double DefTodouble(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean)value) ? (double)1 : 0;
        } else if (value instanceof Character) {
            return (char)value;
        } else {
            return ((Number)value).doubleValue();
        }
    }

    public static Boolean DefToBoolean(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return (boolean)value;
        } else if (value instanceof Character) {
            return ((char)value) != 0;
        } else {
            return ((Number)value).intValue() != 0;
        }
    }

    public static Byte DefToByte(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean)value) ? (byte)1 : 0;
        } else if (value instanceof Character) {
            return (byte)(char)value;
        } else {
            return ((Number)value).byteValue();
        }
    }

    public static Short DefToShort(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean)value) ? (short)1 : 0;
        } else if (value instanceof Character) {
            return (short)(char)value;
        } else {
            return ((Number)value).shortValue();
        }
    }

    public static Character DefToCharacter(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean)value) ? (char)1 : 0;
        } else if (value instanceof Character) {
            return ((Character)value);
        } else {
            return (char)((Number)value).intValue();
        }
    }

    public static Integer DefToInteger(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean)value) ? 1 : 0;
        } else if (value instanceof Character) {
            return (int)(char)value;
        } else {
            return ((Number)value).intValue();
        }
    }

    public static Long DefToLong(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean)value) ? 1L : 0;
        } else if (value instanceof Character) {
            return (long)(char)value;
        } else {
            return ((Number)value).longValue();
        }
    }

    public static Float DefToFloat(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean)value) ? (float)1 : 0;
        } else if (value instanceof Character) {
            return (float)(char)value;
        } else {
            return ((Number)value).floatValue();
        }
    }

    public static Double DefToDouble(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Boolean) {
            return ((Boolean)value) ? (double)1 : 0;
        } else if (value instanceof Character) {
            return (double)(char)value;
        } else {
            return ((Number)value).doubleValue();
        }
    }
}
