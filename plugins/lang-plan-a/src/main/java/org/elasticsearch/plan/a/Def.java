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

package org.elasticsearch.plan.a;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.plan.a.Definition.*;

public class Def {
    public static Object methodCall(final Object owner, final String name, final Definition definition,
                                    final Object[] arguments, final boolean[] typesafe) {
        final Method method = getMethod(owner, name, definition);

        if (method == null) {
            throw new IllegalArgumentException("Unable to find dynamic method [" + name + "] " +
                    "for class [" + owner.getClass().getCanonicalName() + "].");
        }

        final MethodHandle handle = method.handle;
        final List<Type> types = method.arguments;
        final Object[] parameters = new Object[arguments.length + 1];

        parameters[0] = owner;

        if (types.size() != arguments.length) {
            throw new IllegalArgumentException("When dynamically calling [" + name + "] from class " +
                    "[" + owner.getClass() + "] expected [" + types.size() + "] arguments," +
                    " but found [" + arguments.length + "].");
        }

        try {
            for (int count = 0; count < arguments.length; ++count) {
                if (typesafe[count]) {
                    parameters[count + 1] = arguments[count];
                } else {
                    final Transform transform = getTransform(arguments[count].getClass(), types.get(count).clazz, definition);
                    parameters[count + 1] = transform == null ? arguments[count] : transform.method.handle.invoke(arguments[count]);
                }
            }

            return handle.invokeWithArguments(parameters);
        } catch (Throwable throwable) {
            throw new IllegalArgumentException("Error invoking method [" + name + "] " +
                    "with owner class [" + owner.getClass().getCanonicalName() + "].", throwable);
        }
    }

    @SuppressWarnings("unchecked")
    public static void fieldStore(final Object owner, Object value, final String name,
                                  final Definition definition, final boolean typesafe) {
        final Field field = getField(owner, name, definition);
        MethodHandle handle = null;

        if (field == null) {
            final String set = "set" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
            final Method method = getMethod(owner, set, definition);

            if (method != null) {
                handle = method.handle;
            }
        } else {
            handle = field.setter;
        }

        if (handle != null) {
            try {
                if (!typesafe) {
                    final Transform transform = getTransform(value.getClass(), handle.type().parameterType(1), definition);

                    if (transform != null) {
                        value = transform.method.handle.invoke(value);
                    }
                }

                handle.invoke(owner, value);
            } catch (Throwable throwable) {
                throw new IllegalArgumentException("Error storing value [" + value + "] " +
                        "in field [" + name + "] with owner class [" + owner.getClass() + "].", throwable);
            }
        } else if (owner instanceof Map) {
            ((Map)owner).put(name, value);
        } else if (owner instanceof List) {
            try {
                final int index = Integer.parseInt(name);
                ((List)owner).add(index, value);
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException( "Illegal list shortcut value [" + name + "].");
            }
        } else {
            throw new IllegalArgumentException("Unable to find dynamic field [" + name + "] " +
                    "for class [" + owner.getClass().getCanonicalName() + "].");
        }
    }

    @SuppressWarnings("unchecked")
    public static Object fieldLoad(final Object owner, final String name, final Definition definition) {
        if (owner.getClass().isArray() && "length".equals(name)) {
            return Array.getLength(owner);
        } else {
            final Field field = getField(owner, name, definition);
            MethodHandle handle;

            if (field == null) {
                final String get = "get" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
                final Method method = getMethod(owner, get, definition);

                if (method != null) {
                    handle = method.handle;
                } else if (owner instanceof Map) {
                    return ((Map)owner).get(name);
                } else if (owner instanceof List) {
                    try {
                        final int index = Integer.parseInt(name);

                        return ((List)owner).get(index);
                    } catch (NumberFormatException exception) {
                        throw new IllegalArgumentException( "Illegal list shortcut value [" + name + "].");
                    }
                } else {
                    throw new IllegalArgumentException("Unable to find dynamic field [" + name + "] " +
                            "for class [" + owner.getClass().getCanonicalName() + "].");
                }
            } else {
                handle = field.getter;
            }

            if (handle == null) {
                throw new IllegalArgumentException(
                        "Unable to read from field [" + name + "] with owner class [" + owner.getClass() + "].");
            } else {
                try {
                    return handle.invoke(owner);
                } catch (final Throwable throwable) {
                    throw new IllegalArgumentException("Error loading value from " +
                            "field [" + name + "] with owner class [" + owner.getClass() + "].", throwable);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void arrayStore(final Object array, Object index, Object value, final Definition definition,
                                  final boolean indexsafe, final boolean valuesafe) {
        if (array instanceof Map) {
            ((Map)array).put(index, value);
        } else {
            try {
                if (!indexsafe) {
                    final Transform transform = getTransform(index.getClass(), Integer.class, definition);

                    if (transform != null) {
                        index = transform.method.handle.invoke(index);
                    }
                }
            } catch (final Throwable throwable) {
                throw new IllegalArgumentException(
                        "Error storing value [" + value + "] in list using index [" + index + "].", throwable);
            }

            if (array.getClass().isArray()) {
                try {
                    if (!valuesafe) {
                        final Transform transform = getTransform(value.getClass(), array.getClass().getComponentType(), definition);

                        if (transform != null) {
                            value = transform.method.handle.invoke(value);
                        }
                    }

                    Array.set(array, (int)index, value);
                } catch (final Throwable throwable) {
                    throw new IllegalArgumentException("Error storing value [" + value + "] " +
                            "in array class [" + array.getClass().getCanonicalName() + "].", throwable);
                }
            } else if (array instanceof List) {
                ((List)array).add((int)index, value);
            } else {
                throw new IllegalArgumentException("Attempting to address a non-array type " +
                        "[" + array.getClass().getCanonicalName() + "] as an array.");
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static Object arrayLoad(final Object array, Object index,
                                   final Definition definition, final boolean indexsafe) {
        if (array instanceof Map) {
            return ((Map)array).get(index);
        } else {
            try {
                if (!indexsafe) {
                    final Transform transform = getTransform(index.getClass(), Integer.class, definition);

                    if (transform != null) {
                        index = transform.method.handle.invoke(index);
                    }
                }
            } catch (final Throwable throwable) {
                throw new IllegalArgumentException(
                        "Error loading value using index [" + index + "].", throwable);
            }

            if (array.getClass().isArray()) {
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
    }

    public static Method getMethod(final Object owner, final String name, final Definition definition) {
        Struct struct = null;
        Class<?> clazz = owner.getClass();
        Method method = null;

        while (clazz != null) {
            struct = definition.classes.get(clazz);

            if (struct != null) {
                method = struct.methods.get(name);

                if (method != null) {
                    break;
                }
            }

            for (final Class iface : clazz.getInterfaces()) {
                struct = definition.classes.get(iface);

                if (struct != null) {
                    method = struct.methods.get(name);

                    if (method != null) {
                        break;
                    }
                }
            }

            if (struct != null) {
                method = struct.methods.get(name);

                if (method != null) {
                    break;
                }
            }

            clazz = clazz.getSuperclass();
        }

        if (struct == null) {
            throw new IllegalArgumentException("Unable to find a dynamic struct for class [" + owner.getClass() + "].");
        }

        return method;
    }

    public static Field getField(final Object owner, final String name, final Definition definition) {
        Struct struct = null;
        Class<?> clazz = owner.getClass();
        Field field = null;

        while (clazz != null) {
            struct = definition.classes.get(clazz);

            if (struct != null) {
                field = struct.members.get(name);

                if (field != null) {
                    break;
                }
            }

            for (final Class iface : clazz.getInterfaces()) {
                struct = definition.classes.get(iface);

                if (struct != null) {
                    field = struct.members.get(name);

                    if (field != null) {
                        break;
                    }
                }
            }

            if (struct != null) {
                field = struct.members.get(name);

                if (field != null) {
                    break;
                }
            }

            clazz = clazz.getSuperclass();
        }

        if (struct == null) {
            throw new IllegalArgumentException("Unable to find a dynamic struct for class [" + owner.getClass() + "].");
        }

        return field;
    }

    public static Transform getTransform(Class<?> fromClass, Class<?> toClass, final Definition definition) {
        Struct fromStruct = null;
        Struct toStruct = null;

        if (fromClass.equals(toClass)) {
            return null;
        }

        while (fromClass != null) {
            fromStruct = definition.classes.get(fromClass);

            if (fromStruct != null) {
                break;
            }

            for (final Class iface : fromClass.getInterfaces()) {
                fromStruct = definition.classes.get(iface);

                if (fromStruct != null) {
                    break;
                }
            }

            if (fromStruct != null) {
                break;
            }

            fromClass = fromClass.getSuperclass();
        }

        if (fromStruct != null) {
            while (toClass != null) {
                toStruct = definition.classes.get(toClass);

                if (toStruct != null) {
                    break;
                }

                for (final Class iface : toClass.getInterfaces()) {
                    toStruct = definition.classes.get(iface);

                    if (toStruct != null) {
                        break;
                    }
                }

                if (toStruct != null) {
                    break;
                }

                toClass = toClass.getSuperclass();
            }
        }

        if (toStruct != null) {
            final Type fromType = definition.getType(fromStruct.name);
            final Type toType = definition.getType(toStruct.name);
            final Cast cast = new Cast(fromType, toType);

            return definition.transforms.get(cast);
        }

        return null;
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
                    return ((Number)left).doubleValue() * (double)(char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() * (float)(char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() * (long)(char)right;
                } else {
                    return ((Number)left).intValue() * (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (double)(char)left * ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (float)(char)left * ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (long)(char)left * ((Number)right).longValue();
                } else {
                    return (int)(char)left * ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left * (int)(char)right;
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
                    return ((Number)left).doubleValue() / (double)(char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() / (float)(char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() / (long)(char)right;
                } else {
                    return ((Number)left).intValue() / (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (double)(char)left / ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (float)(char)left / ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (long)(char)left / ((Number)right).longValue();
                } else {
                    return (int)(char)left / ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left / (int)(char)right;
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
                    return ((Number)left).doubleValue() % (double)(char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() % (float)(char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() % (long)(char)right;
                } else {
                    return ((Number)left).intValue() % (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (double)(char)left % ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (float)(char)left % ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (long)(char)left % ((Number)right).longValue();
                } else {
                    return (int)(char)left % ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left % (int)(char)right;
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
                    return ((Number)left).doubleValue() + (double)(char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() + (float)(char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() + (long)(char)right;
                } else {
                    return ((Number)left).intValue() + (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (double)(char)left + ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (float)(char)left + ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (long)(char)left + ((Number)right).longValue();
                } else {
                    return (int)(char)left + ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left + (int)(char)right;
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
                    return ((Number)left).doubleValue() - (double)(char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() - (float)(char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() - (long)(char)right;
                } else {
                    return ((Number)left).intValue() - (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (double)(char)left - ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (float)(char)left - ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (long)(char)left - ((Number)right).longValue();
                } else {
                    return (int)(char)left - ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left - (int)(char)right;
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
                    return ((Number)left).longValue() << (long)(char)right;
                } else {
                    return ((Number)left).intValue() << (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (long)(char)left << ((Number)right).longValue();
                } else {
                    return (int)(char)left << ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left << (int)(char)right;
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
                    return ((Number)left).longValue() >> (long)(char)right;
                } else {
                    return ((Number)left).intValue() >> (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (long)(char)left >> ((Number)right).longValue();
                } else {
                    return (int)(char)left >> ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left >> (int)(char)right;
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
                    return ((Number)left).longValue() >>> (long)(char)right;
                } else {
                    return ((Number)left).intValue() >>> (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (long)(char)left >>> ((Number)right).longValue();
                } else {
                    return (int)(char)left >>> ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left >>> (int)(char)right;
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
                    return ((Number)left).longValue() & (long)(char)right;
                } else {
                    return ((Number)left).intValue() & (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (long)(char)left & ((Number)right).longValue();
                } else {
                    return (int)(char)left & ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left & (int)(char)right;
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
                    return ((Number)left).longValue() ^ (long)(char)right;
                } else {
                    return ((Number)left).intValue() ^ (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (long)(char)left ^ ((Number)right).longValue();
                } else {
                    return (int)(char)left ^ ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left ^ (int)(char)right;
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
                    return ((Number)left).longValue() | (long)(char)right;
                } else {
                    return ((Number)left).intValue() | (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double || right instanceof Float || right instanceof Long) {
                    return (long)(char)left | ((Number)right).longValue();
                } else {
                    return (int)(char)left | ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left | (int)(char)right;
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
                    return (double)left == (double)(char)right;
                }
            } else if (right instanceof Double) {
                if (left instanceof Number) {
                    return ((Number)left).doubleValue() == (double)right;
                } else if (left instanceof Character) {
                    return (double)(char)left == ((Number)right).doubleValue();
                }
            } else if (left instanceof Float) {
                if (right instanceof Number) {
                    return (float)left == ((Number)right).floatValue();
                } else if (right instanceof Character) {
                    return (float)left == (float)(char)right;
                }
            } else if (right instanceof Float) {
                if (left instanceof Number) {
                    return ((Number)left).floatValue() == (float)right;
                } else if (left instanceof Character) {
                    return (float)(char)left == ((Number)right).floatValue();
                }
            } else if (left instanceof Long) {
                if (right instanceof Number) {
                    return (long)left == ((Number)right).longValue();
                } else if (right instanceof Character) {
                    return (long)left == (long)(char)right;
                }
            } else if (right instanceof Long) {
                if (left instanceof Number) {
                    return ((Number)left).longValue() == (long)right;
                } else if (left instanceof Character) {
                    return (long)(char)left == ((Number)right).longValue();
                }
            } else if (left instanceof Number) {
                if (right instanceof Number) {
                    return ((Number)left).intValue() == ((Number)right).intValue();
                } else if (right instanceof Character) {
                    return ((Number)left).intValue() == (int)(char)right;
                }
            } else if (right instanceof Number && left instanceof Character) {
                return (int)(char)left == ((Number)right).intValue();
            } else if (left instanceof Character && right instanceof Character) {
                return (int)(char)left == (int)(char)right;
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
                    return ((Number)left).doubleValue() < (double)(char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() < (float)(char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() < (long)(char)right;
                } else {
                    return ((Number)left).intValue() < (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (double)(char)left < ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (float)(char)left < ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (long)(char)left < ((Number)right).longValue();
                } else {
                    return (int)(char)left < ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left < (int)(char)right;
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
                    return ((Number)left).doubleValue() <= (double)(char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() <= (float)(char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() <= (long)(char)right;
                } else {
                    return ((Number)left).intValue() <= (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (double)(char)left <= ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (float)(char)left <= ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (long)(char)left <= ((Number)right).longValue();
                } else {
                    return (int)(char)left <= ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left <= (int)(char)right;
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
                    return ((Number)left).doubleValue() > (double)(char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() > (float)(char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() > (long)(char)right;
                } else {
                    return ((Number)left).intValue() > (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (double)(char)left > ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (float)(char)left > ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (long)(char)left > ((Number)right).longValue();
                } else {
                    return (int)(char)left > ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left > (int)(char)right;
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
                    return ((Number)left).doubleValue() >= (double)(char)right;
                } else if (left instanceof Float) {
                    return ((Number)left).floatValue() >= (float)(char)right;
                } else if (left instanceof Long) {
                    return ((Number)left).longValue() >= (long)(char)right;
                } else {
                    return ((Number)left).intValue() >= (int)(char)right;
                }
            }
        } else if (left instanceof Character) {
            if (right instanceof Number) {
                if (right instanceof Double) {
                    return (double)(char)left >= ((Number)right).doubleValue();
                } else if (right instanceof Float) {
                    return (float)(char)left >= ((Number)right).floatValue();
                } else if (right instanceof Long) {
                    return (long)(char)left >= ((Number)right).longValue();
                } else {
                    return (int)(char)left >= ((Number)right).intValue();
                }
            } else if (right instanceof Character) {
                return (int)(char)left >= (int)(char)right;
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
            return (int)(char)value;
        } else {
            return ((Number)value).intValue();
        }
    }

    public static long DefTolong(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean)value) ? 1L : 0;
        } else if (value instanceof Character) {
            return (long)(char)value;
        } else {
            return ((Number)value).longValue();
        }
    }

    public static float DefTofloat(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean)value) ? (float)1 : 0;
        } else if (value instanceof Character) {
            return (float)(char)value;
        } else {
            return ((Number)value).floatValue();
        }
    }

    public static double DefTodouble(final Object value) {
        if (value instanceof Boolean) {
            return ((Boolean)value) ? (double)1 : 0;
        } else if (value instanceof Character) {
            return (double)(char)value;
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
