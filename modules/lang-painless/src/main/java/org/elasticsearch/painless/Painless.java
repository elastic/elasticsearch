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

import org.objectweb.asm.Opcodes;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Painless {

    /**
     * Key for looking up a method.
     * <p>
     * Methods are keyed on both name and arity, and can be overloaded once per arity.
     * This allows signatures such as {@code String.indexOf(String) vs String.indexOf(String, int)}.
     * <p>
     * It is less flexible than full signature overloading where types can differ too, but
     * better than just the name, and overloading types adds complexity to users, too.
     */
    public static final class MethodKey {
        public final String name;
        public final int arity;

        /**
         * Create a new lookup key
         * @param name name of the method
         * @param arity number of parameters
         */
        public MethodKey(String name, int arity) {
            this.name = Objects.requireNonNull(name);
            this.arity = arity;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + arity;
            result = prime * result + name.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (getClass() != obj.getClass()) return false;
            MethodKey other = (MethodKey) obj;
            if (arity != other.arity) return false;
            if (!name.equals(other.name)) return false;
            return true;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(name);
            sb.append('/');
            sb.append(arity);
            return sb.toString();
        }
    }

    public static class Method {
        public final String name;
        public final Struct owner;
        public final Class<?> augmentation;
        public final Class<?> rtn;
        public final List<Class<?>> arguments;
        public final org.objectweb.asm.commons.Method method;
        public final int modifiers;
        public final MethodHandle handle;

        public Method(String name, Struct owner, Class<?> augmentation, Class<?> rtn, List<Class<?>> arguments,
                      org.objectweb.asm.commons.Method method, int modifiers, MethodHandle handle) {
            this.name = name;
            this.augmentation = augmentation;
            this.owner = owner;
            this.rtn = rtn;
            this.arguments = Collections.unmodifiableList(arguments);
            this.method = method;
            this.modifiers = modifiers;
            this.handle = handle;
        }

        /**
         * Returns MethodType for this method.
         * <p>
         * This works even for user-defined Methods (where the MethodHandle is null).
         */
        public MethodType getMethodType() {
            // we have a methodhandle already (e.g. whitelisted class)
            // just return its type
            if (handle != null) {
                return handle.type();
            }
            // otherwise compute it
            final Class<?> params[];
            final Class<?> returnValue;
            if (augmentation != null) {
                // static method disguised as virtual/interface method
                params = new Class<?>[1 + arguments.size()];
                params[0] = augmentation;
                for (int i = 0; i < arguments.size(); i++) {
                    params[i + 1] = Definition.defClassToObjectClass(arguments.get(i));
                }
                returnValue = Definition.defClassToObjectClass(rtn);
            } else if (Modifier.isStatic(modifiers)) {
                // static method: straightforward copy
                params = new Class<?>[arguments.size()];
                for (int i = 0; i < arguments.size(); i++) {
                    params[i] = Definition.defClassToObjectClass(arguments.get(i));
                }
                returnValue = Definition.defClassToObjectClass(rtn);
            } else if ("<init>".equals(name)) {
                // constructor: returns the owner class
                params = new Class<?>[arguments.size()];
                for (int i = 0; i < arguments.size(); i++) {
                    params[i] = Definition.defClassToObjectClass(arguments.get(i));
                }
                returnValue = owner.clazz;
            } else {
                // virtual/interface method: add receiver class
                params = new Class<?>[1 + arguments.size()];
                params[0] = owner.clazz;
                for (int i = 0; i < arguments.size(); i++) {
                    params[i + 1] = Definition.defClassToObjectClass(arguments.get(i));
                }
                returnValue = Definition.defClassToObjectClass(rtn);
            }
            return MethodType.methodType(returnValue, params);
        }

        public void write(MethodWriter writer) {
            final org.objectweb.asm.Type type;
            final Class<?> clazz;
            if (augmentation != null) {
                assert Modifier.isStatic(modifiers);
                clazz = augmentation;
                type = org.objectweb.asm.Type.getType(augmentation);
            } else {
                clazz = owner.clazz;
                type = owner.type;
            }

            if (Modifier.isStatic(modifiers)) {
                // invokeStatic assumes that the owner class is not an interface, so this is a
                // special case for interfaces where the interface method boolean needs to be set to
                // true to reference the appropriate class constant when calling a static interface
                // method since java 8 did not check, but java 9 and 10 do
                if (Modifier.isInterface(clazz.getModifiers())) {
                    writer.visitMethodInsn(Opcodes.INVOKESTATIC,
                            type.getInternalName(), name, getMethodType().toMethodDescriptorString(), true);
                } else {
                    writer.invokeStatic(type, method);
                }
            } else if (Modifier.isInterface(clazz.getModifiers())) {
                writer.invokeInterface(type, method);
            } else {
                writer.invokeVirtual(type, method);
            }
        }
    }

    public static final class Field {
        public final String name;
        public final Struct owner;
        public final Class<?> clazz;
        public final String javaName;
        public final int modifiers;
        public final MethodHandle getter;
        public final MethodHandle setter;

        public Field(String name, String javaName, Struct owner, Class<?> clazz, int modifiers, MethodHandle getter, MethodHandle setter) {
            this.name = name;
            this.javaName = javaName;
            this.owner = owner;
            this.clazz = clazz;
            this.modifiers = modifiers;
            this.getter = getter;
            this.setter = setter;
        }
    }

    public static final class Struct {
        public final String name;
        public final Class<?> clazz;
        public final org.objectweb.asm.Type type;

        public final Map<MethodKey, Method> constructors;
        public final Map<MethodKey, Method> staticMethods;
        public final Map<MethodKey, Method> methods;

        public final Map<String, Field> staticMembers;
        public final Map<String, Field> members;

        public final Map<String, MethodHandle> getters;
        public final Map<String, MethodHandle> setters;

        public final Method functionalMethod;

        public Struct(String name, Class<?> clazz, org.objectweb.asm.Type type) {
            this.name = name;
            this.clazz = clazz;
            this.type = type;

            constructors = new HashMap<>();
            staticMethods = new HashMap<>();
            methods = new HashMap<>();

            staticMembers = new HashMap<>();
            members = new HashMap<>();

            getters = new HashMap<>();
            setters = new HashMap<>();

            functionalMethod = null;
        }

        private Struct(Struct struct, Method functionalMethod) {
            name = struct.name;
            clazz = struct.clazz;
            type = struct.type;

            constructors = Collections.unmodifiableMap(struct.constructors);
            staticMethods = Collections.unmodifiableMap(struct.staticMethods);
            methods = Collections.unmodifiableMap(struct.methods);

            staticMembers = Collections.unmodifiableMap(struct.staticMembers);
            members = Collections.unmodifiableMap(struct.members);

            getters = Collections.unmodifiableMap(struct.getters);
            setters = Collections.unmodifiableMap(struct.setters);

            this.functionalMethod = functionalMethod;
        }

        public Struct freeze(Method functionalMethod) {
            return new Struct(this, functionalMethod);
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }

            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            Struct struct = (Struct)object;

            return name.equals(struct.name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }
    }

    public static class Cast {

        /** Create a standard cast with no boxing/unboxing. */
        public static Cast standard(Class<?> from, Class<?> to, boolean explicit) {
            return new Cast(from, to, explicit, null, null, null, null);
        }

        /** Create a cast where the from type will be unboxed, and then the cast will be performed. */
        public static Cast unboxFrom(Class<?> from, Class<?> to, boolean explicit, Class<?> unboxFrom) {
            return new Cast(from, to, explicit, unboxFrom, null, null, null);
        }

        /** Create a cast where the to type will be unboxed, and then the cast will be performed. */
        public static Cast unboxTo(Class<?> from, Class<?> to, boolean explicit, Class<?> unboxTo) {
            return new Cast(from, to, explicit, null, unboxTo, null, null);
        }

        /** Create a cast where the from type will be boxed, and then the cast will be performed. */
        public static Cast boxFrom(Class<?> from, Class<?> to, boolean explicit, Class<?> boxFrom) {
            return new Cast(from, to, explicit, null, null, boxFrom, null);
        }

        /** Create a cast where the to type will be boxed, and then the cast will be performed. */
        public static Cast boxTo(Class<?> from, Class<?> to, boolean explicit, Class<?> boxTo) {
            return new Cast(from, to, explicit, null, null, null, boxTo);
        }

        public final Class<?> from;
        public final Class<?> to;
        public final boolean explicit;
        public final Class<?> unboxFrom;
        public final Class<?> unboxTo;
        public final Class<?> boxFrom;
        public final Class<?> boxTo;

        private Cast(Class<?> from, Class<?> to, boolean explicit, Class<?> unboxFrom, Class<?> unboxTo, Class<?> boxFrom, Class<?> boxTo) {
            this.from = from;
            this.to = to;
            this.explicit = explicit;
            this.unboxFrom = unboxFrom;
            this.unboxTo = unboxTo;
            this.boxFrom = boxFrom;
            this.boxTo = boxTo;
        }
    }
}
