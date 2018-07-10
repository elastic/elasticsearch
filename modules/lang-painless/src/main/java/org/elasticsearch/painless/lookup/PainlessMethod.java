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

import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Opcodes;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;

public class PainlessMethod {
    public final String name;
    public final PainlessClass owner;
    public final Class<?> augmentation;
    public final Class<?> rtn;
    public final List<Class<?>> arguments;
    public final org.objectweb.asm.commons.Method method;
    public final int modifiers;
    public final MethodHandle handle;

    public PainlessMethod(String name, PainlessClass owner, Class<?> augmentation, Class<?> rtn, List<Class<?>> arguments,
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
                params[i + 1] = PainlessLookup.defClassToObjectClass(arguments.get(i));
            }
            returnValue = PainlessLookup.defClassToObjectClass(rtn);
        } else if (Modifier.isStatic(modifiers)) {
            // static method: straightforward copy
            params = new Class<?>[arguments.size()];
            for (int i = 0; i < arguments.size(); i++) {
                params[i] = PainlessLookup.defClassToObjectClass(arguments.get(i));
            }
            returnValue = PainlessLookup.defClassToObjectClass(rtn);
        } else if ("<init>".equals(name)) {
            // constructor: returns the owner class
            params = new Class<?>[arguments.size()];
            for (int i = 0; i < arguments.size(); i++) {
                params[i] = PainlessLookup.defClassToObjectClass(arguments.get(i));
            }
            returnValue = owner.clazz;
        } else {
            // virtual/interface method: add receiver class
            params = new Class<?>[1 + arguments.size()];
            params[0] = owner.clazz;
            for (int i = 0; i < arguments.size(); i++) {
                params[i + 1] = PainlessLookup.defClassToObjectClass(arguments.get(i));
            }
            returnValue = PainlessLookup.defClassToObjectClass(rtn);
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
