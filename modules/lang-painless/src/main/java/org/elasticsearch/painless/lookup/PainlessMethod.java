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
import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;

public class PainlessMethod {
    public final String name;
    public final Class<?> target;
    public final Class<?> augmentation;
    public final Class<?> rtn;
    public final List<Class<?>> arguments;
    public final org.objectweb.asm.commons.Method method;
    public final int modifiers;
    public final MethodHandle handle;
    public final MethodType methodType;

    public PainlessMethod(String name, Class<?> target, Class<?> augmentation, Class<?> rtn, List<Class<?>> arguments,
                          org.objectweb.asm.commons.Method method, int modifiers, MethodHandle handle, MethodType methodType) {
        this.name = name;
        this.augmentation = augmentation;
        this.target = target;
        this.rtn = rtn;
        this.arguments = Collections.unmodifiableList(arguments);
        this.method = method;
        this.modifiers = modifiers;
        this.handle = handle;
        this.methodType = methodType;
    }

    public void write(MethodWriter writer) {
        final org.objectweb.asm.Type type;
        final Class<?> clazz;
        if (augmentation != null) {
            assert Modifier.isStatic(modifiers);
            clazz = augmentation;
            type = org.objectweb.asm.Type.getType(augmentation);
        } else {
            clazz = target;
            type = Type.getType(target);
        }

        if (Modifier.isStatic(modifiers)) {
            // invokeStatic assumes that the owner class is not an interface, so this is a
            // special case for interfaces where the interface method boolean needs to be set to
            // true to reference the appropriate class constant when calling a static interface
            // method since java 8 did not check, but java 9 and 10 do
            if (Modifier.isInterface(clazz.getModifiers())) {
                writer.visitMethodInsn(Opcodes.INVOKESTATIC,
                        type.getInternalName(), name, methodType.toMethodDescriptorString(), true);
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
