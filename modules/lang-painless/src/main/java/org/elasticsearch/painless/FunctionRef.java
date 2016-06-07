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
import org.objectweb.asm.Handle;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;

/** 
 * computes "everything you need" to call LambdaMetaFactory, given an expected interface,
 * and reference class + method name
 */
public class FunctionRef {
    public final String invokedName;

    public final Type invokedType;
    public final MethodType invokedMethodType;

    public final Handle implMethod;
    public final MethodHandle implMethodHandle;

    public final Type samType;
    public final MethodType samMethodType;

    public final Type interfaceType;
    public final MethodType interfaceMethodType;
    
    public FunctionRef(Definition.Type expected, String type, String call) {
        boolean isCtorReference = "new".equals(call);
        // check its really a functional interface
        // for e.g. Comparable
        Method method = expected.struct.getFunctionalMethod();
        if (method == null) {
            throw new IllegalArgumentException("Cannot convert function reference [" + type + "::" + call + "] " +
                                               "to [" + expected.name + "], not a functional interface");
        }
        // e.g. compareTo
        invokedName = method.name;
        // e.g. (Object)Comparator
        invokedType = Type.getMethodType(expected.type);
        invokedMethodType = MethodType.methodType(expected.clazz);
        // e.g. (Object,Object)int
        interfaceType = Type.getMethodType(method.method.getDescriptor());
        interfaceMethodType = method.handle.type().dropParameterTypes(0, 1);
        // lookup requested method
        Definition.Struct struct = Definition.getType(type).struct;
        final Definition.Method impl;
        // ctor ref
        if (isCtorReference) {
            impl = struct.constructors.get(new Definition.MethodKey("<init>", method.arguments.size()));
        } else {
            // look for a static impl first
            Definition.Method staticImpl = struct.staticMethods.get(new Definition.MethodKey(call, method.arguments.size()));
            if (staticImpl == null) {
                // otherwise a virtual impl
                impl = struct.methods.get(new Definition.MethodKey(call, method.arguments.size()-1));
            } else {
                impl = staticImpl;
            }
        }
        if (impl == null) {
            throw new IllegalArgumentException("Unknown reference [" + type + "::" + call + "] matching " +
                                               "[" + expected + "]");
        }
        
        final int tag;
        if (isCtorReference) {
            tag = Opcodes.H_NEWINVOKESPECIAL;
        } else if (Modifier.isStatic(impl.modifiers)) {
            tag = Opcodes.H_INVOKESTATIC;
        } else {
            tag = Opcodes.H_INVOKEVIRTUAL;
        }
        implMethod = new Handle(tag, struct.type.getInternalName(), impl.name, impl.method.getDescriptor());
        implMethodHandle = impl.handle;
        if (isCtorReference) {
            samType = Type.getMethodType(interfaceType.getReturnType(), impl.method.getArgumentTypes());
            samMethodType = MethodType.methodType(interfaceMethodType.returnType(), impl.handle.type().parameterArray());
        } else if (Modifier.isStatic(impl.modifiers)) {
            samType = Type.getMethodType(impl.method.getReturnType(), impl.method.getArgumentTypes());
            samMethodType = impl.handle.type();
        } else {
            Type[] argTypes = impl.method.getArgumentTypes();
            Type[] params = new Type[argTypes.length + 1];
            System.arraycopy(argTypes, 0, params, 1, argTypes.length);
            params[0] = struct.type;
            samType = Type.getMethodType(impl.method.getReturnType(), params);
            samMethodType = impl.handle.type();
        }
    }
}
