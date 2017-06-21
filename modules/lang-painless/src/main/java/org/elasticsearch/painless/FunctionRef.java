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
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.api.Augmentation;

import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;

import static org.elasticsearch.painless.WriterConstants.CLASS_NAME;
import static org.objectweb.asm.Opcodes.H_INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.H_INVOKESTATIC;
import static org.objectweb.asm.Opcodes.H_INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.H_NEWINVOKESPECIAL;

/**
 * Reference to a function or lambda.
 * <p>
 * Once you have created one of these, you have "everything you need" to call {@link LambdaBootstrap}
 * either statically from bytecode with invokedynamic, or at runtime from Java.
 */
public class FunctionRef {

    /** functional interface method name */
    public final String interfaceMethodName;
    /** factory (CallSite) method signature */
    public final MethodType factoryMethodType;
    /** functional interface method signature */
    public final MethodType interfaceMethodType;
    /** class of the delegate method to be called */
    public final String delegateClassName;
    /** the invocation type of the delegate method */
    public final int delegateInvokeType;
    /** the name of the delegate method */
    public final String delegateMethodName;
    /** delegate method signature */
    public final MethodType delegateMethodType;

    /** interface method */
    public final Method interfaceMethod;
    /** delegate method */
    public final Method delegateMethod;

    /** factory method type descriptor */
    public final String factoryDescriptor;
    /** functional interface method as type */
    public final org.objectweb.asm.Type interfaceType;
    /** delegate method type method as type */
    public final org.objectweb.asm.Type delegateType;

    /**
     * Creates a new FunctionRef, which will resolve {@code type::call} from the whitelist.
     * @param definition the whitelist against which this script is being compiled
     * @param expected functional interface type to implement.
     * @param type the left hand side of a method reference expression
     * @param call the right hand side of a method reference expression
     * @param numCaptures number of captured arguments
     */
    public FunctionRef(Definition definition, Type expected, String type, String call, int numCaptures) {
        this(expected, expected.struct.getFunctionalMethod(), lookup(definition, expected, type, call, numCaptures > 0), numCaptures);
    }

    /**
     * Creates a new FunctionRef (already resolved)
     * @param expected functional interface type to implement
     * @param interfaceMethod functional interface method
     * @param delegateMethod implementation method
     * @param numCaptures number of captured arguments
     */
    public FunctionRef(Type expected, Method interfaceMethod, Method delegateMethod, int numCaptures) {
        MethodType delegateMethodType = delegateMethod.getMethodType();

        interfaceMethodName = interfaceMethod.name;
        factoryMethodType = MethodType.methodType(expected.clazz,
                delegateMethodType.dropParameterTypes(numCaptures, delegateMethodType.parameterCount()));
        interfaceMethodType = interfaceMethod.getMethodType().dropParameterTypes(0, 1);

        // the Painless$Script class can be inferred if owner is null
        if (delegateMethod.owner == null) {
            delegateClassName = CLASS_NAME;
        } else if (delegateMethod.augmentation != null) {
            delegateClassName = delegateMethod.augmentation.getName();
        } else {
            delegateClassName = delegateMethod.owner.clazz.getName();
        }

        if ("<init>".equals(delegateMethod.name)) {
            delegateInvokeType = H_NEWINVOKESPECIAL;
        } else if (Modifier.isStatic(delegateMethod.modifiers)) {
            delegateInvokeType = H_INVOKESTATIC;
        } else if (delegateMethod.owner.clazz.isInterface()) {
            delegateInvokeType = H_INVOKEINTERFACE;
        } else {
            delegateInvokeType = H_INVOKEVIRTUAL;
        }

        delegateMethodName = delegateMethod.name;
        this.delegateMethodType = delegateMethodType.dropParameterTypes(0, numCaptures);

        this.interfaceMethod = interfaceMethod;
        this.delegateMethod = delegateMethod;

        factoryDescriptor = factoryMethodType.toMethodDescriptorString();
        interfaceType = org.objectweb.asm.Type.getMethodType(interfaceMethodType.toMethodDescriptorString());
        delegateType = org.objectweb.asm.Type.getMethodType(this.delegateMethodType.toMethodDescriptorString());
    }

    /**
     * Creates a new FunctionRef (low level).
     * It is for runtime use only.
     */
    public FunctionRef(Type expected, Method interfaceMethod, String delegateMethodName, MethodType delegateMethodType, int numCaptures) {
        interfaceMethodName = interfaceMethod.name;
        factoryMethodType = MethodType.methodType(expected.clazz,
            delegateMethodType.dropParameterTypes(numCaptures, delegateMethodType.parameterCount()));
        interfaceMethodType = interfaceMethod.getMethodType().dropParameterTypes(0, 1);

        delegateClassName = CLASS_NAME;
        delegateInvokeType = H_INVOKESTATIC;
        this.delegateMethodName = delegateMethodName;
        this.delegateMethodType = delegateMethodType.dropParameterTypes(0, numCaptures);

        this.interfaceMethod = null;
        delegateMethod = null;

        factoryDescriptor = null;
        interfaceType = null;
        delegateType = null;
    }

    /**
     * Looks up {@code type::call} from the whitelist, and returns a matching method.
     */
    private static Definition.Method lookup(Definition definition, Definition.Type expected,
                                            String type, String call, boolean receiverCaptured) {
        // check its really a functional interface
        // for e.g. Comparable
        Method method = expected.struct.getFunctionalMethod();
        if (method == null) {
            throw new IllegalArgumentException("Cannot convert function reference [" + type + "::" + call + "] " +
                                               "to [" + expected.name + "], not a functional interface");
        }

        // lookup requested method
        Definition.Struct struct = definition.getType(type).struct;
        final Definition.Method impl;
        // ctor ref
        if ("new".equals(call)) {
            impl = struct.constructors.get(new Definition.MethodKey("<init>", method.arguments.size()));
        } else {
            // look for a static impl first
            Definition.Method staticImpl = struct.staticMethods.get(new Definition.MethodKey(call, method.arguments.size()));
            if (staticImpl == null) {
                // otherwise a virtual impl
                final int arity;
                if (receiverCaptured) {
                    // receiver captured
                    arity = method.arguments.size();
                } else {
                    // receiver passed
                    arity = method.arguments.size() - 1;
                }
                impl = struct.methods.get(new Definition.MethodKey(call, arity));
            } else {
                impl = staticImpl;
            }
        }
        if (impl == null) {
            throw new IllegalArgumentException("Unknown reference [" + type + "::" + call + "] matching " +
                                               "[" + expected + "]");
        }
        return impl;
    }
}
