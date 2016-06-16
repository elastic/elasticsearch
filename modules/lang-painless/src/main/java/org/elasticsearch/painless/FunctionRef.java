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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;

/** 
 * Reference to a function or lambda. 
 * <p>
 * Once you have created one of these, you have "everything you need" to call LambdaMetaFactory
 * either statically from bytecode with invokedynamic, or at runtime from Java.  
 */
public class FunctionRef {
    /** Function Object's method name */
    public final String invokedName;
    /** CallSite signature */
    public final MethodType invokedType;
    /** Implementation method */
    public final MethodHandle implMethod;
    /** Function Object's method signature */
    public final MethodType samMethodType;
    /** When bridging is required, request this bridge interface */
    public final MethodType interfaceMethodType;
    
    /** ASM "Handle" to the method, for the constant pool */
    public final Handle implMethodASM;
    
    /**
     * Creates a new FunctionRef, which will resolve {@code type::call} from the whitelist.
     * @param expected interface type to implement.
     * @param type the left hand side of a method reference expression
     * @param call the right hand side of a method reference expression
     * @param captures captured arguments
     */    
    public FunctionRef(Definition.Type expected, String type, String call, Class<?>... captures) {
        this(expected, expected.struct.getFunctionalMethod(), lookup(expected, type, call, captures.length > 0), captures);
    }

    /**
     * Creates a new FunctionRef (already resolved)
     * @param expected interface type to implement
     * @param method functional interface method
     * @param impl implementation method
     * @param captures captured arguments
     */   
    public FunctionRef(Definition.Type expected, Definition.Method method, Definition.Method impl, Class<?>... captures) {
        // e.g. compareTo
        invokedName = method.name;
        // e.g. (Object)Comparator
        invokedType = MethodType.methodType(expected.clazz, captures);
        // e.g. (Object,Object)int
        interfaceMethodType = method.getMethodType().dropParameterTypes(0, 1);

        final int tag;
        if ("<init>".equals(impl.name)) {
            tag = Opcodes.H_NEWINVOKESPECIAL;
        } else if (Modifier.isStatic(impl.modifiers)) {
            tag = Opcodes.H_INVOKESTATIC;
        } else if (impl.owner.clazz.isInterface()) {
            tag = Opcodes.H_INVOKEINTERFACE;
        } else {
            tag = Opcodes.H_INVOKEVIRTUAL;
        }
        final String owner;
        final boolean ownerIsInterface;
        if (impl.owner == null) {
            // owner == null: script class itself
            ownerIsInterface = false;
            owner = WriterConstants.CLASS_TYPE.getInternalName();
        } else {
            ownerIsInterface = impl.owner.clazz.isInterface();
            owner = impl.owner.type.getInternalName();
        }
        implMethodASM = new Handle(tag, owner, impl.name, impl.method.getDescriptor(), ownerIsInterface);
        implMethod = impl.handle;
        
        // remove any prepended captured arguments for the 'natural' signature.
        samMethodType = adapt(interfaceMethodType, impl.getMethodType().dropParameterTypes(0, captures.length));
    }

    /**
     * Creates a new FunctionRef (low level). 
     * <p>
     * This will <b>not</b> set implMethodASM. It is for runtime use only.
     */
    public FunctionRef(Definition.Type expected, Definition.Method method, MethodHandle impl, Class<?>... captures) {
        // e.g. compareTo
        invokedName = method.name;
        // e.g. (Object)Comparator
        invokedType = MethodType.methodType(expected.clazz, captures);
        // e.g. (Object,Object)int
        interfaceMethodType = method.getMethodType().dropParameterTypes(0, 1);

        implMethod = impl;
        
        implMethodASM = null;
        
        // remove any prepended captured arguments for the 'natural' signature.
        samMethodType = adapt(interfaceMethodType, impl.type().dropParameterTypes(0, captures.length));
    }

    /** 
     * Looks up {@code type::call} from the whitelist, and returns a matching method.
     */
    private static Definition.Method lookup(Definition.Type expected, String type, String call, boolean receiverCaptured) {
        // check its really a functional interface
        // for e.g. Comparable
        Method method = expected.struct.getFunctionalMethod();
        if (method == null) {
            throw new IllegalArgumentException("Cannot convert function reference [" + type + "::" + call + "] " +
                                               "to [" + expected.name + "], not a functional interface");
        }

        // lookup requested method
        Definition.Struct struct = Definition.getType(type).struct;
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

    /** Returns true if you should ask LambdaMetaFactory to construct a bridge for the interface signature */
    public boolean needsBridges() {
        // currently if the interface differs, we ask for a bridge, but maybe we should do smarter checking?
        // either way, stuff will fail if its wrong :)
        return interfaceMethodType.equals(samMethodType) == false;
    }
    
    /** 
     * If the interface expects a primitive type to be returned, we can't return Object,
     * But we can set SAM to the wrapper version, and a cast will take place 
     */
    private static MethodType adapt(MethodType expected, MethodType actual) {
        if (expected.returnType().isPrimitive() && actual.returnType() == Object.class) {
            actual = actual.changeReturnType(MethodType.methodType(expected.returnType()).wrap().returnType());
        }
        return actual;
    }
}
