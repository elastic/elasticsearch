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

import org.elasticsearch.painless.Locals.LocalMethod;
import org.elasticsearch.painless.lookup.PainlessClass;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.objectweb.asm.Type;

import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.List;

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
    public final PainlessMethod interfaceMethod;
    /** delegate method type parameters */
    public final List<Class<?>> delegateTypeParameters;
    /** delegate method return type */
    public final Class<?> delegateReturnType;

    /** factory method type descriptor */
    public final String factoryDescriptor;
    /** functional interface method as type */
    public final Type interfaceType;
    /** delegate method type method as type */
    public final Type delegateType;

    /** whether a call is made on a delegate interface */
    public final boolean isDelegateInterface;

    /**
     * Creates a new FunctionRef, which will resolve {@code type::call} from the whitelist.
     * @param painlessLookup the whitelist against which this script is being compiled
     * @param expected functional interface type to implement.
     * @param type the left hand side of a method reference expression
     * @param call the right hand side of a method reference expression
     * @param numCaptures number of captured arguments
     */
    public static FunctionRef resolveFromLookup(
            PainlessLookup painlessLookup, Class<?> expected, String type, String call, int numCaptures) {

        if ("new".equals(call)) {
            return new FunctionRef(expected, painlessLookup.lookupPainlessClass(expected).functionalMethod,
                    lookup(painlessLookup, expected, type), numCaptures);
        } else {
            return new FunctionRef(expected, painlessLookup.lookupPainlessClass(expected).functionalMethod,
                    lookup(painlessLookup, expected, type, call, numCaptures > 0), numCaptures);
        }
    }

    /**
     * Creates a new FunctionRef (already resolved)
     * @param expected functional interface type to implement
     * @param interfaceMethod functional interface method
     * @param delegateConstructor implementation constructor
     * @param numCaptures number of captured arguments
     */
    public FunctionRef(Class<?> expected, PainlessMethod interfaceMethod, PainlessConstructor delegateConstructor, int numCaptures) {
        Constructor<?> javaConstructor = delegateConstructor.javaConstructor;
        MethodType delegateMethodType = delegateConstructor.methodType;

        this.interfaceMethodName = interfaceMethod.javaMethod.getName();
        this.factoryMethodType = MethodType.methodType(expected,
                delegateMethodType.dropParameterTypes(numCaptures, delegateMethodType.parameterCount()));
        this.interfaceMethodType = interfaceMethod.methodType.dropParameterTypes(0, 1);

        this.delegateClassName = javaConstructor.getDeclaringClass().getName();
        this.isDelegateInterface = false;
        this.delegateInvokeType = H_NEWINVOKESPECIAL;
        this.delegateMethodName = PainlessLookupUtility.CONSTRUCTOR_NAME;
        this.delegateMethodType = delegateMethodType.dropParameterTypes(0, numCaptures);

        this.interfaceMethod = interfaceMethod;
        this.delegateTypeParameters = delegateConstructor.typeParameters;
        this.delegateReturnType = void.class;

        this.factoryDescriptor = factoryMethodType.toMethodDescriptorString();
        this.interfaceType = Type.getMethodType(interfaceMethodType.toMethodDescriptorString());
        this.delegateType = Type.getMethodType(this.delegateMethodType.toMethodDescriptorString());
    }

    /**
     * Creates a new FunctionRef (already resolved)
     * @param expected functional interface type to implement
     * @param interfaceMethod functional interface method
     * @param delegateMethod implementation method
     * @param numCaptures number of captured arguments
     */
    public FunctionRef(Class<?> expected, PainlessMethod interfaceMethod, PainlessMethod delegateMethod, int numCaptures) {
        MethodType delegateMethodType = delegateMethod.methodType;

        this.interfaceMethodName = interfaceMethod.javaMethod.getName();
        this.factoryMethodType = MethodType.methodType(expected,
                delegateMethodType.dropParameterTypes(numCaptures, delegateMethodType.parameterCount()));
        this.interfaceMethodType = interfaceMethod.methodType.dropParameterTypes(0, 1);

        this.delegateClassName = delegateMethod.javaMethod.getDeclaringClass().getName();
        this.isDelegateInterface = delegateMethod.javaMethod.getDeclaringClass().isInterface();

        if (Modifier.isStatic(delegateMethod.javaMethod.getModifiers())) {
            this.delegateInvokeType = H_INVOKESTATIC;
        } else if (delegateMethod.javaMethod.getDeclaringClass().isInterface()) {
            this.delegateInvokeType = H_INVOKEINTERFACE;
        } else {
            this.delegateInvokeType = H_INVOKEVIRTUAL;
        }

        this.delegateMethodName = delegateMethod.javaMethod.getName();
        this.delegateMethodType = delegateMethodType.dropParameterTypes(0, numCaptures);

        this.interfaceMethod = interfaceMethod;
        this.delegateTypeParameters = delegateMethod.typeParameters;
        this.delegateReturnType = delegateMethod.returnType;

        this.factoryDescriptor = factoryMethodType.toMethodDescriptorString();
        this.interfaceType = Type.getMethodType(interfaceMethodType.toMethodDescriptorString());
        this.delegateType = Type.getMethodType(this.delegateMethodType.toMethodDescriptorString());
    }

    /**
     * Creates a new FunctionRef (already resolved)
     * @param expected functional interface type to implement
     * @param interfaceMethod functional interface method
     * @param delegateMethod implementation method
     * @param numCaptures number of captured arguments
     */
    public FunctionRef(Class<?> expected, PainlessMethod interfaceMethod, LocalMethod delegateMethod, int numCaptures) {
        MethodType delegateMethodType = delegateMethod.methodType;

        this.interfaceMethodName = interfaceMethod.javaMethod.getName();
        this.factoryMethodType = MethodType.methodType(expected,
                delegateMethodType.dropParameterTypes(numCaptures, delegateMethodType.parameterCount()));
        this.interfaceMethodType = interfaceMethod.methodType.dropParameterTypes(0, 1);

        this.delegateClassName = CLASS_NAME;
        this.isDelegateInterface = false;
        this.delegateInvokeType = H_INVOKESTATIC;

        this.delegateMethodName = delegateMethod.name;
        this.delegateMethodType = delegateMethodType.dropParameterTypes(0, numCaptures);

        this.interfaceMethod = interfaceMethod;
        this.delegateTypeParameters = delegateMethod.typeParameters;
        this.delegateReturnType = delegateMethod.returnType;

        this.factoryDescriptor = factoryMethodType.toMethodDescriptorString();
        this.interfaceType = Type.getMethodType(interfaceMethodType.toMethodDescriptorString());
        this.delegateType = Type.getMethodType(this.delegateMethodType.toMethodDescriptorString());
    }

    /**
     * Creates a new FunctionRef (low level).
     * It is for runtime use only.
     */
    public FunctionRef(Class<?> expected,
                       PainlessMethod interfaceMethod, String delegateMethodName, MethodType delegateMethodType, int numCaptures) {
        this.interfaceMethodName = interfaceMethod.javaMethod.getName();
        this.factoryMethodType = MethodType.methodType(expected,
            delegateMethodType.dropParameterTypes(numCaptures, delegateMethodType.parameterCount()));
        this.interfaceMethodType = interfaceMethod.methodType.dropParameterTypes(0, 1);

        this.delegateClassName = CLASS_NAME;
        this.delegateInvokeType = H_INVOKESTATIC;
        this.delegateMethodName = delegateMethodName;
        this.delegateMethodType = delegateMethodType.dropParameterTypes(0, numCaptures);
        this.isDelegateInterface = false;

        this.interfaceMethod = null;
        this.delegateTypeParameters = null;
        this.delegateReturnType = null;

        this.factoryDescriptor = null;
        this.interfaceType = null;
        this.delegateType = null;
    }

    /**
     * Looks up {@code type} from the whitelist, and returns a matching constructor.
     */
    private static PainlessConstructor lookup(PainlessLookup painlessLookup, Class<?> expected, String type) {
        // check its really a functional interface
        // for e.g. Comparable
        PainlessMethod method = painlessLookup.lookupPainlessClass(expected).functionalMethod;
        if (method == null) {
            throw new IllegalArgumentException("Cannot convert function reference [" + type + "::new] " +
                    "to [" + PainlessLookupUtility.typeToCanonicalTypeName(expected) + "], not a functional interface");
        }

        // lookup requested constructor
        PainlessClass struct = painlessLookup.lookupPainlessClass(painlessLookup.canonicalTypeNameToType(type));
        PainlessConstructor impl = struct.constructors.get(PainlessLookupUtility.buildPainlessConstructorKey(method.typeParameters.size()));

        if (impl == null) {
            throw new IllegalArgumentException("Unknown reference [" + type + "::new] matching [" + expected + "]");
        }

        return impl;
    }

    /**
     * Looks up {@code type::call} from the whitelist, and returns a matching method.
     */
    private static PainlessMethod lookup(PainlessLookup painlessLookup, Class<?> expected,
                                         String type, String call, boolean receiverCaptured) {
        // check its really a functional interface
        // for e.g. Comparable
        PainlessMethod method = painlessLookup.lookupPainlessClass(expected).functionalMethod;
        if (method == null) {
            throw new IllegalArgumentException("Cannot convert function reference [" + type + "::" + call + "] " +
                    "to [" + PainlessLookupUtility.typeToCanonicalTypeName(expected) + "], not a functional interface");
        }

        // lookup requested method
        PainlessClass struct = painlessLookup.lookupPainlessClass(painlessLookup.canonicalTypeNameToType(type));
        final PainlessMethod impl;
        // look for a static impl first
        PainlessMethod staticImpl =
                struct.staticMethods.get(PainlessLookupUtility.buildPainlessMethodKey(call, method.typeParameters.size()));
        if (staticImpl == null) {
            // otherwise a virtual impl
            final int arity;
            if (receiverCaptured) {
                // receiver captured
                arity = method.typeParameters.size();
            } else {
                // receiver passed
                arity = method.typeParameters.size() - 1;
            }
            impl = struct.methods.get(PainlessLookupUtility.buildPainlessMethodKey(call, arity));
        } else {
            impl = staticImpl;
        }
        if (impl == null) {
            throw new IllegalArgumentException("Unknown reference [" + type + "::" + call + "] matching " +
                                               "[" + expected + "]");
        }
        return impl;
    }
}
