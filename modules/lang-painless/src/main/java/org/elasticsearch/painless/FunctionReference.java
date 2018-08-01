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

import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;

import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.Objects;

import static org.elasticsearch.painless.WriterConstants.CLASS_NAME;
import static org.objectweb.asm.Opcodes.H_INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.H_INVOKESTATIC;
import static org.objectweb.asm.Opcodes.H_INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.H_NEWINVOKESPECIAL;

public class FunctionReference {

    /**
     * Creates a new FunctionReference which will resolve {@code type::call} from the whitelist.
     * @param painlessLookup the whitelist against which this script is being compiled
     * @param locals the local state for a script instance
     * @param targetClass functional interface type to implement.
     * @param typeName the left hand side of a method reference expression
     * @param methodName the right hand side of a method reference expression
     * @param numCaptures number of captured arguments
     */
    public static FunctionReference resolve(PainlessLookup painlessLookup, Locals locals,
            Class<?> targetClass, String typeName, String methodName, int numCaptures) {

        Objects.requireNonNull(painlessLookup);
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(typeName);
        Objects.requireNonNull(methodName);

        String targetClassName = PainlessLookupUtility.typeToCanonicalTypeName(targetClass);
        PainlessMethod interfaceMethod;

        try {
            interfaceMethod = painlessLookup.lookupFunctionalInterfacePainlessMethod(targetClass);
        } catch (IllegalArgumentException iae) {
            throw new IllegalArgumentException("cannot convert function reference [" + typeName + "::" + methodName + "] " +
                    "to a non-functional interface [" + targetClassName + "]", iae);
        }

        int typeParametersSize = interfaceMethod.typeParameters.size();

        String interfaceMethodName = interfaceMethod.javaMethod.getName();
        MethodType interfaceMethodType = interfaceMethod.methodType.dropParameterTypes(0, 1);
        String delegateClassName;
        boolean isDelegateInterface;
        int delegateInvokeType;
        String delegateMethodName;
        MethodType delegateMethodType;

        if ("this".equals(typeName)) {
            Objects.requireNonNull(locals);

            Locals.LocalMethod localMethod = locals.getMethod(Locals.buildLocalMethodKey(methodName, typeParametersSize));

            if (localMethod == null) {
                throw new IllegalArgumentException("function reference [this::" + methodName + "] not found");
            }

            delegateClassName = CLASS_NAME;
            isDelegateInterface = false;
            delegateInvokeType = H_INVOKESTATIC;
            delegateMethodName = localMethod.name;
            delegateMethodType = localMethod.methodType.dropParameterTypes(0, numCaptures);
        } else if ("new".equals(methodName)) {
            PainlessConstructor painlessConstructor;

            try {
                painlessConstructor = painlessLookup.lookupPainlessConstructor(typeName, typeParametersSize);
            } catch (IllegalArgumentException iae) {
                throw new IllegalArgumentException(
                        "function reference [" + typeName + "::new] matching [" + targetClassName + "] not found", iae);
            }

            delegateClassName = painlessConstructor.javaConstructor.getDeclaringClass().getName();
            isDelegateInterface = false;
            delegateInvokeType = H_NEWINVOKESPECIAL;
            delegateMethodName = PainlessLookupUtility.CONSTRUCTOR_NAME;
            delegateMethodType = painlessConstructor.methodType.dropParameterTypes(0, numCaptures);
        } else {
            PainlessMethod painlessMethod;

            try {
                painlessMethod = painlessLookup.lookupPainlessMethod(targetClassName, true, methodName, typeParametersSize);
            } catch (IllegalArgumentException staticIAE) {
                try {
                    painlessMethod = painlessLookup.lookupPainlessMethod(
                            targetClassName, false, methodName, numCaptures > 0 ? typeParametersSize : typeParametersSize - 1);
                } catch (IllegalArgumentException iae) {
                    throw new IllegalArgumentException(
                            "function reference [" + typeName + "::" + methodName + "] matching [" + targetClassName + "] not found", iae);
                }
            }

            delegateClassName = painlessMethod.javaMethod.getDeclaringClass().getName();
            isDelegateInterface = painlessMethod.javaMethod.getDeclaringClass().isInterface();

            if (Modifier.isStatic(painlessMethod.javaMethod.getModifiers())) {
                delegateInvokeType = H_INVOKESTATIC;
            } else if (isDelegateInterface) {
                delegateInvokeType = H_INVOKEINTERFACE;
            } else {
                delegateInvokeType = H_INVOKEVIRTUAL;
            }

            delegateMethodName = painlessMethod.javaMethod.getName();
            delegateMethodType = painlessMethod.methodType.dropParameterTypes(0, numCaptures);
        }

        MethodType factoryMethodType = MethodType.methodType(targetClass,
                delegateMethodType.dropParameterTypes(numCaptures, delegateMethodType.parameterCount()));

        return new FunctionReference(interfaceMethodName, interfaceMethodType,
                delegateClassName, isDelegateInterface, delegateInvokeType, delegateMethodName, delegateMethodType,
                factoryMethodType
        );
    }

    /** functional interface method name */
    public final String interfaceMethodName;
    /** functional interface method signature */
    public final MethodType interfaceMethodType;
    /** class of the delegate method to be called */
    public final String delegateClassName;
    /** whether a call is made on a delegate interface */
    public final boolean isDelegateInterface;
    /** the invocation type of the delegate method */
    public final int delegateInvokeType;
    /** the name of the delegate method */
    public final String delegateMethodName;
    /** delegate method signature */
    public final MethodType delegateMethodType;
    /** factory (CallSite) method signature */
    public final MethodType factoryMethodType;

    public FunctionReference(
            String interfaceMethodName, MethodType interfaceMethodType,
            String delegateClassName, boolean isDelegateInterface,
            int delegateInvokeType, String delegateMethodName, MethodType delegateMethodType,
            MethodType factoryMethodType) {

        this.interfaceMethodName = interfaceMethodName;
        this.interfaceMethodType = interfaceMethodType;
        this.delegateClassName = delegateClassName;
        this.isDelegateInterface = isDelegateInterface;
        this.delegateInvokeType = delegateInvokeType;
        this.delegateMethodName = delegateMethodName;
        this.delegateMethodType = delegateMethodType;
        this.factoryMethodType = factoryMethodType;
    }
}
