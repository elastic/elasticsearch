/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.objectweb.asm.Type;

import java.lang.invoke.MethodType;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.painless.WriterConstants.CLASS_NAME;
import static org.objectweb.asm.Opcodes.H_INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.H_INVOKESTATIC;
import static org.objectweb.asm.Opcodes.H_INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.H_NEWINVOKESPECIAL;

/**
 * Contains all the values necessary to write the instruction to initiate a
 * {@link LambdaBootstrap} for either a function reference or a user-defined
 * lambda function.
 */
public class FunctionRef {
    /**
     * Creates a new FunctionRef which will resolve {@code type::call} from the whitelist.
     * @param painlessLookup the whitelist against which this script is being compiled
     * @param functionTable user-defined and synthetic methods generated directly on the script class
     * @param location the character number within the script at compile-time
     * @param targetClass functional interface type to implement.
     * @param typeName the left hand side of a method reference expression
     * @param methodName the right hand side of a method reference expression
     * @param numberOfCaptures number of captured arguments
     * @param constants constants used for injection when necessary
     * @param needsScriptInstance uses an instance method and so receiver must be captured.
     */
    public static FunctionRef create(
        PainlessLookup painlessLookup,
        FunctionTable functionTable,
        Location location,
        Class<?> targetClass,
        String typeName,
        String methodName,
        int numberOfCaptures,
        Map<String, Object> constants,
        boolean needsScriptInstance
    ) {

        Objects.requireNonNull(painlessLookup);
        Objects.requireNonNull(targetClass);
        Objects.requireNonNull(typeName);
        Objects.requireNonNull(methodName);

        String targetClassName = PainlessLookupUtility.typeToCanonicalTypeName(targetClass);
        PainlessMethod interfaceMethod;

        try {
            interfaceMethod = painlessLookup.lookupFunctionalInterfacePainlessMethod(targetClass);

            if (interfaceMethod == null) {
                throw new IllegalArgumentException(
                    "cannot convert function reference ["
                        + typeName
                        + "::"
                        + methodName
                        + "] "
                        + "to a non-functional interface ["
                        + targetClassName
                        + "]"
                );
            }

            String interfaceMethodName = interfaceMethod.javaMethod().getName();
            MethodType interfaceMethodType = interfaceMethod.methodType().dropParameterTypes(0, 1);
            String delegateClassName;
            boolean isDelegateInterface;
            boolean isDelegateAugmented;
            int delegateInvokeType;
            String delegateMethodName;
            MethodType delegateMethodType;
            Object[] delegateInjections;

            Class<?> delegateMethodReturnType;
            List<Class<?>> delegateMethodParameters;
            int interfaceTypeParametersSize = interfaceMethod.typeParameters().size();

            if ("this".equals(typeName)) {
                Objects.requireNonNull(functionTable);

                if (numberOfCaptures < 0) {
                    throw new IllegalStateException("internal error");
                }

                String localFunctionKey = FunctionTable.buildLocalFunctionKey(methodName, numberOfCaptures + interfaceTypeParametersSize);
                LocalFunction localFunction = functionTable.getFunction(localFunctionKey);

                if (localFunction == null) {
                    throw new IllegalArgumentException(
                        "function reference [this::"
                            + localFunctionKey
                            + "] "
                            + "matching ["
                            + targetClassName
                            + ", "
                            + interfaceMethodName
                            + "/"
                            + interfaceTypeParametersSize
                            + "] "
                            + "not found"
                            + (localFunctionKey.contains("$") ? " due to an incorrect number of arguments" : "")
                    );
                }

                delegateClassName = CLASS_NAME;
                isDelegateInterface = false;
                isDelegateAugmented = false;
                delegateInvokeType = needsScriptInstance ? H_INVOKEVIRTUAL : H_INVOKESTATIC;
                delegateMethodName = localFunction.getMangledName();
                delegateMethodType = localFunction.getMethodType();
                delegateInjections = new Object[0];

                delegateMethodReturnType = localFunction.getReturnType();
                delegateMethodParameters = localFunction.getTypeParameters();
            } else if ("new".equals(methodName)) {
                if (numberOfCaptures != 0) {
                    throw new IllegalStateException("internal error");
                }

                PainlessConstructor painlessConstructor = painlessLookup.lookupPainlessConstructor(typeName, interfaceTypeParametersSize);

                if (painlessConstructor == null) {
                    throw new IllegalArgumentException(
                        "function reference ["
                            + typeName
                            + "::new/"
                            + interfaceTypeParametersSize
                            + "] "
                            + "matching ["
                            + targetClassName
                            + ", "
                            + interfaceMethodName
                            + "/"
                            + interfaceTypeParametersSize
                            + "] "
                            + "not found"
                    );
                }

                delegateClassName = painlessConstructor.javaConstructor().getDeclaringClass().getName();
                isDelegateInterface = false;
                isDelegateAugmented = false;
                delegateInvokeType = H_NEWINVOKESPECIAL;
                delegateMethodName = PainlessLookupUtility.CONSTRUCTOR_NAME;
                delegateMethodType = painlessConstructor.methodType();
                delegateInjections = new Object[0];

                delegateMethodReturnType = painlessConstructor.javaConstructor().getDeclaringClass();
                delegateMethodParameters = painlessConstructor.typeParameters();
            } else {
                if (numberOfCaptures != 0 && numberOfCaptures != 1) {
                    throw new IllegalStateException("internal error");
                }

                boolean captured = numberOfCaptures == 1;
                PainlessMethod painlessMethod = painlessLookup.lookupPainlessMethod(
                    typeName,
                    true,
                    methodName,
                    interfaceTypeParametersSize
                );

                if (painlessMethod == null) {
                    painlessMethod = painlessLookup.lookupPainlessMethod(
                        typeName,
                        false,
                        methodName,
                        captured ? interfaceTypeParametersSize : interfaceTypeParametersSize - 1
                    );

                    if (painlessMethod == null) {
                        throw new IllegalArgumentException(
                            "function reference "
                                + "["
                                + typeName
                                + "::"
                                + methodName
                                + "/"
                                + interfaceTypeParametersSize
                                + "] "
                                + "matching ["
                                + targetClassName
                                + ", "
                                + interfaceMethodName
                                + "/"
                                + interfaceTypeParametersSize
                                + "] "
                                + "not found"
                        );
                    }
                } else if (captured) {
                    throw new IllegalArgumentException(
                        "cannot use a static method as a function reference "
                            + "["
                            + typeName
                            + "::"
                            + methodName
                            + "/"
                            + interfaceTypeParametersSize
                            + "] "
                            + "with a non-static captured variable"
                    );
                }

                delegateClassName = painlessMethod.javaMethod().getDeclaringClass().getName();
                isDelegateInterface = painlessMethod.javaMethod().getDeclaringClass().isInterface();
                isDelegateAugmented = painlessMethod.javaMethod().getDeclaringClass() != painlessMethod.targetClass();

                if (Modifier.isStatic(painlessMethod.javaMethod().getModifiers())) {
                    delegateInvokeType = H_INVOKESTATIC;
                } else if (isDelegateInterface) {
                    delegateInvokeType = H_INVOKEINTERFACE;
                } else {
                    delegateInvokeType = H_INVOKEVIRTUAL;
                }

                delegateMethodName = painlessMethod.javaMethod().getName();
                delegateMethodType = painlessMethod.methodType();

                // interfaces that override a method from Object receive the method handle for
                // Object rather than for the interface; we change the first parameter to match
                // the interface type so the constant interface method reference is correctly
                // written to the constant pool
                if (delegateInvokeType != H_INVOKESTATIC
                    && painlessMethod.javaMethod().getDeclaringClass() != painlessMethod.methodType().parameterType(0)) {
                    if (painlessMethod.methodType().parameterType(0) != Object.class) {
                        throw new IllegalStateException("internal error");
                    }

                    delegateMethodType = delegateMethodType.changeParameterType(0, painlessMethod.javaMethod().getDeclaringClass());
                }

                delegateInjections = PainlessLookupUtility.buildInjections(painlessMethod, constants);

                delegateMethodReturnType = painlessMethod.returnType();

                if (delegateMethodType.parameterList().size() > painlessMethod.typeParameters().size()) {
                    delegateMethodParameters = new ArrayList<>(painlessMethod.typeParameters());
                    delegateMethodParameters.add(0, delegateMethodType.parameterType(0));
                } else {
                    delegateMethodParameters = painlessMethod.typeParameters();
                }
            }

            if (location != null) {
                for (int typeParameter = 0; typeParameter < interfaceTypeParametersSize; ++typeParameter) {
                    Class<?> from = interfaceMethod.typeParameters().get(typeParameter);
                    Class<?> to = delegateMethodParameters.get(numberOfCaptures + typeParameter);
                    AnalyzerCaster.getLegalCast(location, from, to, false, true);
                }

                if (interfaceMethod.returnType() != void.class) {
                    AnalyzerCaster.getLegalCast(location, delegateMethodReturnType, interfaceMethod.returnType(), false, true);
                }
            }

            MethodType factoryMethodType = MethodType.methodType(
                targetClass,
                delegateMethodType.dropParameterTypes(numberOfCaptures, delegateMethodType.parameterCount())
            );
            delegateMethodType = delegateMethodType.dropParameterTypes(0, numberOfCaptures);

            return new FunctionRef(
                interfaceMethodName,
                interfaceMethodType,
                delegateClassName,
                isDelegateInterface,
                isDelegateAugmented,
                delegateInvokeType,
                delegateMethodName,
                delegateMethodType,
                delegateInjections,
                factoryMethodType,
                needsScriptInstance ? WriterConstants.CLASS_TYPE : null
            );
        } catch (IllegalArgumentException iae) {
            if (location != null) {
                throw location.createError(iae);
            }

            throw iae;
        }
    }

    /** functional interface method name */
    public final String interfaceMethodName;
    /** functional interface method signature */
    public final MethodType interfaceMethodType;
    /** class of the delegate method to be called */
    public final String delegateClassName;
    /** whether a call is made on a delegate interface */
    public final boolean isDelegateInterface;
    /** if delegate method is augmented */
    public final boolean isDelegateAugmented;
    /** the invocation type of the delegate method */
    public final int delegateInvokeType;
    /** the name of the delegate method */
    public final String delegateMethodName;
    /** delegate method signature */
    public final MethodType delegateMethodType;
    /** injected constants */
    public final Object[] delegateInjections;
    /** factory (CallSite) method signature */
    private final MethodType factoryMethodType;
    /** factory (CallSite) method receiver, this modifies the method descriptor for the factory method */
    public final Type factoryMethodReceiver;

    private FunctionRef(
        String interfaceMethodName,
        MethodType interfaceMethodType,
        String delegateClassName,
        boolean isDelegateInterface,
        boolean isDelegateAugmented,
        int delegateInvokeType,
        String delegateMethodName,
        MethodType delegateMethodType,
        Object[] delegateInjections,
        MethodType factoryMethodType,
        Type factoryMethodReceiver
    ) {

        this.interfaceMethodName = interfaceMethodName;
        this.interfaceMethodType = interfaceMethodType;
        this.delegateClassName = delegateClassName;
        this.isDelegateInterface = isDelegateInterface;
        this.isDelegateAugmented = isDelegateAugmented;
        this.delegateInvokeType = delegateInvokeType;
        this.delegateMethodName = delegateMethodName;
        this.delegateMethodType = delegateMethodType;
        this.delegateInjections = delegateInjections;
        this.factoryMethodType = factoryMethodType;
        this.factoryMethodReceiver = factoryMethodReceiver;
    }

    /** Get the factory method type, with updated receiver if {@code factoryMethodReceiver} is set */
    public String getFactoryMethodDescriptor() {
        if (factoryMethodReceiver == null) {
            return factoryMethodType.toMethodDescriptorString();
        }
        List<Type> arguments = factoryMethodType.parameterList().stream().map(Type::getType).collect(Collectors.toList());
        arguments.add(0, factoryMethodReceiver);
        Type[] argArray = new Type[arguments.size()];
        arguments.toArray(argArray);
        return Type.getMethodDescriptor(Type.getType(factoryMethodType.returnType()), argArray);
    }

    /** Get the factory method type, updating the receiver if {@code factoryMethodReceiverClass} is non-null */
    public Class<?>[] factoryMethodParameters(Class<?> factoryMethodReceiverClass) {
        List<Class<?>> parameters = new ArrayList<>(factoryMethodType.parameterList());
        if (factoryMethodReceiverClass != null) {
            parameters.add(0, factoryMethodReceiverClass);
        }
        return parameters.toArray(new Class<?>[0]);
    }
}
