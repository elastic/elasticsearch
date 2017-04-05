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

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.LambdaConversionException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.invoke.MethodHandles.Lookup;
import static org.elasticsearch.painless.Compiler.Loader;
import static org.elasticsearch.painless.WriterConstants.CLASS_VERSION;
import static org.elasticsearch.painless.WriterConstants.DELEGATE_BOOTSTRAP_HANDLE;
import static org.objectweb.asm.Opcodes.ACC_FINAL;
import static org.objectweb.asm.Opcodes.ACC_PRIVATE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ACC_SYNTHETIC;
import static org.objectweb.asm.Opcodes.H_INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.H_INVOKESTATIC;
import static org.objectweb.asm.Opcodes.H_INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.H_NEWINVOKESPECIAL;

public final class LambdaBootstrap {

    private static final class Capture {
        private final String name;
        private final Type type;
        private final String desc;

        private Capture(int count, Class type) {
            this.name = "arg$" + count;
            this.type = Type.getType(type);
            this.desc = this.type.getDescriptor();
        }
    }

    private static final AtomicLong COUNTER = new AtomicLong(0);

    public static CallSite lambdaBootstrap(
        Lookup lookup,
        String lambdaMethodName,
        MethodType factoryMethodType,
        MethodType lambdaMethodType,
        String delegateClassName,
        int delegateInvokeType,
        String delegateMethodName,
        MethodType delegateMethodType)
        throws LambdaConversionException {

        String lambdaClassName = lookup.lookupClass().getName().replace('.', '/') +
            "$$Lambda" + COUNTER.getAndIncrement();
        Type lambdaClassType = Type.getType("L" + lambdaClassName + ";");

        validateTypes(lambdaMethodType, delegateMethodType);

        ClassWriter cw =
            beginLambdaClass(lambdaClassName, factoryMethodType.returnType().getName());
        Capture[] captures = generateCaptureFields(cw, factoryMethodType);
        Method constructorMethod =
            generateLambdaConstructor(cw, lambdaClassType, factoryMethodType, captures);

        if (captures.length > 0) {
            generateFactoryMethod(cw, factoryMethodType, lambdaClassType, constructorMethod);
        }

        generateLambdaMethod(cw, factoryMethodType, lambdaClassName, lambdaClassType,
            lambdaMethodName, lambdaMethodType, delegateClassName, delegateInvokeType,
            delegateMethodName, delegateMethodType, captures);
        endLambdaClass(cw);

        Class<?> lambdaClass =
            createLambdaClass((Loader)lookup.lookupClass().getClassLoader(), cw, lambdaClassName);

        if (captures.length > 0) {
            return createCaptureCallSite(lookup, factoryMethodType, lambdaClass);
        } else {
            return createNoCaptureCallSite(factoryMethodType, lambdaClass);
        }
    }

    private static void validateTypes(MethodType lambdaMethodType, MethodType delegateMethodType)
        throws LambdaConversionException {

        if (lambdaMethodType.returnType() != void.class &&
            delegateMethodType.returnType() == void.class) {
            throw new LambdaConversionException("lambda expects return type ["
                + lambdaMethodType.returnType() + "], but found return type [void]");
        }
    }

    private static ClassWriter beginLambdaClass(String lambdaClassName, String lambdaInterface) {
        String baseClass = Object.class.getName().replace('.', '/');
        lambdaInterface = lambdaInterface.replace('.', '/');
        int modifiers = ACC_PUBLIC | ACC_STATIC | ACC_SUPER | ACC_FINAL | ACC_SYNTHETIC;

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        cw.visit(CLASS_VERSION,
            modifiers, lambdaClassName, null, baseClass, new String[] {lambdaInterface});

        return cw;
    }

    private static Capture[] generateCaptureFields(ClassWriter cw, MethodType factoryMethodType) {
        int captureTotal = factoryMethodType.parameterCount();
        Capture[] captures = new Capture[captureTotal];

        for (int captureCount = 0; captureCount < captureTotal; ++captureCount) {
            captures[captureCount] =
                new Capture(captureCount, factoryMethodType.parameterType(captureCount));
            int modifiers = ACC_PRIVATE + ACC_FINAL;

            FieldVisitor fv = cw.visitField(
                modifiers, captures[captureCount].name, captures[captureCount].desc, null, null);
            fv.visitEnd();
        }

        return captures;
    }

    private static Method generateLambdaConstructor(
        ClassWriter cw,
        Type lambdaClassType,
        MethodType factoryMethodType,
        Capture[] captures) {

        String conName = "<init>";
        String conDesc = factoryMethodType.changeReturnType(void.class).toMethodDescriptorString();
        Method conMeth = new Method(conName, conDesc);
        Type baseConType = Type.getType(Object.class);
        Method baseConMeth = new Method(conName,
            MethodType.methodType(void.class).toMethodDescriptorString());
        int modifiers = ACC_PUBLIC;

        GeneratorAdapter constructor = new GeneratorAdapter(modifiers, conMeth,
            cw.visitMethod(modifiers, conName, conDesc, null, null));
        constructor.visitCode();
        constructor.loadThis();
        constructor.invokeConstructor(baseConType, baseConMeth);

        for (int captureCount = 0; captureCount < captures.length; ++captureCount) {
            constructor.loadThis();
            constructor.loadArg(captureCount);
            constructor.putField(
                lambdaClassType, captures[captureCount].name, captures[captureCount].type);
        }

        constructor.returnValue();
        constructor.endMethod();

        return conMeth;
    }

    private static void generateFactoryMethod(
        ClassWriter cw,
        MethodType factoryMethodType,
        Type lambdaClassType,
        Method constructorMethod) {

        String facName = "get$lambda";
        String facDesc = factoryMethodType.toMethodDescriptorString();
        Method facMeth = new Method(facName, facDesc);
        int modifiers = ACC_PUBLIC | ACC_STATIC;

        GeneratorAdapter factory = new GeneratorAdapter(modifiers, facMeth,
            cw.visitMethod(modifiers, facName, facDesc, null, null));
        factory.visitCode();
        factory.newInstance(lambdaClassType);
        factory.dup();
        factory.loadArgs();
        factory.invokeConstructor(lambdaClassType, constructorMethod);
        factory.returnValue();
        factory.endMethod();
    }

    public static void generateLambdaMethod(
        ClassWriter cw,
        MethodType factoryMethodType,
        String lambdaClassName,
        Type lambdaClassType,
        String lambdaMethodName,
        MethodType lambdaMethodType,
        String delegateClassName,
        int delegateInvokeType,
        String delegateMethodName,
        MethodType delegateMethodType,
        Capture[] captures)
        throws LambdaConversionException {

        String lamDesc = lambdaMethodType.toMethodDescriptorString();
        Method lamMeth = new Method(lambdaClassName, lamDesc);
        int modifiers = ACC_PUBLIC;

        GeneratorAdapter lambda = new GeneratorAdapter(modifiers, lamMeth,
            cw.visitMethod(modifiers, lambdaMethodName, lamDesc, null, null));
        lambda.visitCode();

        if (delegateInvokeType == H_NEWINVOKESPECIAL) {
            String conName = "<init>";
            String conDesc = MethodType.methodType(void.class).toMethodDescriptorString();
            Method conMeth = new Method(conName, conDesc);
            Type conType = Type.getType(delegateMethodType.returnType());

            lambda.newInstance(conType);
            lambda.dup();
            lambda.invokeConstructor(conType, conMeth);
        } else {
            for (int captureCount = 0; captureCount < captures.length; ++captureCount) {
                lambda.loadThis();
                lambda.getField(
                    lambdaClassType, captures[captureCount].name, captures[captureCount].type);
            }

            lambda.loadArgs();

            if (delegateInvokeType == H_INVOKESTATIC) {
                lambdaMethodType =
                    lambdaMethodType.insertParameterTypes(0, factoryMethodType.parameterArray());
                delegateMethodType =
                    delegateMethodType.insertParameterTypes(0, factoryMethodType.parameterArray());
            } else if (delegateInvokeType == H_INVOKEVIRTUAL ||
                delegateInvokeType == H_INVOKEINTERFACE) {
                if (captures.length == 0) {
                    Class<?> clazz = delegateMethodType.parameterType(0);
                    delegateClassName = clazz.getName();
                    delegateMethodType = delegateMethodType.dropParameterTypes(0, 1);
                } else if (captures.length == 1) {
                    Class<?> clazz = factoryMethodType.parameterType(0);
                    delegateClassName = clazz.getName();
                    lambdaMethodType = lambdaMethodType.insertParameterTypes(0, clazz);
                } else {
                    throw new LambdaConversionException(
                        "unexpected number of captures [ " + captures.length + "]");
                }
            }

            Handle delegateHandle =
                new Handle(delegateInvokeType, delegateClassName.replace('.', '/'),
                    delegateMethodName, delegateMethodType.toMethodDescriptorString(),
                    delegateInvokeType == H_INVOKEINTERFACE);
            lambda.invokeDynamic(delegateMethodName, Type.getMethodType(lambdaMethodType
                    .toMethodDescriptorString()).getDescriptor(), DELEGATE_BOOTSTRAP_HANDLE,
                delegateHandle);
        }

        lambda.returnValue();
        lambda.endMethod();
    }

    private static void endLambdaClass(ClassWriter cw) {
        cw.visitEnd();
    }

    private static Class<?> createLambdaClass(
        Loader loader,
        ClassWriter cw,
        String lambdaClassName) {

        byte[] classBytes = cw.toByteArray();
        return AccessController.doPrivileged((PrivilegedAction<Class<?>>)() ->
            loader.defineLambda(lambdaClassName.replace('/', '.'), classBytes));
    }

    private static CallSite createNoCaptureCallSite(
        MethodType factoryMethodType,
        Class<?> lambdaClass) {

        Constructor<?> constructor = AccessController.doPrivileged(
            (PrivilegedAction<Constructor<?>>)() -> {
                try {
                    return lambdaClass.getConstructor();
                } catch (NoSuchMethodException nsme) {
                    throw new IllegalStateException("unable to create lambda class", nsme);
                }
            });

        try {
            return new ConstantCallSite(MethodHandles.constant(
                factoryMethodType.returnType(), constructor.newInstance()));
        } catch (InstantiationException |
            IllegalAccessException |
            InvocationTargetException exception) {
            throw new IllegalStateException("unable to create lambda class", exception);
        }
    }

    private static CallSite createCaptureCallSite(
        Lookup lookup,
        MethodType factoryMethodType,
        Class<?> lambdaClass) {

        try {
            return new ConstantCallSite(
                lookup.findStatic(lambdaClass, "get$lambda", factoryMethodType));
        } catch (NoSuchMethodException | IllegalAccessException exception) {
            throw new IllegalStateException("unable to create lambda factory class", exception);
        }
    }

    public static CallSite delegateBootstrap(Lookup lookup,
                                             String lambdaName,
                                             MethodType lambdaMethodType,
                                             MethodHandle delegateMethodHandle) {
        return new ConstantCallSite(delegateMethodHandle.asType(lambdaMethodType));
    }
}
