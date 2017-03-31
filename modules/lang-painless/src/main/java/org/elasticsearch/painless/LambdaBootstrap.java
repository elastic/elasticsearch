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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.painless.WriterConstants.CLASS_VERSION;
import static org.elasticsearch.painless.WriterConstants.LAMBDA_BOOTSTRAP_HANDLE2;
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

public class LambdaBootstrap {
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    public static CallSite bootstrap2(MethodHandles.Lookup lookup, String lambdaName,
                                      MethodType delegateMethodType,
                                      MethodHandle lambdaMethodHandle) {
        try {
            return new ConstantCallSite(lambdaMethodHandle.asType(delegateMethodType));
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    public static CallSite bootstrap(MethodHandles.Lookup lookup, String name, MethodType type,
                                     MethodType delegateMethodType, String className, int callType,
                                     String lambdaName, MethodType lambdaMethodType)
        throws LambdaConversionException {

        try {
            if (delegateMethodType.returnType() != void.class
                && lambdaMethodType.returnType() == void.class) {
                throw new LambdaConversionException("Type mismatch for lambda expected return:" +
                    " void is not convertible to " + delegateMethodType.returnType());
            }

            String baseClassName = "java/lang/Object";
            String lambdaClassName = lookup.lookupClass().getName().replace('.', '/') + "$$Lambda"
                + COUNTER.getAndIncrement();
            String lambdaInterfaceName = type.returnType().getName().replace('.', '/');
            ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
            cw.visit(CLASS_VERSION, ACC_PUBLIC | ACC_STATIC | ACC_SUPER | ACC_FINAL | ACC_SYNTHETIC,
                lambdaClassName, null, baseClassName, new String[]{lambdaInterfaceName});

            Type lambdaType = Type.getType("L" + lambdaClassName + ";");
            String conName = "<init>";
            String conDesc = type.changeReturnType(void.class).toMethodDescriptorString();
            Method conMeth = getAsmMethod(void.class, conName, type.parameterArray());
            Type baseConType = Type.getType(Object.class);
            Method baseConMeth = getAsmMethod(void.class, conName);
            GeneratorAdapter constructor = new GeneratorAdapter(ACC_PUBLIC, conMeth,
                cw.visitMethod(ACC_PUBLIC, conName, conDesc, null, null));
            constructor.visitCode();
            constructor.loadThis();
            constructor.invokeConstructor(baseConType, baseConMeth);

            for (int paramCount = 0; paramCount < type.parameterCount(); ++paramCount) {
                String argName = "arg$" + paramCount;
                Type argType = Type.getType(type.parameterType(paramCount));
                String argDesc = argType.getDescriptor();

                FieldVisitor fv =
                    cw.visitField(ACC_PRIVATE + ACC_FINAL, argName, argDesc, null, null);
                fv.visitEnd();

                constructor.loadThis();
                constructor.loadArg(paramCount);
                constructor.putField(lambdaType, argName, argType);
            }

            constructor.returnValue();
            constructor.endMethod();

            String facName = "get$lambda";
            String facDesc = type.toMethodDescriptorString();
            Method facMeth = new Method(facName, type.toMethodDescriptorString());

            GeneratorAdapter factory = new GeneratorAdapter(ACC_PUBLIC | ACC_STATIC, facMeth,
                cw.visitMethod(ACC_PUBLIC | ACC_STATIC, facName, facDesc, null, null));
            factory.visitCode();
            factory.newInstance(lambdaType);
            factory.dup();
            factory.loadArgs();
            factory.invokeConstructor(lambdaType, conMeth);
            factory.returnValue();
            factory.endMethod();

            Method delegateMethod = new Method(name, delegateMethodType.toMethodDescriptorString());
            MethodWriter delegate = new MethodWriter(ACC_PUBLIC, delegateMethod, cw, null, null);
            delegate.visitCode();

            if (callType == H_NEWINVOKESPECIAL) {
                Type newType = Type.getType(lambdaMethodType.returnType());
                delegate.newInstance(Type.getType(lambdaMethodType.returnType()));
                delegate.dup();
                delegate.invokeConstructor(newType, new Method("<init>", "()V"));
            } else {

                for (int paramCount = 0; paramCount < type.parameterCount(); ++paramCount) {
                    String argName = "arg$" + paramCount;
                    Type argType = Type.getType(type.parameterType(paramCount));
                    delegate.loadThis();
                    delegate.getField(lambdaType, argName, argType);
                }

                boolean iface = false;

                if (callType == H_INVOKESTATIC) {
                    lambdaMethodType =
                        lambdaMethodType.insertParameterTypes(0, type.parameterArray());
                    delegateMethodType =
                        delegateMethodType.insertParameterTypes(0, type.parameterArray());
                } else if (callType == H_INVOKEVIRTUAL || callType == H_INVOKEINTERFACE) {
                    if (type.parameterCount() == 0) {
                        Class<?> c = lambdaMethodType.parameterType(0);
                        className = c.getName();
                        lambdaMethodType = lambdaMethodType.dropParameterTypes(0, 1)
                            .insertParameterTypes(0, type.parameterArray());
                        delegateMethodType =
                            delegateMethodType.insertParameterTypes(0, type.parameterArray());
                    } else if (type.parameterCount() == 1) {
                        Class<?> c = type.parameterType(0);
                        className = c.getName();
                        delegateMethodType = delegateMethodType.insertParameterTypes(0, c);
                    } else {
                        throw new RuntimeException(
                            "unexpected number of captures: " + type.parameterCount());
                    }
                }

                delegate.loadArgs();
                Handle lambdaHandle =
                    new Handle(callType, className.replace('.', '/'),
                        lambdaName, lambdaMethodType.toMethodDescriptorString(),
                        callType == H_INVOKEINTERFACE);
                delegate.invokeDynamic(lambdaName, Type.getMethodType(delegateMethodType
                        .toMethodDescriptorString()).getDescriptor(), LAMBDA_BOOTSTRAP_HANDLE2,
                    lambdaHandle);
            }

            delegate.returnValue();
            delegate.endMethod();

            cw.visitEnd();

            final byte[] classBytes = cw.toByteArray();

            final Class<?> lambdaClass = AccessController.doPrivileged(
                new PrivilegedAction<Class<?>>() {
                    @Override
                    public Class<?> run() {
                        Compiler.Loader loader =
                            (Compiler.Loader)lookup.lookupClass().getClassLoader();
                        return loader.defineLambda(lambdaClassName.replace('/', '.'), classBytes);
                    }
                });

            if (type.parameterCount() == 0) {
                final Constructor<?> ctr = AccessController.doPrivileged(
                    new PrivilegedAction<Constructor<?>>() {
                        @Override
                        public Constructor<?> run() {
                            try {
                                return lambdaClass.getConstructor();
                            } catch (Exception exception) {
                                throw new RuntimeException(exception);
                            }
                        }
                    });

                Object inst = ctr.newInstance();
                return new ConstantCallSite(MethodHandles.constant(type.returnType(), inst));
            } else {
                return new ConstantCallSite(lookup.findStatic(lambdaClass, facName, type));
            }
        } catch (LambdaConversionException lce) {
            throw lce;
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    private static Method getAsmMethod(
        final Class<?> rtype, final String name, final Class<?>... ptypes) {
        return new Method(name, MethodType.methodType(rtype, ptypes).toMethodDescriptorString());
    }
}
