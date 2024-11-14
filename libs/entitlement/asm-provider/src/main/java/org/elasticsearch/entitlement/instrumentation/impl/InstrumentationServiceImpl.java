/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.entitlement.instrumentation.CheckerMethod;
import org.elasticsearch.entitlement.instrumentation.InstrumentationService;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class InstrumentationServiceImpl implements InstrumentationService {

    @Override
    public Instrumenter newInstrumenter(String classNameSuffix, Map<MethodKey, CheckerMethod> instrumentationMethods) {
        return new InstrumenterImpl(classNameSuffix, instrumentationMethods);
    }

    /**
     * @return a {@link MethodKey} suitable for looking up the given {@code targetMethod} in the entitlements trampoline
     */
    public MethodKey methodKeyForTarget(Method targetMethod) {
        Type actualType = Type.getMethodType(Type.getMethodDescriptor(targetMethod));
        return new MethodKey(
            Type.getInternalName(targetMethod.getDeclaringClass()),
            targetMethod.getName(),
            Stream.of(actualType.getArgumentTypes()).map(Type::getInternalName).toList(),
            Modifier.isStatic(targetMethod.getModifiers())
        );
    }

    @Override
    public Map<MethodKey, CheckerMethod> lookupMethodsToInstrument(String entitlementCheckerClassName) throws ClassNotFoundException,
        IOException {
        var methodsToInstrument = new HashMap<MethodKey, CheckerMethod>();
        var checkerClass = Class.forName(entitlementCheckerClassName);
        var instrumentationTargetAnnotationDescriptor = Type.getDescriptor(
            Class.forName("org.elasticsearch.entitlement.bridge.InstrumentationTarget")
        );
        var classFileInfo = InstrumenterImpl.getClassFileInfo(checkerClass);
        ClassReader reader = new ClassReader(classFileInfo.bytecodes());
        ClassVisitor visitor = new ClassVisitor(Opcodes.ASM9) {
            @Override
            public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
                var mv = super.visitMethod(access, name, descriptor, signature, exceptions);
                final String className = Type.getInternalName(checkerClass);
                final String methodName = name;
                final String methodDescriptor = descriptor;
                return new MethodVisitor(Opcodes.ASM9, mv) {
                    @Override
                    public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
                        var av = super.visitAnnotation(descriptor, visible);
                        if (descriptor.equals(instrumentationTargetAnnotationDescriptor)) {
                            return new InstrumentationTargetAnnotationVisitor(
                                av,
                                className,
                                methodName,
                                methodDescriptor,
                                methodsToInstrument
                            );
                        }
                        return av;
                    }
                };
            }
        };
        reader.accept(visitor, 0);
        return methodsToInstrument;
    }

    private static class InstrumentationTargetAnnotationVisitor extends AnnotationVisitor {
        private final String checkerClassName;
        private final String checkerMethodName;
        private final String checkerMethodDescriptor;
        private final HashMap<MethodKey, CheckerMethod> methodsToInstrument;
        private String targetClassName;
        private String targetMethodName;
        private boolean targetMethodIsStatic;

        InstrumentationTargetAnnotationVisitor(
            AnnotationVisitor av,
            String checkerClassName,
            String checkerMethodName,
            String checkerMethodDescriptor,
            HashMap<MethodKey, CheckerMethod> methodsToInstrument
        ) {
            super(Opcodes.ASM9, av);
            this.checkerClassName = checkerClassName;
            this.checkerMethodName = checkerMethodName;
            this.checkerMethodDescriptor = checkerMethodDescriptor;
            this.methodsToInstrument = methodsToInstrument;
        }

        @Override
        public void visit(String name, Object value) {
            super.visit(name, value);
            if (name.equals("className")) {
                this.targetClassName = (String) value;
            }
            if (name.equals("methodName")) {
                this.targetMethodName = (String) value;
            }
            if (name.equals("isStatic")) {
                this.targetMethodIsStatic = (boolean) value;
            }
        }

        @Override
        public void visitEnd() {
            super.visitEnd();
            assert targetClassName != null : "InstrumentationTarget for " + checkerMethodName + " is missing className";
            assert targetMethodName != null : "InstrumentationTarget for " + checkerMethodName + " is missing methodName";
            var targetParameterTypes = Arrays.stream(Type.getArgumentTypes(checkerMethodDescriptor))
                .skip(targetMethodIsStatic ? 1 : 2)
                .map(Type::getInternalName)
                .toList();
            var methodToInstrument = new MethodKey(targetClassName, targetMethodName, targetParameterTypes, targetMethodIsStatic);

            var checkerParameterDescriptors = Arrays.stream(Type.getArgumentTypes(checkerMethodDescriptor))
                .map(Type::getDescriptor)
                .toList();
            var checkerMethod = new CheckerMethod(checkerClassName, checkerMethodName, checkerParameterDescriptors);

            methodsToInstrument.put(methodToInstrument, checkerMethod);
        }
    }
}
