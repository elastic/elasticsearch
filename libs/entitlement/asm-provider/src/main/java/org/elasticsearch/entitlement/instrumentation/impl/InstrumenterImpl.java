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
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.RecordComponentVisitor;
import org.objectweb.asm.Type;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.stream.Stream;

import static org.objectweb.asm.ClassWriter.COMPUTE_FRAMES;
import static org.objectweb.asm.ClassWriter.COMPUTE_MAXS;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.GETSTATIC;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;

public class InstrumenterImpl implements Instrumenter {

    private static final String checkerClassDescriptor;
    private static final String handleClass;
    static {
        int javaVersion = Runtime.version().feature();
        final String classNamePrefix;
        if (javaVersion >= 23) {
            classNamePrefix = "Java23";
        } else {
            classNamePrefix = "";
        }
        String checkerClass = "org/elasticsearch/entitlement/bridge/" + classNamePrefix + "EntitlementChecker";
        handleClass = checkerClass + "Handle";
        checkerClassDescriptor = Type.getObjectType(checkerClass).getDescriptor();
    }

    /**
     * To avoid class name collisions during testing without an agent to replace classes in-place.
     */
    private final String classNameSuffix;
    private final Map<MethodKey, CheckerMethod> instrumentationMethods;

    public InstrumenterImpl(String classNameSuffix, Map<MethodKey, CheckerMethod> instrumentationMethods) {
        this.classNameSuffix = classNameSuffix;
        this.instrumentationMethods = instrumentationMethods;
    }

    public ClassFileInfo instrumentClassFile(Class<?> clazz) throws IOException {
        ClassFileInfo initial = getClassFileInfo(clazz);
        return new ClassFileInfo(initial.fileName(), instrumentClass(Type.getInternalName(clazz), initial.bytecodes()));
    }

    public static ClassFileInfo getClassFileInfo(Class<?> clazz) throws IOException {
        String internalName = Type.getInternalName(clazz);
        String fileName = "/" + internalName + ".class";
        byte[] originalBytecodes;
        try (InputStream classStream = clazz.getResourceAsStream(fileName)) {
            if (classStream == null) {
                throw new IllegalStateException("Classfile not found in jar: " + fileName);
            }
            originalBytecodes = classStream.readAllBytes();
        }
        return new ClassFileInfo(fileName, originalBytecodes);
    }

    @Override
    public byte[] instrumentClass(String className, byte[] classfileBuffer) {
        ClassReader reader = new ClassReader(classfileBuffer);
        ClassWriter writer = new ClassWriter(reader, COMPUTE_FRAMES | COMPUTE_MAXS);
        ClassVisitor visitor = new EntitlementClassVisitor(Opcodes.ASM9, writer, className);
        reader.accept(visitor, 0);
        return writer.toByteArray();
    }

    class EntitlementClassVisitor extends ClassVisitor {

        private static final String ENTITLEMENT_ANNOTATION = "EntitlementInstrumented";

        private final String className;

        private boolean isAnnotationPresent;
        private boolean annotationNeeded = true;

        EntitlementClassVisitor(int api, ClassVisitor classVisitor, String className) {
            super(api, classVisitor);
            this.className = className;
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            super.visit(version, access, name + classNameSuffix, signature, superName, interfaces);
        }

        @Override
        public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
            if (visible && descriptor.equals(ENTITLEMENT_ANNOTATION)) {
                isAnnotationPresent = true;
                annotationNeeded = false;
            }
            return cv.visitAnnotation(descriptor, visible);
        }

        @Override
        public void visitNestMember(String nestMember) {
            addClassAnnotationIfNeeded();
            super.visitNestMember(nestMember);
        }

        @Override
        public void visitPermittedSubclass(String permittedSubclass) {
            addClassAnnotationIfNeeded();
            super.visitPermittedSubclass(permittedSubclass);
        }

        @Override
        public void visitInnerClass(String name, String outerName, String innerName, int access) {
            addClassAnnotationIfNeeded();
            super.visitInnerClass(name, outerName, innerName, access);
        }

        @Override
        public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
            addClassAnnotationIfNeeded();
            return super.visitField(access, name, descriptor, signature, value);
        }

        @Override
        public RecordComponentVisitor visitRecordComponent(String name, String descriptor, String signature) {
            addClassAnnotationIfNeeded();
            return super.visitRecordComponent(name, descriptor, signature);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
            addClassAnnotationIfNeeded();
            var mv = super.visitMethod(access, name, descriptor, signature, exceptions);
            if (isAnnotationPresent == false) {
                boolean isStatic = (access & ACC_STATIC) != 0;
                boolean isCtor = "<init>".equals(name);
                var key = new MethodKey(className, name, Stream.of(Type.getArgumentTypes(descriptor)).map(Type::getInternalName).toList());
                var instrumentationMethod = instrumentationMethods.get(key);
                if (instrumentationMethod != null) {
                    // LOGGER.debug("Will instrument method {}", key);
                    return new EntitlementMethodVisitor(Opcodes.ASM9, mv, isStatic, isCtor, descriptor, instrumentationMethod);
                } else {
                    // LOGGER.trace("Will not instrument method {}", key);
                }
            }
            return mv;
        }

        /**
         * A class annotation can be added via visitAnnotation; we need to call visitAnnotation after all other visitAnnotation
         * calls (in case one of them detects our annotation is already present), but before any other subsequent visit* method is called
         * (up to visitMethod -- if no visitMethod is called, there is nothing to instrument).
         * This includes visitNestMember, visitPermittedSubclass, visitInnerClass, visitField, visitRecordComponent and, of course,
         * visitMethod (see {@link ClassVisitor} javadoc).
         */
        private void addClassAnnotationIfNeeded() {
            if (annotationNeeded) {
                // logger.debug("Adding {} annotation", ENTITLEMENT_ANNOTATION);
                AnnotationVisitor av = cv.visitAnnotation(ENTITLEMENT_ANNOTATION, true);
                if (av != null) {
                    av.visitEnd();
                }
                annotationNeeded = false;
            }
        }
    }

    class EntitlementMethodVisitor extends MethodVisitor {
        private final boolean instrumentedMethodIsStatic;
        private final boolean instrumentedMethodIsCtor;
        private final String instrumentedMethodDescriptor;
        private final CheckerMethod instrumentationMethod;
        private boolean hasCallerSensitiveAnnotation = false;

        EntitlementMethodVisitor(
            int api,
            MethodVisitor methodVisitor,
            boolean instrumentedMethodIsStatic,
            boolean instrumentedMethodIsCtor,
            String instrumentedMethodDescriptor,
            CheckerMethod instrumentationMethod
        ) {
            super(api, methodVisitor);
            this.instrumentedMethodIsStatic = instrumentedMethodIsStatic;
            this.instrumentedMethodIsCtor = instrumentedMethodIsCtor;
            this.instrumentedMethodDescriptor = instrumentedMethodDescriptor;
            this.instrumentationMethod = instrumentationMethod;
        }

        @Override
        public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
            if (visible && descriptor.endsWith("CallerSensitive;")) {
                hasCallerSensitiveAnnotation = true;
            }
            return super.visitAnnotation(descriptor, visible);
        }

        @Override
        public void visitCode() {
            pushEntitlementChecker();
            pushCallerClass();
            forwardIncomingArguments();
            invokeInstrumentationMethod();
            super.visitCode();
        }

        private void pushEntitlementChecker() {
            InstrumenterImpl.this.pushEntitlementChecker(mv);
        }

        private void pushCallerClass() {
            if (hasCallerSensitiveAnnotation) {
                mv.visitMethodInsn(
                    INVOKESTATIC,
                    "jdk/internal/reflect/Reflection",
                    "getCallerClass",
                    Type.getMethodDescriptor(Type.getType(Class.class)),
                    false
                );
            } else {
                mv.visitFieldInsn(
                    GETSTATIC,
                    Type.getInternalName(StackWalker.Option.class),
                    "RETAIN_CLASS_REFERENCE",
                    Type.getDescriptor(StackWalker.Option.class)
                );
                mv.visitMethodInsn(
                    INVOKESTATIC,
                    Type.getInternalName(StackWalker.class),
                    "getInstance",
                    Type.getMethodDescriptor(Type.getType(StackWalker.class), Type.getType(StackWalker.Option.class)),
                    false
                );
                mv.visitMethodInsn(
                    INVOKEVIRTUAL,
                    Type.getInternalName(StackWalker.class),
                    "getCallerClass",
                    Type.getMethodDescriptor(Type.getType(Class.class)),
                    false
                );
            }
        }

        private void forwardIncomingArguments() {
            int localVarIndex = 0;
            if (instrumentedMethodIsCtor) {
                localVarIndex++;
            } else if (instrumentedMethodIsStatic == false) {
                mv.visitVarInsn(Opcodes.ALOAD, localVarIndex++);
            }
            for (Type type : Type.getArgumentTypes(instrumentedMethodDescriptor)) {
                mv.visitVarInsn(type.getOpcode(Opcodes.ILOAD), localVarIndex);
                localVarIndex += type.getSize();
            }
        }

        private void invokeInstrumentationMethod() {
            mv.visitMethodInsn(
                INVOKEINTERFACE,
                instrumentationMethod.className(),
                instrumentationMethod.methodName(),
                Type.getMethodDescriptor(
                    Type.VOID_TYPE,
                    instrumentationMethod.parameterDescriptors().stream().map(Type::getType).toArray(Type[]::new)
                ),
                true
            );
        }
    }

    protected void pushEntitlementChecker(MethodVisitor mv) {
        mv.visitMethodInsn(INVOKESTATIC, handleClass, "instance", "()" + checkerClassDescriptor, false);
    }

    public record ClassFileInfo(String fileName, byte[] bytecodes) {}
}
