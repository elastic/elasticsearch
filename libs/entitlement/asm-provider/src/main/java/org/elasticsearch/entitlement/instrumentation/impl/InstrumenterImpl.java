/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.instrumentation.impl;

import org.elasticsearch.core.Strings;
import org.elasticsearch.entitlement.bridge.NotEntitledException;
import org.elasticsearch.entitlement.instrumentation.EntitlementInstrumented;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
import org.elasticsearch.entitlement.rules.DeniedEntitlementStrategy;
import org.elasticsearch.entitlement.runtime.registry.InstrumentationInfo;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.RecordComponentVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.util.CheckClassAdapter;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.stream.Stream;

import static org.objectweb.asm.ClassWriter.COMPUTE_FRAMES;
import static org.objectweb.asm.ClassWriter.COMPUTE_MAXS;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;

public final class InstrumenterImpl implements Instrumenter {
    private static final Logger logger = LogManager.getLogger(InstrumenterImpl.class);

    private final String registryClassMethodDescriptor;
    private final String handleClass;
    private final Map<MethodKey, InstrumentationInfo> instrumentedMethods;

    InstrumenterImpl(String handleClass, String registryClassMethodDescriptor, Map<MethodKey, InstrumentationInfo> instrumentedMethods) {
        this.handleClass = handleClass;
        this.registryClassMethodDescriptor = registryClassMethodDescriptor;
        this.instrumentedMethods = instrumentedMethods;
    }

    public static InstrumenterImpl create(Class<?> registryClass, Map<MethodKey, InstrumentationInfo> checkMethods) {
        Type registryClassType = Type.getType(registryClass);
        String handleClass = registryClassType.getInternalName() + "Handle";
        String getCheckerClassMethodDescriptor = Type.getMethodDescriptor(registryClassType);
        return new InstrumenterImpl(handleClass, getCheckerClassMethodDescriptor, checkMethods);
    }

    private enum VerificationPhase {
        BEFORE_INSTRUMENTATION,
        AFTER_INSTRUMENTATION
    }

    private static String verify(byte[] classfileBuffer) {
        ClassReader reader = new ClassReader(classfileBuffer);
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        CheckClassAdapter.verify(reader, false, printWriter);
        return stringWriter.toString();
    }

    private static void verifyAndLog(byte[] classfileBuffer, String className, VerificationPhase phase) {
        try {
            String result = verify(classfileBuffer);
            if (result.isEmpty() == false) {
                logger.error(Strings.format("Bytecode verification (%s) for class [%s] failed: %s", phase, className, result));
            } else {
                logger.info("Bytecode verification ({}) for class [{}] passed", phase, className);
            }
        } catch (ClassCircularityError e) {
            // Apparently, verification during instrumentation is challenging for class resolution and loading
            // Treat this not as an error, but as "inconclusive"
            logger.warn(Strings.format("Cannot perform bytecode verification (%s) for class [%s]", phase, className), e);
        } catch (IllegalArgumentException e) {
            // The ASM CheckClassAdapter in some cases throws this instead of printing the error
            logger.error(Strings.format("Bytecode verification (%s) for class [%s] failed", phase, className), e);
        }
    }

    @Override
    public byte[] instrumentClass(String className, byte[] classfileBuffer, boolean verify) {
        if (verify) {
            verifyAndLog(classfileBuffer, className, VerificationPhase.BEFORE_INSTRUMENTATION);
        }

        ClassReader reader = new ClassReader(classfileBuffer);
        ClassWriter writer = new ClassWriter(reader, COMPUTE_FRAMES | COMPUTE_MAXS);
        ClassVisitor visitor = new EntitlementClassVisitor(Opcodes.ASM9, writer, className);
        reader.accept(visitor, 0);
        var outBytes = writer.toByteArray();

        if (verify) {
            verifyAndLog(outBytes, className, VerificationPhase.AFTER_INSTRUMENTATION);
        }

        return outBytes;
    }

    class EntitlementClassVisitor extends ClassVisitor {

        private static final String ENTITLEMENT_ANNOTATION_DESCRIPTOR = Type.getDescriptor(EntitlementInstrumented.class);

        private final String className;

        private boolean isAnnotationPresent;
        private boolean annotationNeeded = true;

        EntitlementClassVisitor(int api, ClassVisitor classVisitor, String className) {
            super(api, classVisitor);
            this.className = className;
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            super.visit(version, access, name, signature, superName, interfaces);
        }

        @Override
        public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
            if (visible && descriptor.equals(ENTITLEMENT_ANNOTATION_DESCRIPTOR)) {
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
                var key = new MethodKey(
                    className,
                    name,
                    Stream.of(Type.getArgumentTypes(descriptor)).map(EntitlementClassVisitor::getTypeName).toList()
                );
                var instrumentationMethod = instrumentedMethods.get(key);
                if (instrumentationMethod != null) {
                    logger.debug("Will instrument {}", key);
                    return new EntitlementMethodVisitor(
                        Opcodes.ASM9,
                        mv,
                        isStatic,
                        isCtor,
                        descriptor,
                        instrumentationMethod.instrumentationId(),
                        instrumentationMethod.handler()
                    );
                } else {
                    logger.trace("Will not instrument {}", key);
                }
            }
            return mv;
        }

        private static String getTypeName(Type type) {
            switch (type.getSort()) {
                case Type.BOOLEAN:
                    return Boolean.class.getName();
                case Type.CHAR:
                    return Character.class.getName();
                case Type.BYTE:
                    return Byte.class.getName();
                case Type.SHORT:
                    return Short.class.getName();
                case Type.INT:
                    return Integer.class.getName();
                case Type.FLOAT:
                    return Float.class.getName();
                case Type.DOUBLE:
                    return Double.class.getName();
                case Type.LONG:
                    return Long.class.getName();
                case Type.ARRAY:
                case Type.OBJECT:
                    return type.getClassName();
                default:
                    throw new IllegalStateException("Unexpected type sort: " + type.getSort());
            }
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
                AnnotationVisitor av = cv.visitAnnotation(ENTITLEMENT_ANNOTATION_DESCRIPTOR, true);
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
        private final String instrumentationId;
        private final DeniedEntitlementStrategy strategy;

        private boolean hasCallerSensitiveAnnotation = false;

        EntitlementMethodVisitor(
            int api,
            MethodVisitor methodVisitor,
            boolean instrumentedMethodIsStatic,
            boolean instrumentedMethodIsCtor,
            String instrumentedMethodDescriptor,
            String instrumentationId,
            DeniedEntitlementStrategy strategy
        ) {
            super(api, methodVisitor);
            this.instrumentedMethodIsStatic = instrumentedMethodIsStatic;
            this.instrumentedMethodIsCtor = instrumentedMethodIsCtor;
            this.instrumentedMethodDescriptor = instrumentedMethodDescriptor;
            this.instrumentationId = instrumentationId;
            this.strategy = strategy;
        }

        @Override
        public AnnotationVisitor visitAnnotation(String descriptor, boolean visible) {
            if (visible && descriptor.endsWith("CallerSensitive;")) {
                hasCallerSensitiveAnnotation = true;
            }
            return super.visitAnnotation(descriptor, visible);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public void visitCode() {
            pushEntitlementChecker();
            pushInstrumentationId();
            pushCallerClass();
            pushArguments();
            switch (strategy) {
                case DeniedEntitlementStrategy.ReturnEarlyDeniedEntitlementStrategy returnEarly -> {
                    // For return early strategy we want to catch not entitled and return early
                    catchNotEntitledAndReturnEarly();
                }
                case DeniedEntitlementStrategy.DefaultValueDeniedEntitlementStrategy defaultValue -> {
                    // For default value strategy we want to catch not entitled and return the default value
                    catchNotEntitledAndReturnValue(defaultValue.getDefaultValue());
                }
                case DeniedEntitlementStrategy.MethodArgumentValueDeniedEntitlementStrategy methodArgValue -> {
                    // For method argument value strategy we want to catch not entitled and return the method argument at the given index
                    catchNotEntitledAndReturnMethodArgument(methodArgValue.getIndex());
                }
                case DeniedEntitlementStrategy.NotEntitledDeniedEntitlementStrategy notEntitled -> {
                    // For not entitled strategy we just want to let the not entitled exception propagate
                    invokeInstrumentationMethod();
                }
                case DeniedEntitlementStrategy.ExceptionDeniedEntitlementStrategy exception -> {
                    // Custom exception strategy is handled by invoking the instrumentation method
                    invokeInstrumentationMethod();
                }
            }
            super.visitCode();
        }

        private void pushEntitlementChecker() {
            mv.visitMethodInsn(INVOKESTATIC, handleClass, "instance", registryClassMethodDescriptor, false);
        }

        private void pushInstrumentationId() {
            mv.visitLdcInsn(instrumentationId);
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
                mv.visitMethodInsn(
                    INVOKESTATIC,
                    "org/elasticsearch/entitlement/bridge/Util",
                    "getCallerClass",
                    Type.getMethodDescriptor(Type.getType(Class.class)),
                    false
                );
            }
        }

        private void pushArguments() {
            Type[] argumentTypes = Type.getArgumentTypes(instrumentedMethodDescriptor);
            boolean passThis = instrumentedMethodIsStatic == false && instrumentedMethodIsCtor == false;
            int numArgs = argumentTypes.length + (passThis ? 1 : 0);

            pushInt(numArgs);
            mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");

            int arrayIndex = 0;
            if (passThis) {
                mv.visitInsn(Opcodes.DUP);
                pushInt(arrayIndex++);
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitInsn(Opcodes.AASTORE);
            }

            int localVarIndex = instrumentedMethodIsStatic ? 0 : 1;

            for (int i = 0; i < argumentTypes.length; i++) {
                mv.visitInsn(Opcodes.DUP);
                pushInt(arrayIndex++);
                Type type = argumentTypes[i];
                mv.visitVarInsn(type.getOpcode(Opcodes.ILOAD), localVarIndex);
                box(type);
                mv.visitInsn(Opcodes.AASTORE);
                localVarIndex += type.getSize();
            }
        }

        private void pushInt(int value) {
            if (value >= -1 && value <= 5) {
                mv.visitInsn(Opcodes.ICONST_0 + value);
            } else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
                mv.visitIntInsn(Opcodes.BIPUSH, value);
            } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
                mv.visitIntInsn(Opcodes.SIPUSH, value);
            } else {
                mv.visitLdcInsn(value);
            }
        }

        private void box(Type type) {
            if (type.getSort() == Type.OBJECT || type.getSort() == Type.ARRAY) {
                return;
            }
            String descriptor = type.getDescriptor();
            String owner;
            switch (type.getSort()) {
                case Type.BOOLEAN:
                    owner = "java/lang/Boolean";
                    break;
                case Type.CHAR:
                    owner = "java/lang/Character";
                    break;
                case Type.BYTE:
                    owner = "java/lang/Byte";
                    break;
                case Type.SHORT:
                    owner = "java/lang/Short";
                    break;
                case Type.INT:
                    owner = "java/lang/Integer";
                    break;
                case Type.FLOAT:
                    owner = "java/lang/Float";
                    break;
                case Type.LONG:
                    owner = "java/lang/Long";
                    break;
                case Type.DOUBLE:
                    owner = "java/lang/Double";
                    break;
                default:
                    throw new IllegalArgumentException("Unexpected type sort: " + type.getSort());
            }
            mv.visitMethodInsn(INVOKESTATIC, owner, "valueOf", "(" + descriptor + ")L" + owner + ";", false);
        }

        private void invokeInstrumentationMethod() {
            mv.visitMethodInsn(
                INVOKEINTERFACE,
                Type.getReturnType(registryClassMethodDescriptor).getInternalName(),
                "check$",
                "(Ljava/lang/String;Ljava/lang/Class;[Ljava/lang/Object;)V",
                true
            );
        }

        private void catchNotEntitledAndReturnValue(Object defaultValue) {
            wrapInstrumentationInTryCatch(() -> { returnConstantValue(defaultValue); });
        }

        private void catchNotEntitledAndReturnEarly() {
            wrapInstrumentationInTryCatch(() -> {
                // Pop the exception from the stack
                mv.visitInsn(Opcodes.POP);
                // Return immediately, making the method a no-op
                mv.visitInsn(Opcodes.RETURN);
            });
        }

        private void catchNotEntitledAndReturnMethodArgument(int argumentIndex) {
            wrapInstrumentationInTryCatch(() -> {
                // Pop the exception from the stack
                mv.visitInsn(Opcodes.POP);

                // Calculate the local variable index for the method argument
                Type[] argumentTypes = Type.getArgumentTypes(instrumentedMethodDescriptor);
                int localVarIndex = instrumentedMethodIsStatic ? 0 : 1;

                // Advance to the specified argument index
                for (int i = 0; i < argumentIndex && i < argumentTypes.length; i++) {
                    localVarIndex += argumentTypes[i].getSize();
                }

                // Load the method argument at the specified index
                if (argumentIndex < argumentTypes.length) {
                    Type argType = argumentTypes[argumentIndex];
                    mv.visitVarInsn(argType.getOpcode(Opcodes.ILOAD), localVarIndex);
                    mv.visitInsn(argType.getOpcode(Opcodes.IRETURN));
                } else {
                    throw new IllegalStateException("Invalid argument index: " + argumentIndex);
                }
            });
        }

        private void wrapInstrumentationInTryCatch(Runnable catchHandler) {
            Label tryStart = new Label();
            Label tryEnd = new Label();
            Label catchStart = new Label();
            Label catchEnd = new Label();
            mv.visitTryCatchBlock(tryStart, tryEnd, catchStart, Type.getType(NotEntitledException.class).getInternalName());
            mv.visitLabel(tryStart);
            invokeInstrumentationMethod();
            mv.visitLabel(tryEnd);
            mv.visitJumpInsn(Opcodes.GOTO, catchEnd);
            mv.visitLabel(catchStart);
            catchHandler.run();
            mv.visitLabel(catchEnd);
        }

        private void returnConstantValue(Object constant) {
            if (constant == null) {
                mv.visitInsn(Opcodes.ACONST_NULL);
                mv.visitInsn(Opcodes.ARETURN);
            } else {
                mv.visitLdcInsn(constant);
                if (constant instanceof String) {
                    mv.visitInsn(Opcodes.ARETURN);
                } else if (constant instanceof Double) {
                    mv.visitInsn(Opcodes.DRETURN);
                } else if (constant instanceof Float) {
                    mv.visitInsn(Opcodes.FRETURN);
                } else if (constant instanceof Long) {
                    mv.visitInsn(Opcodes.LRETURN);
                } else if (constant instanceof Integer
                    || constant instanceof Character
                    || constant instanceof Short
                    || constant instanceof Byte
                    || constant instanceof Boolean) {
                        mv.visitInsn(Opcodes.IRETURN);
                    } else {
                        throw new IllegalStateException("unexpected check method constant [" + constant + "]");
                    }
            }
        }
    }
}
