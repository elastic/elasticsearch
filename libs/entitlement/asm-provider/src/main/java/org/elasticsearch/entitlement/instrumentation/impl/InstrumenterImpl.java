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
import org.elasticsearch.entitlement.instrumentation.CheckMethod;
import org.elasticsearch.entitlement.instrumentation.EntitlementInstrumented;
import org.elasticsearch.entitlement.instrumentation.Instrumenter;
import org.elasticsearch.entitlement.instrumentation.MethodKey;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.stream.Stream;

import static org.objectweb.asm.ClassWriter.COMPUTE_FRAMES;
import static org.objectweb.asm.ClassWriter.COMPUTE_MAXS;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;

public final class InstrumenterImpl implements Instrumenter {
    private static final Logger logger = LogManager.getLogger(InstrumenterImpl.class);

    private final String getCheckerClassMethodDescriptor;
    private final String handleClass;

    /**
     * To avoid class name collisions during testing without an agent to replace classes in-place.
     */
    private final String classNameSuffix;
    private final Map<MethodKey, CheckMethod> checkMethods;

    InstrumenterImpl(
        String handleClass,
        String getCheckerClassMethodDescriptor,
        String classNameSuffix,
        Map<MethodKey, CheckMethod> checkMethods
    ) {
        this.handleClass = handleClass;
        this.getCheckerClassMethodDescriptor = getCheckerClassMethodDescriptor;
        this.classNameSuffix = classNameSuffix;
        this.checkMethods = checkMethods;
    }

    public static InstrumenterImpl create(Class<?> checkerClass, Map<MethodKey, CheckMethod> checkMethods) {
        Type checkerClassType = Type.getType(checkerClass);
        String handleClass = checkerClassType.getInternalName() + "Handle";
        String getCheckerClassMethodDescriptor = Type.getMethodDescriptor(checkerClassType);
        return new InstrumenterImpl(handleClass, getCheckerClassMethodDescriptor, "", checkMethods);
    }

    static ClassFileInfo getClassFileInfo(Class<?> clazz) throws IOException {
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
            super.visit(version, access, name + classNameSuffix, signature, superName, interfaces);
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
                var key = new MethodKey(className, name, Stream.of(Type.getArgumentTypes(descriptor)).map(Type::getInternalName).toList());
                var instrumentationMethod = checkMethods.get(key);
                if (instrumentationMethod != null) {
                    logger.debug("Will instrument {}", key);
                    return new EntitlementMethodVisitor(Opcodes.ASM9, mv, isStatic, isCtor, descriptor, instrumentationMethod);
                } else {
                    logger.trace("Will not instrument {}", key);
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
        private final CheckMethod checkMethod;
        private boolean hasCallerSensitiveAnnotation = false;

        EntitlementMethodVisitor(
            int api,
            MethodVisitor methodVisitor,
            boolean instrumentedMethodIsStatic,
            boolean instrumentedMethodIsCtor,
            String instrumentedMethodDescriptor,
            CheckMethod checkMethod
        ) {
            super(api, methodVisitor);
            this.instrumentedMethodIsStatic = instrumentedMethodIsStatic;
            this.instrumentedMethodIsCtor = instrumentedMethodIsCtor;
            this.instrumentedMethodDescriptor = instrumentedMethodDescriptor;
            if ("check$java_lang_ProcessBuilder$start".equals(checkMethod.methodName())) {
                this.checkMethod = CheckMethod.checkedException(
                    checkMethod.className(),
                    checkMethod.methodName(),
                    checkMethod.parameterDescriptors(),
                    IOException.class
                );
            } else if ("check$java_nio_file_Files$$exists".equals(checkMethod.methodName())) {
                this.checkMethod = CheckMethod.constantValue(
                    checkMethod.className(),
                    checkMethod.methodName(),
                    checkMethod.parameterDescriptors(),
                    false
                );
            } else if ("check$java_net_URLConnection$getHeaderFieldInt".equals(checkMethod.methodName())) {
                this.checkMethod = CheckMethod.parameterValue(
                    checkMethod.className(),
                    checkMethod.methodName(),
                    checkMethod.parameterDescriptors(),
                    3
                );
            } else {
                this.checkMethod = checkMethod;
            }
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
            if (checkMethod.checkMethodType() == CheckMethod.CheckMethodType.NOT_ENTITLED) {
                invokeInstrumentationMethod();
            } else {
                catchNotEntitled();
            }
            super.visitCode();
        }

        private void pushEntitlementChecker() {
            mv.visitMethodInsn(INVOKESTATIC, handleClass, "instance", getCheckerClassMethodDescriptor, false);
            mv.visitTypeInsn(CHECKCAST, checkMethod.className());
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

        private void catchNotEntitled() {
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
            if (checkMethod.checkMethodType() == CheckMethod.CheckMethodType.CHECKED_EXCEPTION) {
                wrapWithCheckedException();
            } else if (checkMethod.checkMethodType() == CheckMethod.CheckMethodType.CONSTANT_VALUE) {
                returnConstantValue();
            } else if (checkMethod.checkMethodType() == CheckMethod.CheckMethodType.PARAMETER_VALUE) {
                returnParameterValue();
            } else {
                throw new IllegalStateException("unexpected check method type [" + checkMethod.checkMethodType() + "]");
            }
            mv.visitLabel(catchEnd);
        }

        private void wrapWithCheckedException() {
            mv.visitTypeInsn(Opcodes.NEW, Type.getType(checkMethod.checked()).getInternalName());
            mv.visitInsn(Opcodes.DUP_X1);
            mv.visitInsn(Opcodes.SWAP);
            mv.visitInsn(Opcodes.DUP);
            mv.visitLdcInsn("not entitled: ");
            mv.visitInsn(Opcodes.SWAP);
            mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/Exception", "getMessage", "()Ljava/lang/String;", false);
            mv.visitInsn(Opcodes.DUP);
            Label endIf = new Label();
            mv.visitJumpInsn(Opcodes.IFNONNULL, endIf);
            mv.visitInsn(Opcodes.POP);
            mv.visitLdcInsn("[no information available]");
            mv.visitLabel(endIf);
            mv.visitMethodInsn(INVOKEVIRTUAL, "java/lang/String", "concat", "(Ljava/lang/String;)Ljava/lang/String;", false);
            mv.visitInsn(Opcodes.SWAP);
            mv.visitMethodInsn(
                INVOKESPECIAL,
                Type.getType(checkMethod.checked()).getInternalName(),
                "<init>",
                "(Ljava/lang/String;Ljava/lang/Throwable;)V",
                false
            );
            mv.visitInsn(Opcodes.ATHROW);
        }

        private void returnConstantValue() {
            Object constant = checkMethod.constant();
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
                        throw new IllegalStateException("unexpected check method constant [" + checkMethod.constant() + "]");
                    }
            }
        }

        private void returnParameterValue() {
            Type type = Type.getType(checkMethod.parameterDescriptors().get(checkMethod.parameter()));
            mv.visitVarInsn(
                type.getOpcode(Opcodes.ILOAD),
                checkMethod.parameter() - (instrumentedMethodIsCtor || instrumentedMethodIsStatic ? 2 : 1)
            );
            mv.visitInsn(type.getOpcode(Opcodes.IRETURN));
        }

        private void invokeInstrumentationMethod() {
            mv.visitMethodInsn(
                INVOKEINTERFACE,
                checkMethod.className(),
                checkMethod.methodName(),
                Type.getMethodDescriptor(
                    Type.VOID_TYPE,
                    checkMethod.parameterDescriptors().stream().map(Type::getType).toArray(Type[]::new)
                ),
                true
            );
        }
    }

    record ClassFileInfo(String fileName, byte[] bytecodes) {}
}
