/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches.hdfs;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import static org.objectweb.asm.Opcodes.ASM9;
import static org.objectweb.asm.Opcodes.BIPUSH;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.IF_ICMPLE;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.POP;

class SubjectGetSubjectPatcher extends ClassVisitor {
    SubjectGetSubjectPatcher(ClassWriter classWriter) {
        super(ASM9, classWriter);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        return new ReplaceCallMethodVisitor(super.visitMethod(access, name, descriptor, signature, exceptions), name, access, descriptor);
    }

    /**
     * Replaces calls to Subject.getSubject(context); with calls to Subject.current();
     */
    private static class ReplaceCallMethodVisitor extends MethodVisitor {
        private static final String SUBJECT_CLASS_INTERNAL_NAME = "javax/security/auth/Subject";
        private static final String METHOD_NAME = "getSubject";

        ReplaceCallMethodVisitor(MethodVisitor methodVisitor, String name, int access, String descriptor) {
            super(ASM9, methodVisitor);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
            if (opcode == INVOKESTATIC && SUBJECT_CLASS_INTERNAL_NAME.equals(owner) && METHOD_NAME.equals(name)) {
                Label olderJdk = new Label();
                Label end = new Label();
                mv.visitMethodInsn(
                    INVOKESTATIC,
                    Type.getInternalName(Runtime.class),
                    "version",
                    Type.getMethodDescriptor(Type.getType(Runtime.Version.class)),
                    false
                );
                mv.visitMethodInsn(
                    INVOKEVIRTUAL,
                    Type.getInternalName(Runtime.Version.class),
                    "feature",
                    Type.getMethodDescriptor(Type.getType(int.class)),
                    false
                );
                mv.visitIntInsn(BIPUSH, 17);
                mv.visitJumpInsn(IF_ICMPLE, olderJdk);
                // Get rid of the extra arg on the stack
                mv.visitInsn(POP);
                // Call Subject.current()
                mv.visitMethodInsn(
                    INVOKESTATIC,
                    SUBJECT_CLASS_INTERNAL_NAME,
                    "current",
                    Type.getMethodDescriptor(Type.getObjectType(SUBJECT_CLASS_INTERNAL_NAME)),
                    false
                );
                mv.visitJumpInsn(GOTO, end);
                mv.visitLabel(olderJdk);
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
                mv.visitLabel(end);
            } else {
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            }
        }
    }
}
