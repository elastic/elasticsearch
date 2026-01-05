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
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import static org.objectweb.asm.Opcodes.ASM9;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.POP;

class SubjectGetSubjectPatcher extends ClassVisitor {
    SubjectGetSubjectPatcher(ClassWriter classWriter) {
        super(ASM9, classWriter);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        return new ReplaceCallMethodVisitor(super.visitMethod(access, name, descriptor, signature, exceptions));
    }

    /**
     * Replaces calls to Subject.getSubject(context); with calls to Subject.current();
     */
    private static class ReplaceCallMethodVisitor extends MethodVisitor {
        private static final String SUBJECT_CLASS_INTERNAL_NAME = "javax/security/auth/Subject";
        private static final String METHOD_NAME = "getSubject";

        ReplaceCallMethodVisitor(MethodVisitor methodVisitor) {
            super(ASM9, methodVisitor);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
            if (opcode == INVOKESTATIC && SUBJECT_CLASS_INTERNAL_NAME.equals(owner) && METHOD_NAME.equals(name)) {
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
            } else {
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            }
        }
    }
}
