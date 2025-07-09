/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches.awsv2sdk;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.util.Locale;

import static org.objectweb.asm.Opcodes.ASM9;
import static org.objectweb.asm.Opcodes.GETSTATIC;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;

class StringFormatInPathResolverPatcher extends ClassVisitor {

    StringFormatInPathResolverPatcher(ClassWriter classWriter) {
        super(ASM9, classWriter);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        return new ReplaceCallMethodVisitor(super.visitMethod(access, name, descriptor, signature, exceptions));
    }

    /**
     * Replaces calls to String.format(format, args); with calls to String.format(Locale.ROOT, format, args);
     */
    private static class ReplaceCallMethodVisitor extends MethodVisitor {
        private static final String CLASS_INTERNAL_NAME = Type.getInternalName(String.class);
        private static final String METHOD_NAME = "format";
        private static final String OLD_METHOD_DESCRIPTOR = Type.getMethodDescriptor(
            Type.getType(String.class),
            Type.getType(String.class),
            Type.getType(Object[].class)
        );
        private static final String NEW_METHOD_DESCRIPTOR = Type.getMethodDescriptor(
            Type.getType(String.class),
            Type.getType(Locale.class),
            Type.getType(String.class),
            Type.getType(Object[].class)
        );

        private boolean foundFormatPattern = false;

        ReplaceCallMethodVisitor(MethodVisitor methodVisitor) {
            super(ASM9, methodVisitor);
        }

        @Override
        public void visitLdcInsn(Object value) {
            if (value instanceof String s && s.startsWith("%s")) {
                if (foundFormatPattern) {
                    throw new IllegalStateException(
                        "A previous string format constant was not paired with a String.format() call. "
                            + "Patching would generate an unbalances stack"
                    );
                }
                // Push the extra arg on the stack
                mv.visitFieldInsn(GETSTATIC, Type.getInternalName(Locale.class), "ROOT", Type.getDescriptor(Locale.class));
                foundFormatPattern = true;
            }
            super.visitLdcInsn(value);
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
            if (opcode == INVOKESTATIC
                && foundFormatPattern
                && CLASS_INTERNAL_NAME.equals(owner)
                && METHOD_NAME.equals(name)
                && OLD_METHOD_DESCRIPTOR.equals(descriptor)) {
                // Replace the call with String.format(Locale.ROOT, format, args)
                mv.visitMethodInsn(INVOKESTATIC, CLASS_INTERNAL_NAME, METHOD_NAME, NEW_METHOD_DESCRIPTOR, false);
                foundFormatPattern = false;
            } else {
                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
            }
        }
    }
}
