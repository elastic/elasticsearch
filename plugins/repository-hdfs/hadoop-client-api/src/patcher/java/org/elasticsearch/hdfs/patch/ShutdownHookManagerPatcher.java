/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.hdfs.patch;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.util.Set;

class ShutdownHookManagerPatcher extends ClassVisitor {
    private static final Set<String> voidMethods = Set.of("addShutdownHook", "clearShutdownHooks");
    private static final Set<String> booleanMethods = Set.of("removeShutdownHook", "hasShutdownHook", "isShutdownInProgress");

    ShutdownHookManagerPatcher(ClassWriter classWriter) {
        super(Opcodes.ASM9, classWriter);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
        if (voidMethods.contains(name)) {
            return new MethodReplacement(mv, () -> { mv.visitInsn(Opcodes.RETURN); });
        } else if (booleanMethods.contains(name)) {
            return new MethodReplacement(mv, () -> {
                mv.visitInsn(Opcodes.ICONST_0);
                mv.visitInsn(Opcodes.IRETURN);
            });
        } else if (name.equals("<clinit>")) {
            return new MethodReplacement(mv, () -> {
                mv.visitFieldInsn(Opcodes.GETSTATIC, "java/util/concurrent/TimeUnit", "SECONDS", "Ljava/util/concurrent/TimeUnit;");
                mv.visitFieldInsn(Opcodes.PUTSTATIC, "org/apache/hadoop/util/ShutdownHookManager", "TIME_UNIT_DEFAULT", "Ljava/util/concurrent/TimeUnit;");
                mv.visitInsn(Opcodes.ACONST_NULL);
                mv.visitFieldInsn(Opcodes.PUTSTATIC, "org/apache/hadoop/util/ShutdownHookManager", "EXECUTOR", "Ljava/util/concurrent/ExecutorService;");
                mv.visitInsn(Opcodes.RETURN);
            });
        }
        return mv;
    }
}
