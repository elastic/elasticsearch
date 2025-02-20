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
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

class ShutdownHookManagerPatcher extends ClassVisitor {
    private static final String CLASSNAME = "org/apache/hadoop/util/ShutdownHookManager";
    private static final Set<String> VOID_METHODS = Set.of("addShutdownHook", "clearShutdownHooks");
    private static final Set<String> BOOLEAN_METHODS = Set.of("removeShutdownHook", "hasShutdownHook", "isShutdownInProgress");

    ShutdownHookManagerPatcher(ClassWriter classWriter) {
        super(Opcodes.ASM9, classWriter);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
        if (VOID_METHODS.contains(name)) {
            // make void methods noops
            return new MethodReplacement(mv, () -> { mv.visitInsn(Opcodes.RETURN); });
        } else if (BOOLEAN_METHODS.contains(name)) {
            // make boolean methods always return false
            return new MethodReplacement(mv, () -> {
                mv.visitInsn(Opcodes.ICONST_0);
                mv.visitInsn(Opcodes.IRETURN);
            });
        } else if (name.equals("<clinit>")) {
            return new MethodReplacement(mv, () -> {
                // just initialize the statics, don't actually get runtime to add shutdown hook

                var classType = Type.getObjectType(CLASSNAME);
                mv.visitTypeInsn(Opcodes.NEW, CLASSNAME);
                mv.visitInsn(Opcodes.DUP);
                mv.visitMethodInsn(Opcodes.INVOKESPECIAL, CLASSNAME, "<init>", "()V", false);
                mv.visitFieldInsn(Opcodes.PUTSTATIC, CLASSNAME, "MGR", classType.getDescriptor());

                var timeUnitType = Type.getType(TimeUnit.class);
                mv.visitFieldInsn(Opcodes.GETSTATIC, timeUnitType.getInternalName(), "SECONDS", timeUnitType.getDescriptor());
                mv.visitFieldInsn(Opcodes.PUTSTATIC, CLASSNAME, "TIME_UNIT_DEFAULT", timeUnitType.getDescriptor());

                var executorServiceType = Type.getType(ExecutorService.class);
                mv.visitInsn(Opcodes.ACONST_NULL);
                mv.visitFieldInsn(Opcodes.PUTSTATIC, CLASSNAME, "EXECUTOR", executorServiceType.getDescriptor());

                mv.visitInsn(Opcodes.RETURN);
            });
        }
        return mv;
    }
}
