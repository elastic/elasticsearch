/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches.azurecore;

import org.elasticsearch.gradle.internal.dependencies.patches.hdfs.MethodReplacement;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

class ImplUtilsPatcher extends ClassVisitor {
    ImplUtilsPatcher(ClassVisitor classVisitor) {
        super(Opcodes.ASM9, classVisitor);
    }

    public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
        MethodVisitor mv = super.visitMethod(access, name, descriptor, signature, exceptions);
        // `addShutdownHook` invokes `java.lang.Runtime.addShutdownHook`, which is forbidden (i.e. it will throw an Entitlements error).
        // We replace the method body here with `return null`.
        if (name.equals("addShutdownHookSafely")) {
            return new MethodReplacement(mv, () -> {
                mv.visitInsn(Opcodes.ACONST_NULL);
                mv.visitInsn(Opcodes.ARETURN);
            });
        }
        return mv;
    }
}
