/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.patches.hdfs;

import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class MethodReplacement extends MethodVisitor {
    private final MethodVisitor delegate;
    private final Runnable bodyWriter;

    MethodReplacement(MethodVisitor delegate, Runnable bodyWriter) {
        super(Opcodes.ASM9);
        this.delegate = delegate;
        this.bodyWriter = bodyWriter;
    }

    @Override
    public void visitCode() {
        // delegate.visitCode();
        bodyWriter.run();
        // delegate.visitEnd();
    }

    @Override
    public void visitMaxs(int maxStack, int maxLocals) {
        delegate.visitMaxs(maxStack, maxLocals);
    }
}
