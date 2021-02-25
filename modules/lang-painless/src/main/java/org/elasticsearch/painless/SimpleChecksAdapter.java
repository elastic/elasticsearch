/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.util.CheckClassAdapter;
import org.objectweb.asm.util.CheckMethodAdapter;

import java.util.HashMap;

/**
 * A CheckClassAdapter that does not use setAccessible to try to access private fields of Label!
 * <p>
 * This means jump insns are not checked, but we still get all the other checking.
 */
// TODO: we should really try to get this fixed in ASM!
public class SimpleChecksAdapter extends CheckClassAdapter {

    public SimpleChecksAdapter(ClassVisitor cv) {
        super(WriterConstants.ASM_VERSION, cv, false);
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
        MethodVisitor in = cv.visitMethod(access, name, desc, signature, exceptions);
        CheckMethodAdapter checker = new CheckMethodAdapter(WriterConstants.ASM_VERSION, in, new HashMap<Label, Integer>()) {
            @Override
            public void visitJumpInsn(int opcode, Label label) {
                mv.visitJumpInsn(opcode, label);
            }

            @Override
            public void visitTryCatchBlock(Label start, Label end, Label handler, String type) {
                mv.visitTryCatchBlock(start, end, handler, type);
            }
        };
        checker.version = WriterConstants.CLASS_VERSION;
        return checker;
    }
}
