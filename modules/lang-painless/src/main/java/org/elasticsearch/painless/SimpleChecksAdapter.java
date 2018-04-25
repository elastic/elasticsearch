/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
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
