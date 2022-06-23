/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package com.elasticsearch.jdk.agent.instrument.math;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.function.Supplier;

import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ASM9;
import static org.objectweb.asm.Opcodes.DLOAD;

public class FastDoubleClassWriter implements Supplier<byte[]> {
    private final ClassReader reader;
    private final ClassWriter writer;
    private final PrintStream traceOutput;

    public FastDoubleClassWriter(PrintStream traceOutput) throws Exception {
        reader = new ClassReader("java.lang.Double");
        writer = new ClassWriter(0);
        this.traceOutput = traceOutput;
    }

    public FastDoubleClassWriter(byte[] originalClassBytes, PrintStream traceOutput) {
        reader = new ClassReader(originalClassBytes);
        writer = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        this.traceOutput = traceOutput;
    }

    @Override
    public byte[] get() {
        reader.accept(new ParseDoubleMethodPatcher(writer), 0);
        return writer.toByteArray();
    }

    // for standalone debugging
    public static void main(String[] args) throws Exception {
        FastDoubleClassWriter p = new FastDoubleClassWriter(System.out);
        p.get();
        try (FileOutputStream fos = new FileOutputStream("Double.class")) {
            fos.write(p.writer.toByteArray());
        }
    }

    class ParseDoubleMethodPatcher extends ClassVisitor {
        ParseDoubleMethodPatcher(ClassVisitor cv) {
            super(ASM9, cv);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
            if (name.equals("parseDouble") && descriptor.equals("(Ljava/lang/String;)D")) {
                MethodVisitor mv = cv.visitMethod(access, name, descriptor, signature, exceptions);
                return new MethodVisitor(ASM9, mv) {
                    @Override
                    public void visitCode() {
                        mv.visitCode();
                        traceOutput.println("Rewriting Double::parseDouble");

                        Label tryStart = new Label();
                        Label tryEnd = new Label();
                        Label handler = new Label();

                        mv.visitTryCatchBlock(tryStart, tryEnd, handler, "java/lang/NumberFormatException");
                        mv.visitLabel(tryStart);
                        mv.visitVarInsn(ALOAD, 0);
                        mv.visitMethodInsn(
                            Opcodes.INVOKESTATIC,
                            "com/elasticsearch/jdk/boot/math/FastDoubleParser",
                            "parseDouble",
                            "(Ljava/lang/CharSequence;)D",
                            false
                        );
                        mv.visitInsn(Opcodes.DRETURN);
                        mv.visitLabel(tryEnd);
                        mv.visitLabel(handler);
                        mv.visitMaxs(0, 0);
                        mv.visitEnd();
                    }
                };
            } else if (name.equals("toString") && descriptor.equals("(D)Ljava/lang/String;")) {
                MethodVisitor delegate = cv.visitMethod(access, name, descriptor, signature, exceptions);
                return new MethodVisitor(ASM9) {
                    @Override
                    public void visitCode() {
                        delegate.visitCode();
                        traceOutput.println("Rewriting Double::toString");

                        delegate.visitVarInsn(DLOAD, 0);
                        delegate.visitMethodInsn(
                            Opcodes.INVOKESTATIC,
                            "com/elasticsearch/jdk/boot/math/DoubleToDecimal",
                            "toString",
                            "(D)Ljava/lang/String;",
                            false
                        );
                        delegate.visitInsn(Opcodes.ARETURN);
                        delegate.visitMaxs(2, 2);
                        delegate.visitEnd();
                    }
                };
            } else {
                return super.visitMethod(access, name, descriptor, signature, exceptions);
            }
        }
    }
}
