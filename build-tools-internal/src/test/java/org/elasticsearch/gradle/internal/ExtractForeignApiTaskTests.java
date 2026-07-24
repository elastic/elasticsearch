/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal;

import org.junit.Test;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.tree.AnnotationNode;
import org.objectweb.asm.tree.ClassNode;
import org.objectweb.asm.tree.MethodNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ExtractForeignApiTaskTests {

    private static final String PREVIEW_ANN = "Ljdk/internal/javac/PreviewFeature;";
    private static final String CALLER_SENSITIVE_ANN = "Ljdk/internal/reflect/CallerSensitive;";
    private static final int V21_PREVIEW = Opcodes.V21 | Opcodes.V_PREVIEW;

    @Test
    public void testNonPublicClassReturnsNull() throws IOException {
        byte[] classBytes = buildClass(0, V21_PREVIEW);
        assertNull(ExtractForeignApiTask.createStub(new ByteArrayInputStream(classBytes)));
    }

    @Test
    public void testPreviewFlagStripped() throws IOException {
        byte[] classBytes = buildClass(Opcodes.ACC_PUBLIC | Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT, V21_PREVIEW);
        ClassNode result = readStub(classBytes);
        assertEquals(Opcodes.V21, result.version);
    }

    @Test
    public void testPreviewAnnotationStrippedFromClass() throws IOException {
        byte[] classBytes = buildClassWithPreviewAnnotation();
        ClassNode result = readStub(classBytes);
        assertNoPreviewAnnotation(result.visibleAnnotations);
    }

    @Test
    public void testPreviewAnnotationStrippedFromMethod() throws IOException {
        byte[] classBytes = buildClassWithAnnotatedMethod(Opcodes.ACC_PUBLIC, PREVIEW_ANN);
        ClassNode result = readStub(classBytes);
        assertEquals(1, result.methods.size());
        assertNoPreviewAnnotation(result.methods.getFirst().visibleAnnotations);
    }

    @Test
    public void testPreviewAnnotationStrippedFromField() throws IOException {
        byte[] classBytes = buildClassWithAnnotatedField(Opcodes.ACC_PUBLIC, PREVIEW_ANN);
        ClassNode result = readStub(classBytes);
        assertEquals(1, result.fields.size());
        assertNoPreviewAnnotation(result.fields.getFirst().visibleAnnotations);
    }

    @Test
    public void testNonPreviewAnnotationPreserved() throws IOException {
        byte[] classBytes = buildClassWithAnnotatedMethod(Opcodes.ACC_PUBLIC, CALLER_SENSITIVE_ANN);
        ClassNode result = readStub(classBytes);
        assertEquals(1, result.methods.size());
        assertNotNull(result.methods.getFirst().visibleAnnotations);
        assertEquals(1, result.methods.get(0).visibleAnnotations.size());
        assertEquals(CALLER_SENSITIVE_ANN, result.methods.getFirst().visibleAnnotations.getFirst().desc);
    }

    @Test
    public void testPrivateFieldsRemoved() throws IOException {
        byte[] classBytes = buildClassWithFields();
        ClassNode result = readStub(classBytes);
        assertEquals(2, result.fields.size());
        assertTrue(result.fields.stream().allMatch(f -> f.name.equals("pubField") || f.name.equals("protField")));
    }

    @Test
    public void testPrivateMethodsRemoved() throws IOException {
        byte[] classBytes = buildClassWithMethods();
        ClassNode result = readStub(classBytes);
        List<String> names = result.methods.stream().map(m -> m.name).toList();
        assertTrue(names.contains("pubMethod"));
        assertTrue(names.contains("protMethod"));
        assertEquals(2, names.size());
    }

    @Test
    public void testMethodBodiesStripped() throws IOException {
        byte[] classBytes = buildClassWithMethodBody();
        ClassNode result = readStub(classBytes);
        assertEquals(1, result.methods.size());
        MethodNode m = result.methods.get(0);
        assertEquals(0, m.instructions.size());
        assertTrue(m.tryCatchBlocks.isEmpty());
        assertTrue(m.localVariables == null || m.localVariables.isEmpty());
        assertEquals(0, m.maxStack);
        assertEquals(0, m.maxLocals);
    }

    @Test
    public void testNonPublicInnerClassesRemoved() throws IOException {
        byte[] classBytes = buildClassWithInnerClasses();
        ClassNode result = readStub(classBytes);
        assertEquals(1, result.innerClasses.size());
        assertEquals("com/example/Outer$PubInner", result.innerClasses.get(0).name);
    }

    @Test(expected = IllegalStateException.class)
    public void testWrongClassVersionRejected_V17() throws IOException {
        byte[] classBytes = buildClass(Opcodes.ACC_PUBLIC | Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT, Opcodes.V17);
        ExtractForeignApiTask.createStub(new ByteArrayInputStream(classBytes));
    }

    @Test(expected = IllegalStateException.class)
    public void testWrongClassVersionRejected_V22() throws IOException {
        byte[] classBytes = buildClass(Opcodes.ACC_PUBLIC | Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT, Opcodes.V22);
        ExtractForeignApiTask.createStub(new ByteArrayInputStream(classBytes));
    }

    // --- helpers ---

    private static ClassNode readStub(byte[] inputClassBytes) throws IOException {
        byte[] stubBytes = ExtractForeignApiTask.createStub(new ByteArrayInputStream(inputClassBytes));
        assertNotNull("createStub returned null", stubBytes);
        ClassNode cn = new ClassNode();
        new ClassReader(stubBytes).accept(cn, 0);
        return cn;
    }

    private static void assertNoPreviewAnnotation(List<? extends AnnotationNode> annotations) {
        if (annotations == null) return;
        for (var ann : annotations) {
            if (ann.desc.equals(PREVIEW_ANN)) {
                throw new AssertionError("Found @PreviewFeature annotation that should have been stripped");
            }
        }
    }

    private static byte[] buildClass(int access, int version) {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(version, access, "com/example/TestClass", null, "java/lang/Object", null);
        cw.visitEnd();
        return cw.toByteArray();
    }

    private static byte[] buildClassWithPreviewAnnotation() {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(V21_PREVIEW, Opcodes.ACC_PUBLIC | Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT, "com/example/TestClass", null,
            "java/lang/Object", null);
        AnnotationVisitor av = cw.visitAnnotation(PREVIEW_ANN, true);
        av.visitEnd();
        cw.visitEnd();
        return cw.toByteArray();
    }

    private static byte[] buildClassWithAnnotatedMethod(int methodAccess, String annotationDesc) {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(V21_PREVIEW, Opcodes.ACC_PUBLIC | Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT, "com/example/TestClass", null,
            "java/lang/Object", null);
        MethodVisitor mv = cw.visitMethod(methodAccess | Opcodes.ACC_ABSTRACT, "testMethod", "()V", null, null);
        AnnotationVisitor av = mv.visitAnnotation(annotationDesc, true);
        av.visitEnd();
        mv.visitEnd();
        cw.visitEnd();
        return cw.toByteArray();
    }

    private static byte[] buildClassWithAnnotatedField(int fieldAccess, String annotationDesc) {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(V21_PREVIEW, Opcodes.ACC_PUBLIC, "com/example/TestClass", null, "java/lang/Object", null);
        FieldVisitor fv = cw.visitField(fieldAccess, "testField", "I", null, null);
        AnnotationVisitor av = fv.visitAnnotation(annotationDesc, true);
        av.visitEnd();
        fv.visitEnd();
        cw.visitEnd();
        return cw.toByteArray();
    }

    private static byte[] buildClassWithFields() {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(V21_PREVIEW, Opcodes.ACC_PUBLIC, "com/example/TestClass", null, "java/lang/Object", null);
        cw.visitField(Opcodes.ACC_PUBLIC, "pubField", "I", null, null).visitEnd();
        cw.visitField(Opcodes.ACC_PROTECTED, "protField", "I", null, null).visitEnd();
        cw.visitField(Opcodes.ACC_PRIVATE, "privField", "I", null, null).visitEnd();
        cw.visitField(0, "pkgField", "I", null, null).visitEnd();
        cw.visitEnd();
        return cw.toByteArray();
    }

    private static byte[] buildClassWithMethods() {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(V21_PREVIEW, Opcodes.ACC_PUBLIC | Opcodes.ACC_INTERFACE | Opcodes.ACC_ABSTRACT, "com/example/TestClass", null,
            "java/lang/Object", null);
        cw.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_ABSTRACT, "pubMethod", "()V", null, null).visitEnd();
        cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_ABSTRACT, "protMethod", "()V", null, null).visitEnd();
        cw.visitMethod(Opcodes.ACC_PRIVATE, "privMethod", "()V", null, null).visitEnd();
        cw.visitMethod(Opcodes.ACC_ABSTRACT, "pkgMethod", "()V", null, null).visitEnd();
        cw.visitEnd();
        return cw.toByteArray();
    }

    private static byte[] buildClassWithMethodBody() {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);
        cw.visit(V21_PREVIEW, Opcodes.ACC_PUBLIC, "com/example/TestClass", null, "java/lang/Object", null);
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "compute", "(II)I", null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ILOAD, 1);
        mv.visitVarInsn(Opcodes.ILOAD, 2);
        mv.visitInsn(Opcodes.IADD);
        mv.visitInsn(Opcodes.IRETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
        cw.visitEnd();
        return cw.toByteArray();
    }

    private static byte[] buildClassWithInnerClasses() {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(V21_PREVIEW, Opcodes.ACC_PUBLIC, "com/example/Outer", null, "java/lang/Object", null);
        cw.visitInnerClass("com/example/Outer$PubInner", "com/example/Outer", "PubInner",
            Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC);
        cw.visitInnerClass("com/example/Outer$PrivInner", "com/example/Outer", "PrivInner",
            Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC);
        cw.visitInnerClass("com/example/Outer$PkgInner", "com/example/Outer", "PkgInner",
            Opcodes.ACC_STATIC);
        cw.visitEnd();
        return cw.toByteArray();
    }
}
