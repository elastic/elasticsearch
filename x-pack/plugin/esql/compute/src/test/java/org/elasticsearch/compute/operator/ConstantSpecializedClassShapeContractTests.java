/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.test.ESTestCase;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.ConstantDynamic;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.function.Consumer;

/**
 * STOP. These tests assert the exact bytecode shape that {@link ConstantMethodResultSpecializer} emits
 * at runtime, via {@link ConstantSpecializedClassShapeValidator}. If you arrived here because you changed
 * the specializer's codegen and a test below now fails, the failure is the point. The spun
 * class runs with the compute module's privileges, so its shape is intentionally locked
 * down to a narrow allowlist (load constant, super-ctor call, return — that's it). Future
 * drift cannot quietly widen what a constant-specialized class is allowed to do, and a drift hit at runtime
 * throws {@link AssertionError} on purpose so the specializer cannot silently degrade to the
 * codegen Standard path.
 *
 * <p>Do not delete or relax an assertion to make this green. If your change is a legitimate
 * widening of the allowlist (for example a new value type that requires one additional
 * opcode), update {@link ConstantSpecializedClassShapeValidator} first with a comment justifying why the
 * new shape is safe, then update this file in the same commit. No silent fixes.
 *
 * <p>These tests live in a separate file from {@link ConstantMethodResultSpecializerTests} so that
 * "modify the specializer, modify the specializer tests" reflex doesn't sweep these up. The drift
 * guard is a second pair of eyes; conflating it with the same file the specializer author
 * edits defeats the intent.
 */
public class ConstantSpecializedClassShapeContractTests extends ESTestCase {

    /** A test base class with one no-arg public ctor and an abstract long accessor. */
    public abstract static class LongBase {
        public LongBase() {}

        protected abstract long divisor();
    }

    // ===== Shared helpers =====

    /** Build the canonical "good" primitive long spec the specializer emits for LongBase / divisor. */
    private static ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec longSpec() {
        return new ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec(
            LongBase.class,
            Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
            "divisor",
            "()J",
            "CONST_DIVISOR",
            "J",
            Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL,
            /* expectsClinit */ false,
            /* expectedCtorCount */ 1
        );
    }

    /** Emit a canonical "good" primitive-long class against {@code cw} (no guard). */
    private static void writeCanonicalGoodPrimitiveLongClass(ClassWriter cw) {
        String baseInternal = Type.getInternalName(LongBase.class);
        String constantSpecializedInternal = baseInternal + "$ConstantSpecialized";
        cw.visit(
            Opcodes.V21,
            Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
            constantSpecializedInternal,
            null,
            baseInternal,
            null
        );
        cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
        MethodVisitor ctor = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        ctor.visitCode();
        ctor.visitVarInsn(Opcodes.ALOAD, 0);
        ctor.visitMethodInsn(Opcodes.INVOKESPECIAL, baseInternal, "<init>", "()V", false);
        ctor.visitInsn(Opcodes.RETURN);
        ctor.visitMaxs(0, 0);
        ctor.visitEnd();
        MethodVisitor m = cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, "divisor", "()J", null, null);
        m.visitCode();
        m.visitFieldInsn(Opcodes.GETSTATIC, constantSpecializedInternal, "CONST_DIVISOR", "J");
        m.visitInsn(Opcodes.LRETURN);
        m.visitMaxs(0, 0);
        m.visitEnd();
        cw.visitEnd();
    }

    /** Assert the validator rejects {@code bytecode} with a message containing {@code expectedFragment}. */
    private static void assertRejects(
        byte[] bytecode,
        ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec spec,
        String expectedFragment
    ) {
        AssertionError err = expectThrows(AssertionError.class, () -> ConstantSpecializedClassShapeValidator.validate(bytecode, spec));
        assertTrue(
            "drift message [" + err.getMessage() + "] must contain [" + expectedFragment + "]",
            err.getMessage() != null && err.getMessage().contains(expectedFragment)
        );
    }

    /** Build a class against the canonical primitive-long shape, then apply a single mutation, and validate. */
    private static byte[] withMutation(Consumer<ClassWriter> mutator) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        mutator.accept(cw);
        return cw.toByteArray();
    }

    // ===== Positive: every shape the specializer actually emits is accepted =====

    public void testAcceptsCanonicalGoodShape() {
        byte[] bytes = withMutation(ConstantSpecializedClassShapeContractTests::writeCanonicalGoodPrimitiveLongClass);
        ConstantSpecializedClassShapeValidator.validate(bytes, longSpec());
    }

    public void testCountPublicCtorsReportsExpected() {
        assertEquals(1, ConstantSpecializedClassShapeValidator.countPublicCtors(LongBase.class));
    }

    // ===== Negative: CLASS-LEVEL drift =====

    public void testRejectsWrongSuperclass() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V21,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                null,
                "java/lang/Object",
                null
            );
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
            MethodVisitor c = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
            c.visitCode();
            c.visitVarInsn(Opcodes.ALOAD, 0);
            c.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
            c.visitInsn(Opcodes.RETURN);
            c.visitMaxs(0, 0);
            c.visitEnd();
            MethodVisitor m = cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, "divisor", "()J", null, null);
            m.visitCode();
            m.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(LongBase.class) + "$ConstantSpecialized", "CONST_DIVISOR", "J");
            m.visitInsn(Opcodes.LRETURN);
            m.visitMaxs(0, 0);
            m.visitEnd();
            cw.visitEnd();
        });
        assertRejects(bytes, longSpec(), "super class");
    }

    public void testRejectsDeclaredInterface() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V21,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                null,
                Type.getInternalName(LongBase.class),
                new String[] { "java/lang/Runnable" }
            );
        });
        assertRejects(bytes, longSpec(), "interfaces");
    }

    public void testRejectsWrongClassAccessFlags() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V21,
                Opcodes.ACC_PUBLIC, // missing FINAL | SUPER
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                null,
                Type.getInternalName(LongBase.class),
                null
            );
        });
        assertRejects(bytes, longSpec(), "class access");
    }

    public void testRejectsWrongClassVersion() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V17,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                null,
                Type.getInternalName(LongBase.class),
                null
            );
        });
        assertRejects(bytes, longSpec(), "class version");
    }

    public void testRejectsClassGenericSignature() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V21,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                "L" + Type.getInternalName(LongBase.class) + "<TT;>;",
                Type.getInternalName(LongBase.class),
                null
            );
        });
        assertRejects(bytes, longSpec(), "generic signature");
    }

    public void testRejectsClassAnnotation() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V21,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                null,
                Type.getInternalName(LongBase.class),
                null
            );
            cw.visitAnnotation("Ljava/lang/Deprecated;", true);
        });
        assertRejects(bytes, longSpec(), "annotation");
    }

    public void testRejectsSourceAttribute() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V21,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                null,
                Type.getInternalName(LongBase.class),
                null
            );
            cw.visitSource("Foo.java", null);
        });
        assertRejects(bytes, longSpec(), "visitSource");
    }

    public void testRejectsInnerClass() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V21,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                null,
                Type.getInternalName(LongBase.class),
                null
            );
            cw.visitInnerClass("Foo$Inner", "Foo", "Inner", Opcodes.ACC_PUBLIC);
        });
        assertRejects(bytes, longSpec(), "inner class");
    }

    public void testRejectsNestMember() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V21,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                null,
                Type.getInternalName(LongBase.class),
                null
            );
            cw.visitNestMember("Foo$NestMember");
        });
        assertRejects(bytes, longSpec(), "visitNestMember");
    }

    public void testRejectsNestHost() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V21,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                null,
                Type.getInternalName(LongBase.class),
                null
            );
            cw.visitNestHost("Foo");
        });
        assertRejects(bytes, longSpec(), "visitNestHost");
    }

    public void testRejectsOuterClass() {
        byte[] bytes = withMutation(cw -> {
            cw.visit(
                Opcodes.V21,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
                Type.getInternalName(LongBase.class) + "$ConstantSpecialized",
                null,
                Type.getInternalName(LongBase.class),
                null
            );
            cw.visitOuterClass("Foo", null, null);
        });
        assertRejects(bytes, longSpec(), "visitOuterClass");
    }

    // ===== Negative: FIELD-LEVEL drift =====

    public void testRejectsSecondField() {
        byte[] bytes = withMutation(cw -> {
            writeCanonicalGoodPrimitiveLongClass(cw); // emits one good field
            // We can't easily "add a second field after visitEnd" because ASM throws; instead build a bespoke shape.
        });
        // Build a shape that emits two fields directly:
        byte[] twoFields = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "EXTRA", "J", null, 0L).visitEnd();
        });
        assertRejects(twoFields, longSpec(), "second field");
    }

    public void testRejectsWrongFieldName() {
        byte[] bytes = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "NOT_CONST_DIVISOR", "J", null, 60L).visitEnd();
        });
        assertRejects(bytes, longSpec(), "field name");
    }

    public void testRejectsWrongFieldDescriptor() {
        byte[] bytes = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "I", null, 60).visitEnd();
        });
        assertRejects(bytes, longSpec(), "field descriptor");
    }

    public void testRejectsWrongFieldAccess() {
        byte[] bytes = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            // Missing FINAL — declares mutable.
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC, "CONST_DIVISOR", "J", null, 60L).visitEnd();
        });
        assertRejects(bytes, longSpec(), "field access");
    }

    public void testRejectsFieldGenericSignature() {
        byte[] bytes = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", "TT;", 60L).visitEnd();
        });
        assertRejects(bytes, longSpec(), "generic signature");
    }

    // ===== Negative: METHOD-LEVEL drift (counts + access + names) =====

    public void testRejectsExtraMethod() {
        byte[] bytes = withMutation(cw -> {
            writeCanonicalGoodPrimitiveLongClass(cw);
            // can't append after visitEnd; build bespoke:
        });
        byte[] extra = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
            MethodVisitor c = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
            c.visitCode();
            c.visitVarInsn(Opcodes.ALOAD, 0);
            c.visitMethodInsn(Opcodes.INVOKESPECIAL, b, "<init>", "()V", false);
            c.visitInsn(Opcodes.RETURN);
            c.visitMaxs(0, 0);
            c.visitEnd();
            MethodVisitor m = cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, "divisor", "()J", null, null);
            m.visitCode();
            m.visitFieldInsn(Opcodes.GETSTATIC, s, "CONST_DIVISOR", "J");
            m.visitInsn(Opcodes.LRETURN);
            m.visitMaxs(0, 0);
            m.visitEnd();
            // unexpected extra method:
            MethodVisitor extraM = cw.visitMethod(Opcodes.ACC_PRIVATE, "secretHelper", "()V", null, null);
            extraM.visitCode();
            extraM.visitInsn(Opcodes.RETURN);
            extraM.visitMaxs(0, 0);
            extraM.visitEnd();
        });
        assertRejects(extra, longSpec(), "unexpected method name");
    }

    public void testRejectsSecondAccessor() {
        byte[] bytes = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
            MethodVisitor c = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
            c.visitCode();
            c.visitVarInsn(Opcodes.ALOAD, 0);
            c.visitMethodInsn(Opcodes.INVOKESPECIAL, b, "<init>", "()V", false);
            c.visitInsn(Opcodes.RETURN);
            c.visitMaxs(0, 0);
            c.visitEnd();
            for (int i = 0; i < 2; i++) {
                MethodVisitor m = cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, "divisor", "()J", null, null);
                m.visitCode();
                m.visitFieldInsn(Opcodes.GETSTATIC, s, "CONST_DIVISOR", "J");
                m.visitInsn(Opcodes.LRETURN);
                m.visitMaxs(0, 0);
                m.visitEnd();
            }
        });
        assertRejects(bytes, longSpec(), "more than one accessor");
    }

    public void testRejectsWrongCtorAccess() {
        byte[] bytes = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
            // ctor declared private — spinner promises public
            MethodVisitor c = cw.visitMethod(Opcodes.ACC_PRIVATE, "<init>", "()V", null, null);
            c.visitCode();
            c.visitVarInsn(Opcodes.ALOAD, 0);
            c.visitMethodInsn(Opcodes.INVOKESPECIAL, b, "<init>", "()V", false);
            c.visitInsn(Opcodes.RETURN);
            c.visitMaxs(0, 0);
            c.visitEnd();
        });
        assertRejects(bytes, longSpec(), "ctor access");
    }

    public void testRejectsWrongAccessorAccess() {
        byte[] bytes = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
            MethodVisitor c = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
            c.visitCode();
            c.visitVarInsn(Opcodes.ALOAD, 0);
            c.visitMethodInsn(Opcodes.INVOKESPECIAL, b, "<init>", "()V", false);
            c.visitInsn(Opcodes.RETURN);
            c.visitMaxs(0, 0);
            c.visitEnd();
            // accessor declared public final, not protected final
            MethodVisitor m = cw.visitMethod(Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL, "divisor", "()J", null, null);
            m.visitCode();
            m.visitFieldInsn(Opcodes.GETSTATIC, s, "CONST_DIVISOR", "J");
            m.visitInsn(Opcodes.LRETURN);
            m.visitMaxs(0, 0);
            m.visitEnd();
        });
        assertRejects(bytes, longSpec(), "accessor access");
    }

    public void testRejectsUnexpectedMethodName() {
        byte[] bytes = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
            MethodVisitor c = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
            c.visitCode();
            c.visitVarInsn(Opcodes.ALOAD, 0);
            c.visitMethodInsn(Opcodes.INVOKESPECIAL, b, "<init>", "()V", false);
            c.visitInsn(Opcodes.RETURN);
            c.visitMaxs(0, 0);
            c.visitEnd();
            // wrong accessor name
            MethodVisitor m = cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, "notTheAccessor", "()J", null, null);
            m.visitCode();
            m.visitFieldInsn(Opcodes.GETSTATIC, s, "CONST_DIVISOR", "J");
            m.visitInsn(Opcodes.LRETURN);
            m.visitMaxs(0, 0);
            m.visitEnd();
        });
        assertRejects(bytes, longSpec(), "unexpected method name");
    }

    public void testRejectsMissingAccessor() {
        byte[] bytes = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
            MethodVisitor c = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
            c.visitCode();
            c.visitVarInsn(Opcodes.ALOAD, 0);
            c.visitMethodInsn(Opcodes.INVOKESPECIAL, b, "<init>", "()V", false);
            c.visitInsn(Opcodes.RETURN);
            c.visitMaxs(0, 0);
            c.visitEnd();
            cw.visitEnd();
        });
        assertRejects(bytes, longSpec(), "0 accessors");
    }

    public void testRejectsUnexpectedClinitForPrimitive() {
        byte[] bytes = withMutation(cw -> {
            String b = Type.getInternalName(LongBase.class);
            String s = b + "$ConstantSpecialized";
            cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
            cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
            MethodVisitor c = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
            c.visitCode();
            c.visitVarInsn(Opcodes.ALOAD, 0);
            c.visitMethodInsn(Opcodes.INVOKESPECIAL, b, "<init>", "()V", false);
            c.visitInsn(Opcodes.RETURN);
            c.visitMaxs(0, 0);
            c.visitEnd();
            // <clinit> NOT expected for primitive constant path (initialized via field initial value)
            MethodVisitor cl = cw.visitMethod(Opcodes.ACC_STATIC, "<clinit>", "()V", null, null);
            cl.visitCode();
            cl.visitInsn(Opcodes.RETURN);
            cl.visitMaxs(0, 0);
            cl.visitEnd();
        });
        assertRejects(bytes, longSpec(), "<clinit> emitted but spec expects none");
    }

    // ===== Negative: METHOD BODY (opcode allowlist) =====

    /** Build a class whose accessor body is replaced with the caller-provided emission. */
    private static byte[] withAccessorBody(Consumer<MethodVisitor> body) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
        String b = Type.getInternalName(LongBase.class);
        String s = b + "$ConstantSpecialized";
        cw.visit(Opcodes.V21, Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER, s, null, b, null);
        cw.visitField(Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_FINAL, "CONST_DIVISOR", "J", null, 60L).visitEnd();
        MethodVisitor c = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        c.visitCode();
        c.visitVarInsn(Opcodes.ALOAD, 0);
        c.visitMethodInsn(Opcodes.INVOKESPECIAL, b, "<init>", "()V", false);
        c.visitInsn(Opcodes.RETURN);
        c.visitMaxs(0, 0);
        c.visitEnd();
        MethodVisitor m = cw.visitMethod(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL, "divisor", "()J", null, null);
        m.visitCode();
        body.accept(m);
        m.visitMaxs(0, 0);
        m.visitEnd();
        cw.visitEnd();
        return cw.toByteArray();
    }

    /**
     * Drive the body through {@link ConstantSpecializedClassShapeValidator#guarding} directly, bypassing
     * ClassWriter+ClassReader. Use this when the test wants to verify a specific {@code visit*}
     * override fires — building real bytecode requires valid stack/frame setup which would
     * tickle our allowlist before the intended op (e.g. you can't push an int on stack for
     * TABLESWITCH using any allowed opcode). Direct emit lets the test focus precisely on
     * which override we're checking.
     */
    private static void assertGuardRejectsBody(Consumer<MethodVisitor> body, String expectedFragment) {
        ConstantSpecializedClassShapeValidator.ConstantSpecializedClassSpec spec = longSpec();
        org.objectweb.asm.ClassVisitor guard = ConstantSpecializedClassShapeValidator.guarding(
            new org.objectweb.asm.ClassVisitor(Opcodes.ASM9) {},
            spec
        );
        guard.visit(
            Opcodes.V21,
            Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER,
            spec.constantSpecializedInternalName(),
            null,
            spec.expectedSuperInternalName(),
            null
        );
        guard.visitField(spec.expectedFieldAccess(), spec.fieldName(), spec.fieldDescriptor(), null, 60L).visitEnd();
        MethodVisitor c = guard.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        c.visitCode();
        c.visitVarInsn(Opcodes.ALOAD, 0);
        c.visitMethodInsn(Opcodes.INVOKESPECIAL, spec.expectedSuperInternalName(), "<init>", "()V", false);
        c.visitInsn(Opcodes.RETURN);
        c.visitMaxs(0, 0);
        c.visitEnd();
        MethodVisitor m = guard.visitMethod(
            Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL,
            spec.accessorName(),
            spec.accessorDescriptor(),
            null,
            null
        );
        m.visitCode();
        AssertionError err = expectThrows(AssertionError.class, () -> body.accept(m));
        assertTrue(
            "drift message [" + err.getMessage() + "] must contain [" + expectedFragment + "]",
            err.getMessage() != null && err.getMessage().contains(expectedFragment)
        );
    }

    public void testRejectsInvokestaticInAccessor() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/System", "currentTimeMillis", "()J", false);
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "method-insn");
    }

    public void testRejectsInvokevirtualInAccessor() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitVarInsn(Opcodes.ALOAD, 0);
            m.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Object", "hashCode", "()I", false);
            m.visitInsn(Opcodes.IRETURN);
        });
        assertRejects(bytes, longSpec(), "method-insn");
    }

    public void testRejectsInvokeinterfaceInAccessor() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitVarInsn(Opcodes.ALOAD, 0);
            m.visitMethodInsn(Opcodes.INVOKEINTERFACE, "java/util/List", "size", "()I", true);
            m.visitInsn(Opcodes.IRETURN);
        });
        assertRejects(bytes, longSpec(), "method-insn");
    }

    public void testRejectsInvokedynamic() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitInvokeDynamicInsn(
                "foo",
                "()V",
                new Handle(
                    Opcodes.H_INVOKESTATIC,
                    "java/lang/invoke/LambdaMetafactory",
                    "metafactory",
                    "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;"
                        + "Ljava/lang/invoke/MethodType;Ljava/lang/invoke/MethodHandle;Ljava/lang/invoke/MethodType;)"
                        + "Ljava/lang/invoke/CallSite;",
                    false
                )
            );
            m.visitInsn(Opcodes.LCONST_0);
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "INVOKEDYNAMIC");
    }

    public void testRejectsInvokespecialOfForeignOwner() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitVarInsn(Opcodes.ALOAD, 0);
            m.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "toString", "()Ljava/lang/String;", false);
            m.visitInsn(Opcodes.ARETURN);
        });
        assertRejects(bytes, longSpec(), "foreign owner");
    }

    public void testRejectsInvokespecialOfNonInit() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitVarInsn(Opcodes.ALOAD, 0);
            m.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(LongBase.class), "divisor", "()J", false);
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "only <init> allowed");
    }

    public void testRejectsPutfield() {
        assertGuardRejectsBody(m -> {
            m.visitVarInsn(Opcodes.ALOAD, 0);
            m.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(LongBase.class) + "$ConstantSpecialized", "CONST_DIVISOR", "J");
            m.visitFieldInsn(Opcodes.PUTFIELD, Type.getInternalName(LongBase.class) + "$ConstantSpecialized", "CONST_DIVISOR", "J");
        }, "field-insn");
    }

    public void testRejectsGetfield() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitVarInsn(Opcodes.ALOAD, 0);
            m.visitFieldInsn(Opcodes.GETFIELD, Type.getInternalName(LongBase.class) + "$ConstantSpecialized", "CONST_DIVISOR", "J");
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "field-insn");
    }

    public void testRejectsPutstaticOutsideClinit() {
        assertGuardRejectsBody(m -> {
            m.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(LongBase.class) + "$ConstantSpecialized", "CONST_DIVISOR", "J");
            m.visitFieldInsn(Opcodes.PUTSTATIC, Type.getInternalName(LongBase.class) + "$ConstantSpecialized", "CONST_DIVISOR", "J");
        }, "field-insn");
    }

    public void testRejectsFieldOpOnForeignOwner() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitFieldInsn(Opcodes.GETSTATIC, "java/lang/Long", "MAX_VALUE", "J");
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "foreign owner");
    }

    public void testRejectsFieldOpOnWrongFieldName() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(LongBase.class) + "$ConstantSpecialized", "OTHER_FIELD", "J");
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "unexpected field");
    }

    public void testRejectsLdcOfString() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitLdcInsn("a string");
            m.visitInsn(Opcodes.ARETURN);
        });
        assertRejects(bytes, longSpec(), "LDC");
    }

    public void testRejectsLdcOfInteger() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitLdcInsn(Integer.valueOf(42));
            m.visitInsn(Opcodes.IRETURN);
        });
        assertRejects(bytes, longSpec(), "LDC");
    }

    public void testRejectsConstantDynamicOutsideClinit() {
        byte[] bytes = withAccessorBody(m -> {
            Handle h = new Handle(
                Opcodes.H_INVOKESTATIC,
                "java/lang/invoke/MethodHandles",
                "classData",
                "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/Class;)Ljava/lang/Object;",
                false
            );
            m.visitLdcInsn(new ConstantDynamic("x", "J", h));
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "LDC only allowed in <clinit>");
    }

    public void testRejectsJumpInsn() {
        byte[] bytes = withAccessorBody(m -> {
            Label end = new Label();
            m.visitJumpInsn(Opcodes.GOTO, end);
            m.visitLabel(end);
            m.visitInsn(Opcodes.LCONST_0);
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "jump-insn");
    }

    public void testRejectsIincInsn() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitIincInsn(0, 1);
            m.visitInsn(Opcodes.LCONST_0);
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "IINC");
    }

    public void testRejectsMultiAnewArray() {
        assertGuardRejectsBody(m -> m.visitMultiANewArrayInsn("[[I", 2), "MULTIANEWARRAY");
    }

    public void testRejectsTableSwitch() {
        assertGuardRejectsBody(m -> {
            Label dflt = new Label();
            Label c0 = new Label();
            m.visitTableSwitchInsn(0, 0, dflt, c0);
        }, "TABLESWITCH");
    }

    public void testRejectsLookupSwitch() {
        assertGuardRejectsBody(m -> {
            Label dflt = new Label();
            Label c0 = new Label();
            m.visitLookupSwitchInsn(dflt, new int[] { 0 }, new Label[] { c0 });
        }, "LOOKUPSWITCH");
    }

    public void testRejectsTryCatch() {
        byte[] bytes = withAccessorBody(m -> {
            Label start = new Label();
            Label end = new Label();
            Label handler = new Label();
            m.visitTryCatchBlock(start, end, handler, "java/lang/Throwable");
            m.visitLabel(start);
            m.visitLabel(end);
            m.visitLabel(handler);
            m.visitInsn(Opcodes.POP);
            m.visitInsn(Opcodes.LCONST_0);
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "try/catch");
    }

    public void testRejectsTypeInsn() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitTypeInsn(Opcodes.NEW, "java/lang/Object");
            m.visitInsn(Opcodes.LCONST_0);
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "type-insn");
    }

    public void testRejectsIntInsn() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitIntInsn(Opcodes.BIPUSH, 42);
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "int-insn");
    }

    public void testRejectsNonReturnZeroOperandOpcode() {
        // ICONST_1 has no operands; it's allowed in *clinit* world only via LDC condy in our spec — never via visitInsn here.
        byte[] bytes = withAccessorBody(m -> {
            m.visitInsn(Opcodes.ICONST_1);
            m.visitInsn(Opcodes.LCONST_0);
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "zero-operand opcode");
    }

    public void testRejectsStoreInsn() {
        // ASTORE etc. would let a method spill state to a local — we never do this.
        assertGuardRejectsBody(m -> {
            m.visitFieldInsn(Opcodes.GETSTATIC, Type.getInternalName(LongBase.class) + "$ConstantSpecialized", "CONST_DIVISOR", "J");
            m.visitVarInsn(Opcodes.LSTORE, 1);
        }, "var-insn");
    }

    public void testRejectsLineNumberDebugInfo() {
        assertGuardRejectsBody(m -> {
            Label l = new Label();
            m.visitLabel(l);
            m.visitLineNumber(42, l);
        }, "line number");
    }

    public void testRejectsLocalVariableDebugInfo() {
        assertGuardRejectsBody(m -> {
            Label start = new Label();
            Label end = new Label();
            m.visitLabel(start);
            m.visitLabel(end);
            m.visitLocalVariable("local", "J", null, start, end, 1);
        }, "local variable");
    }

    public void testRejectsMethodAnnotation() {
        byte[] bytes = withAccessorBody(m -> {
            m.visitAnnotation("Ljava/lang/Deprecated;", true);
            m.visitInsn(Opcodes.LCONST_0);
            m.visitInsn(Opcodes.LRETURN);
        });
        assertRejects(bytes, longSpec(), "method annotation");
    }
}
