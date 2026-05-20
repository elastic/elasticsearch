/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ConstantDynamic;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.ModuleVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.RecordComponentVisitor;
import org.objectweb.asm.Type;
import org.objectweb.asm.TypePath;

import java.lang.reflect.Modifier;

/**
 * Structural drift guard for {@link JitConstantSpinner}'s emitted hidden classes.
 *
 * <p>The spinner emits an extremely narrow class shape per spun subclass: one mirror
 * of each public base constructor (chained super call, no body), one constant-returning
 * accessor that reads a {@code static final} field and returns it, and — for reference
 * constants — one {@code <clinit>} that loads a {@code ConstantDynamic} via condy and
 * stores it into the field. That's it. No other fields, no other methods, no inner
 * classes, no annotations, no module/nest/record metadata, no debug attributes, no
 * control flow inside method bodies, no foreign method calls, no instance field access.
 *
 * <p>This validator wraps a {@link ClassVisitor} (production: the spinner's
 * {@code ClassWriter}; tests: a no-op delegate) and asserts every {@code visit*} call
 * against that shape <em>at emit time</em>, before the bytecode is assembled or the
 * hidden class loaded. Anything that doesn't match throws {@link AssertionError}
 * synchronously.
 *
 * <p><b>Drift is treated as a hard failure on purpose.</b> {@link AssertionError} is
 * deliberately chosen so that the spinner's outer {@code catch (RuntimeException)}
 * (which routes ordinary spin failures to the codegen Standard) does <em>not</em>
 * catch it. A drifted emitter is unsafe territory — silently degrading to Standard
 * would mask the problem and leave the codebase one PR away from a real defect. The
 * error propagates out of {@code defineHiddenClass}, out of {@code computeIfAbsent},
 * up the call stack, aborts the query, and (via ES's standard uncaught-error handling)
 * escalates to a node-level halt. Better to crash visibly now than emit something we
 * didn't expect into a JVM-loaded hidden class.
 *
 * <p>The validator is kept in its own class on purpose. Drift in {@code JitConstantSpinner}
 * itself is exactly what this is meant to catch; conflating "what we emit" with "what we
 * promised to emit" in a single file would defeat the second-pair-of-eyes intent.
 */
final class SpunClassShapeValidator {

    /**
     * Immutable contract describing the exact shape we promise to emit for one spun class.
     * The spinner constructs one of these per spin and passes it to {@link #guarding}.
     */
    record SpunClassSpec(
        Class<?> baseClass,
        String spunInternalName,
        String accessorName,
        String accessorDescriptor,
        String fieldName,
        String fieldDescriptor,
        int expectedFieldAccess,
        boolean expectsClinit,
        int expectedCtorCount
    ) {
        String expectedSuperInternalName() {
            return Type.getInternalName(baseClass);
        }
    }

    private SpunClassShapeValidator() {}

    /** Production entry: every {@code visit*} the spinner calls on the returned visitor is asserted. */
    static ClassVisitor guarding(ClassVisitor delegate, SpunClassSpec spec) {
        return new GuardingClassVisitor(delegate, spec);
    }

    /**
     * Test-only entry: validate a hand-crafted byte[] against {@code spec} by replaying
     * it through the same guard with a no-op delegate. Production never calls this —
     * {@link #guarding} is the active check at emit time.
     */
    static void validate(byte[] bytecode, SpunClassSpec spec) {
        new ClassReader(bytecode).accept(new GuardingClassVisitor(new ClassVisitor(Opcodes.ASM9) {}, spec), ClassReader.SKIP_FRAMES);
    }

    // ----- Class-level guard -----

    private static final int EXPECTED_CLASS_ACCESS = Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER;
    private static final int EXPECTED_CLASS_VERSION = Opcodes.V21;
    private static final int EXPECTED_CTOR_ACCESS = Opcodes.ACC_PUBLIC;
    private static final int EXPECTED_ACCESSOR_ACCESS = Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL;
    private static final int EXPECTED_CLINIT_ACCESS = Opcodes.ACC_STATIC;

    private static final class GuardingClassVisitor extends ClassVisitor {
        private final SpunClassSpec spec;
        private int fieldCount = 0;
        private int ctorCount = 0;
        private int accessorCount = 0;
        private int clinitCount = 0;
        private boolean visitedHeader = false;

        GuardingClassVisitor(ClassVisitor delegate, SpunClassSpec spec) {
            super(Opcodes.ASM9, delegate);
            this.spec = spec;
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            visitedHeader = true;
            if (version != EXPECTED_CLASS_VERSION) {
                throw drift("class version is " + version + ", expected " + EXPECTED_CLASS_VERSION);
            }
            if (access != EXPECTED_CLASS_ACCESS) {
                throw drift(
                    "class access is 0x" + Integer.toHexString(access) + ", expected 0x" + Integer.toHexString(EXPECTED_CLASS_ACCESS)
                );
            }
            if (spec.spunInternalName.equals(name) == false) {
                throw drift("class internal name is [" + name + "], expected [" + spec.spunInternalName + "]");
            }
            if (spec.expectedSuperInternalName().equals(superName) == false) {
                throw drift("super class is [" + superName + "], expected [" + spec.expectedSuperInternalName() + "]");
            }
            if (interfaces != null && interfaces.length > 0) {
                throw drift("spun class declares interfaces " + java.util.Arrays.toString(interfaces) + " (none allowed)");
            }
            if (signature != null) {
                throw drift("spun class declares a generic signature (none allowed)");
            }
            super.visit(version, access, name, signature, superName, interfaces);
        }

        @Override
        public FieldVisitor visitField(int access, String name, String descriptor, String signature, Object value) {
            if (fieldCount > 0) {
                throw drift("second field [" + name + "] emitted; spinner promises exactly one");
            }
            if (spec.fieldName.equals(name) == false) {
                throw drift("field name is [" + name + "], expected [" + spec.fieldName + "]");
            }
            if (spec.fieldDescriptor.equals(descriptor) == false) {
                throw drift("field descriptor is [" + descriptor + "], expected [" + spec.fieldDescriptor + "]");
            }
            if (access != spec.expectedFieldAccess) {
                throw drift(
                    "field access is 0x" + Integer.toHexString(access) + ", expected 0x" + Integer.toHexString(spec.expectedFieldAccess)
                );
            }
            if (signature != null) {
                throw drift("field [" + name + "] declares a generic signature (none allowed)");
            }
            fieldCount++;
            FieldVisitor delegate = super.visitField(access, name, descriptor, signature, value);
            return new GuardingFieldVisitor(delegate);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
            if (signature != null) {
                throw drift("method [" + name + "] declares a generic signature (none allowed)");
            }
            if (exceptions != null && exceptions.length > 0) {
                throw drift("method [" + name + "] declares throws " + java.util.Arrays.toString(exceptions) + " (none allowed)");
            }

            switch (name) {
                case "<init>":
                    ctorCount++;
                    if (ctorCount > spec.expectedCtorCount) {
                        throw drift(
                            "emitted "
                                + ctorCount
                                + " ctors; base "
                                + spec.baseClass.getName()
                                + " has "
                                + spec.expectedCtorCount
                                + " public ctor(s)"
                        );
                    }
                    if (access != EXPECTED_CTOR_ACCESS) {
                        throw drift(
                            "ctor access is 0x" + Integer.toHexString(access) + ", expected 0x" + Integer.toHexString(EXPECTED_CTOR_ACCESS)
                        );
                    }
                    break;
                case "<clinit>":
                    clinitCount++;
                    if (spec.expectsClinit == false) {
                        throw drift("<clinit> emitted but spec expects none (primitive constant path)");
                    }
                    if (clinitCount > 1) {
                        throw drift("more than one <clinit> emitted");
                    }
                    if (access != EXPECTED_CLINIT_ACCESS) {
                        throw drift(
                            "<clinit> access is 0x"
                                + Integer.toHexString(access)
                                + ", expected 0x"
                                + Integer.toHexString(EXPECTED_CLINIT_ACCESS)
                        );
                    }
                    if ("()V".equals(descriptor) == false) {
                        throw drift("<clinit> descriptor is [" + descriptor + "], expected ()V");
                    }
                    break;
                default:
                    if (spec.accessorName.equals(name) == false) {
                        throw drift("unexpected method name [" + name + "] (allowed: <init>, <clinit>, " + spec.accessorName + ")");
                    }
                    accessorCount++;
                    if (accessorCount > 1) {
                        throw drift("more than one accessor [" + spec.accessorName + "] emitted");
                    }
                    if (access != EXPECTED_ACCESSOR_ACCESS) {
                        throw drift(
                            "accessor access is 0x"
                                + Integer.toHexString(access)
                                + ", expected 0x"
                                + Integer.toHexString(EXPECTED_ACCESSOR_ACCESS)
                        );
                    }
                    if (spec.accessorDescriptor.equals(descriptor) == false) {
                        throw drift("accessor descriptor is [" + descriptor + "], expected [" + spec.accessorDescriptor + "]");
                    }
            }
            MethodVisitor delegate = super.visitMethod(access, name, descriptor, signature, exceptions);
            return new GuardingMethodVisitor(delegate, name, spec);
        }

        @Override
        public void visitEnd() {
            if (visitedHeader == false) throw drift("visitEnd before visit(): missing class header");
            if (fieldCount != 1) throw drift("emitted " + fieldCount + " fields; expected exactly 1");
            if (ctorCount != spec.expectedCtorCount) throw drift("emitted " + ctorCount + " ctors; expected " + spec.expectedCtorCount);
            if (accessorCount != 1) throw drift("emitted " + accessorCount + " accessors; expected exactly 1");
            int expectedClinits = spec.expectsClinit ? 1 : 0;
            if (clinitCount != expectedClinits) throw drift("emitted " + clinitCount + " <clinit>s; expected " + expectedClinits);
            super.visitEnd();
        }

        // ----- All other class-level visit* points: reject. We don't emit any of these. -----

        @Override
        public void visitSource(String source, String debug) {
            throw drift("visitSource (debug source attribute) not allowed");
        }

        @Override
        public ModuleVisitor visitModule(String n, int a, String v) {
            throw drift("visitModule not allowed");
        }

        @Override
        public void visitNestHost(String nestHost) {
            throw drift("visitNestHost not allowed");
        }

        @Override
        public void visitOuterClass(String o, String n, String d) {
            throw drift("visitOuterClass not allowed");
        }

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean v) {
            throw drift("class annotation [" + desc + "] not allowed");
        }

        @Override
        public AnnotationVisitor visitTypeAnnotation(int tr, TypePath tp, String d, boolean v) {
            throw drift("class type annotation not allowed");
        }

        @Override
        public void visitAttribute(Attribute attr) {
            throw drift("class attribute [" + attr.type + "] not allowed");
        }

        @Override
        public void visitNestMember(String nestMember) {
            throw drift("visitNestMember not allowed");
        }

        @Override
        public void visitPermittedSubclass(String permittedSubclass) {
            throw drift("visitPermittedSubclass not allowed");
        }

        @Override
        public void visitInnerClass(String n, String o, String i, int a) {
            throw drift("inner class [" + n + "] not allowed");
        }

        @Override
        public RecordComponentVisitor visitRecordComponent(String n, String d, String s) {
            throw drift("record component not allowed");
        }

        private AssertionError drift(String what) {
            return new AssertionError("JitConstantSpinner shape drift in spun [" + spec.spunInternalName + "]: " + what);
        }
    }

    // ----- Field-level guard (just reject extras on the field) -----

    private static final class GuardingFieldVisitor extends FieldVisitor {
        GuardingFieldVisitor(FieldVisitor delegate) {
            super(Opcodes.ASM9, delegate);
        }

        @Override
        public AnnotationVisitor visitAnnotation(String d, boolean v) {
            throw new AssertionError("field annotation not allowed");
        }

        @Override
        public AnnotationVisitor visitTypeAnnotation(int t, TypePath p, String d, boolean v) {
            throw new AssertionError("field type annotation not allowed");
        }

        @Override
        public void visitAttribute(Attribute a) {
            throw new AssertionError("field attribute not allowed");
        }
    }

    // ----- Method-level guard (opcode allowlist + reject method-level extras) -----

    private static final class GuardingMethodVisitor extends MethodVisitor {
        private final String mName;
        private final SpunClassSpec spec;
        private final String expectedSuper;

        GuardingMethodVisitor(MethodVisitor delegate, String mName, SpunClassSpec spec) {
            super(Opcodes.ASM9, delegate);
            this.mName = mName;
            this.spec = spec;
            this.expectedSuper = spec.expectedSuperInternalName();
        }

        // ---- Opcode allowlist ----

        @Override
        public void visitInsn(int opcode) {
            switch (opcode) {
                case Opcodes.RETURN, Opcodes.IRETURN, Opcodes.LRETURN, Opcodes.FRETURN, Opcodes.DRETURN, Opcodes.ARETURN -> super.visitInsn(
                    opcode
                );
                default -> throw drift("unexpected zero-operand opcode " + opcode);
            }
        }

        @Override
        public void visitVarInsn(int opcode, int var) {
            switch (opcode) {
                case Opcodes.ALOAD, Opcodes.ILOAD, Opcodes.LLOAD, Opcodes.FLOAD, Opcodes.DLOAD -> super.visitVarInsn(opcode, var);
                default -> throw drift("unexpected var-insn opcode " + opcode + " (only *LOAD allowed)");
            }
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String descriptor) {
            if (spec.spunInternalName.equals(owner) == false) {
                throw drift("field op on foreign owner [" + owner + "]");
            }
            if (spec.fieldName.equals(name) == false) {
                throw drift("field op on unexpected field [" + name + "]");
            }
            if (spec.fieldDescriptor.equals(descriptor) == false) {
                throw drift("field op with unexpected descriptor [" + descriptor + "]");
            }
            if (opcode == Opcodes.GETSTATIC) {
                super.visitFieldInsn(opcode, owner, name, descriptor);
                return;
            }
            if (opcode == Opcodes.PUTSTATIC && "<clinit>".equals(mName)) {
                super.visitFieldInsn(opcode, owner, name, descriptor);
                return;
            }
            throw drift("unexpected field-insn opcode " + opcode + " (allowed: GETSTATIC, PUTSTATIC in <clinit>)");
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String descriptor, boolean isInterface) {
            if (opcode != Opcodes.INVOKESPECIAL) {
                throw drift("unexpected method-insn opcode " + opcode + " (only INVOKESPECIAL of super.<init> allowed)");
            }
            if (expectedSuper.equals(owner) == false) {
                throw drift("INVOKESPECIAL on foreign owner [" + owner + "] (expected super " + expectedSuper + ")");
            }
            if ("<init>".equals(name) == false) {
                throw drift("INVOKESPECIAL of method [" + name + "] (only <init> allowed)");
            }
            if (isInterface) {
                throw drift("INVOKESPECIAL on interface owner not allowed");
            }
            super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
        }

        @Override
        public void visitLdcInsn(Object cst) {
            if ("<clinit>".equals(mName) == false) {
                throw drift("LDC only allowed in <clinit>");
            }
            if ((cst instanceof ConstantDynamic) == false) {
                throw drift("LDC of " + cst.getClass().getSimpleName() + " (only ConstantDynamic allowed)");
            }
            super.visitLdcInsn(cst);
        }

        // ---- Whole opcode families we never emit: reject ----

        @Override
        public void visitIntInsn(int opcode, int operand) {
            throw drift("int-insn opcode " + opcode + " not allowed");
        }

        @Override
        public void visitTypeInsn(int opcode, String type) {
            throw drift("type-insn opcode " + opcode + " not allowed");
        }

        @Override
        public void visitJumpInsn(int opcode, Label label) {
            throw drift("jump-insn opcode " + opcode + " not allowed");
        }

        @Override
        public void visitIincInsn(int var, int increment) {
            throw drift("IINC not allowed");
        }

        @Override
        public void visitInvokeDynamicInsn(String n, String d, Handle b, Object... a) {
            throw drift("INVOKEDYNAMIC not allowed");
        }

        @Override
        public void visitMultiANewArrayInsn(String d, int dims) {
            throw drift("MULTIANEWARRAY not allowed");
        }

        @Override
        public void visitTableSwitchInsn(int min, int max, Label dflt, Label... labels) {
            throw drift("TABLESWITCH not allowed");
        }

        @Override
        public void visitLookupSwitchInsn(Label dflt, int[] keys, Label[] labels) {
            throw drift("LOOKUPSWITCH not allowed");
        }

        @Override
        public void visitTryCatchBlock(Label s, Label e, Label h, String t) {
            throw drift("try/catch block not allowed");
        }

        // ---- Method-level metadata we never emit: reject ----

        @Override
        public void visitParameter(String name, int access) {
            throw drift("explicit parameter metadata not allowed");
        }

        @Override
        public AnnotationVisitor visitAnnotationDefault() {
            throw drift("annotation default not allowed");
        }

        @Override
        public AnnotationVisitor visitAnnotation(String d, boolean v) {
            throw drift("method annotation not allowed");
        }

        @Override
        public AnnotationVisitor visitTypeAnnotation(int t, TypePath p, String d, boolean v) {
            throw drift("method type annotation not allowed");
        }

        @Override
        public void visitAnnotableParameterCount(int parameterCount, boolean v) {
            if (parameterCount != 0) throw drift("annotable parameter count not allowed");
        }

        @Override
        public AnnotationVisitor visitParameterAnnotation(int param, String d, boolean v) {
            throw drift("parameter annotation not allowed");
        }

        @Override
        public void visitAttribute(Attribute attribute) {
            throw drift("method attribute [" + attribute.type + "] not allowed");
        }

        @Override
        public AnnotationVisitor visitInsnAnnotation(int tr, TypePath tp, String d, boolean v) {
            throw drift("insn annotation not allowed");
        }

        @Override
        public AnnotationVisitor visitTryCatchAnnotation(int tr, TypePath tp, String d, boolean v) {
            throw drift("try/catch annotation not allowed");
        }

        @Override
        public void visitLocalVariable(String n, String d, String s, Label start, Label end, int index) {
            throw drift("local variable debug info not allowed");
        }

        @Override
        public AnnotationVisitor visitLocalVariableAnnotation(int tr, TypePath tp, Label[] s, Label[] e, int[] idx, String d, boolean v) {
            throw drift("local variable annotation not allowed");
        }

        @Override
        public void visitLineNumber(int line, Label start) {
            throw drift("line number debug info not allowed");
        }

        // visitCode / visitLabel / visitFrame / visitMaxs / visitEnd pass through unchanged
        // (visitFrame is suppressed by COMPUTE_FRAMES on the underlying ClassWriter anyway).

        private AssertionError drift(String what) {
            return new AssertionError(
                "JitConstantSpinner shape drift in spun [" + spec.spunInternalName + "] method [" + mName + "]: " + what
            );
        }
    }

    // Used only by tests that build a SpunClassSpec from a base class to compute expected ctor count.
    static int countPublicCtors(Class<?> base) {
        int n = 0;
        for (var c : base.getConstructors()) {
            if (Modifier.isPrivate(c.getModifiers()) == false) n++;
        }
        return n;
    }
}
