/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.processor.model.FieldModel;
import org.elasticsearch.foreign.processor.model.LibraryModel;
import org.elasticsearch.foreign.processor.model.NativeType;
import org.elasticsearch.foreign.processor.model.StructModel;

import java.lang.classfile.ClassFile;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.AccessFlag;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.processing.Filer;
import javax.lang.model.element.TypeElement;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Arena;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayoutArray;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayoutPathElement;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegment;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Object;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_String;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_StructLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_VarHandle;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_long;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_void;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.emitValueLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.fieldClassDesc;

/**
 * Generates {@code <StructName>$Impl} class files for {@code @Struct}-annotated nested interfaces.
 *
 * <p>Two layouts are supported:
 * <ul>
 *   <li><b>Static</b>: a {@code LAYOUT} StructLayout and per-field VarHandles are built at
 *       class-init time; the no-arg constructor allocates via {@code Arena.ofAuto().allocate(LAYOUT)}.</li>
 *   <li><b>Dynamic</b>: no LAYOUT/VarHandle fields; the constructor takes
 *       {@code (long sizeof, long off0, ...)} and getters/setters use
 *       {@code segment.get/set(ValueLayout, offset)}.</li>
 * </ul>
 */
class StructClassWriter {

    private final Filer filer;

    StructClassWriter(Filer filer) {
        this.filer = filer;
    }

    /**
     * Generates and writes the {@code <StructName>$Impl} class for the given struct model.
     */
    void generate(LibraryModel libraryModel, StructModel struct, TypeElement sourceElement) throws Exception {
        String innerName = struct.simpleName() + "$Impl";
        String outerPrefix = libraryModel.packageName().isEmpty()
            ? libraryModel.simpleName()
            : libraryModel.packageName() + "." + libraryModel.simpleName();
        String generatedName = outerPrefix + "$" + innerName;

        ClassDesc generatedDesc = ClassDesc.of(generatedName);
        ClassDesc interfaceDesc = ClassDesc.of(outerPrefix + "$" + struct.simpleName());

        List<FieldModel> fields = struct.fields();

        byte[] classBytes = ClassFile.of().build(generatedDesc, cb -> {
            cb.withVersion(ClassFile.JAVA_21_VERSION, 0);
            cb.withFlags(AccessFlag.FINAL, AccessFlag.SUPER);
            cb.withSuperclass(CD_Object);
            cb.withInterfaceSymbols(interfaceDesc);

            // package-private final — outer factory methods access this directly
            cb.withField("segment", CD_MemorySegment, fb -> fb.withFlags(AccessFlag.FINAL));

            if (!struct.isDynamic()) {
                generateStaticStructBody(cb, generatedDesc, fields);
            } else {
                generateDynamicStructBody(cb, generatedDesc, fields);
            }
        });

        try (var os = filer.createClassFile(generatedName, sourceElement).openOutputStream()) {
            os.write(classBytes);
        }
    }

    // -------------------------------------------------------------------------
    // Static layout body
    // -------------------------------------------------------------------------

    /**
     * Generates a static-layout struct $Impl. The layout and per-field VarHandles are built once
     * at class load time. The no-arg constructor allocates a fresh backing segment on every call.
     */
    private static void generateStaticStructBody(java.lang.classfile.ClassBuilder cb, ClassDesc generatedDesc, List<FieldModel> fields) {
        cb.withField("LAYOUT", CD_StructLayout, fb -> fb.withFlags(AccessFlag.PUBLIC, AccessFlag.STATIC, AccessFlag.FINAL));

        for (FieldModel field : fields) {
            cb.withField(field.name() + "$vh", CD_VarHandle, fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL));
        }

        cb.withMethodBody("<clinit>", MethodTypeDesc.of(CD_void), ClassFile.ACC_STATIC, clinit -> {
            emitStaticLayoutInit(clinit, generatedDesc, fields);
            clinit.return_();
        });

        cb.withMethodBody("<init>", MethodTypeDesc.of(CD_void), 0, init -> {
            init.aload(0);
            init.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
            init.aload(0);
            init.invokestatic(CD_Arena, "ofAuto", MethodTypeDesc.of(CD_Arena), true);
            init.getstatic(generatedDesc, "LAYOUT", CD_StructLayout);
            init.invokeinterface(CD_Arena, "allocate", MethodTypeDesc.of(CD_MemorySegment, CD_MemoryLayout));
            init.putfield(generatedDesc, "segment", CD_MemorySegment);
            init.return_();
        });

        for (FieldModel field : fields) {
            emitStaticStructGetter(cb, generatedDesc, field);
            if (field.hasSetter()) {
                emitStaticStructSetter(cb, generatedDesc, field);
            }
        }
    }

    /**
     * Builds the StructLayout and per-field VarHandles at class load time. Fields are named in the
     * layout so each VarHandle can target its field by name. Padding entries are inserted before
     * any field annotated with {@code @Padding}.
     */
    private static void emitStaticLayoutInit(java.lang.classfile.CodeBuilder cb, ClassDesc generatedDesc, List<FieldModel> fields) {
        int elementCount = 0;
        for (FieldModel field : fields) {
            if (field.paddingBefore() > 0) {
                elementCount++;
            }
            elementCount++;
        }

        cb.loadConstant(elementCount);
        cb.anewarray(CD_MemoryLayout);
        int arrayIdx = 0;
        for (FieldModel field : fields) {
            if (field.paddingBefore() > 0) {
                cb.dup();
                cb.loadConstant(arrayIdx);
                cb.loadConstant((long) field.paddingBefore());
                cb.invokestatic(CD_MemoryLayout, "paddingLayout", MethodTypeDesc.of(CD_MemoryLayout, CD_long), true);
                cb.aastore();
                arrayIdx++;
            }
            cb.dup();
            cb.loadConstant(arrayIdx);
            emitValueLayout(cb, layoutTypeForField(field));
            cb.ldc(field.name());
            cb.invokeinterface(CD_MemoryLayout, "withName", MethodTypeDesc.of(CD_MemoryLayout, CD_String));
            cb.aastore();
            arrayIdx++;
        }

        cb.invokestatic(CD_MemoryLayout, "structLayout", MethodTypeDesc.of(CD_StructLayout, CD_MemoryLayoutArray), true);
        cb.putstatic(generatedDesc, "LAYOUT", CD_StructLayout);

        ClassDesc CD_MemSegUtil = ClassDesc.of("org.elasticsearch.foreign.MemorySegmentUtil");
        for (FieldModel field : fields) {
            cb.getstatic(generatedDesc, "LAYOUT", CD_StructLayout);
            cb.ldc(field.name());
            cb.invokestatic(CD_MemoryLayoutPathElement, "groupElement", MethodTypeDesc.of(CD_MemoryLayoutPathElement, CD_String), true);
            cb.invokestatic(
                CD_MemSegUtil,
                "varHandleWithoutOffset",
                MethodTypeDesc.of(CD_VarHandle, CD_MemoryLayout, CD_MemoryLayoutPathElement)
            );
            cb.putstatic(generatedDesc, field.name() + "$vh", CD_VarHandle);
        }
    }

    /**
     * Reads a field value via the pre-built VarHandle. VarHandle.get is signature-polymorphic;
     * after {@code varHandleWithoutOffset} the handle takes only a MemorySegment (no offset
     * argument), so the call descriptor must match that shape exactly.
     */
    private static void emitStaticStructGetter(java.lang.classfile.ClassBuilder cb, ClassDesc generatedDesc, FieldModel field) {
        ClassDesc returnDesc = fieldClassDesc(field.nativeType());
        cb.withMethodBody(field.name(), MethodTypeDesc.of(returnDesc), ClassFile.ACC_PUBLIC, code -> {
            code.getstatic(generatedDesc, field.name() + "$vh", CD_VarHandle);
            code.aload(0);
            code.getfield(generatedDesc, "segment", CD_MemorySegment);
            // VarHandle.get is signature-polymorphic: descriptor is (MemorySegment)<ReturnType>
            code.invokevirtual(CD_VarHandle, "get", MethodTypeDesc.of(returnDesc, CD_MemorySegment));
            emitFieldReturn(code, field.nativeType());
        });
    }

    /**
     * Writes a field value via the pre-built VarHandle. See {@link #emitStaticStructGetter} for
     * the signature-polymorphism constraint on the call descriptor.
     */
    private static void emitStaticStructSetter(java.lang.classfile.ClassBuilder cb, ClassDesc generatedDesc, FieldModel field) {
        ClassDesc paramDesc = fieldClassDesc(field.nativeType());
        cb.withMethodBody(field.name(), MethodTypeDesc.of(CD_void, paramDesc), ClassFile.ACC_PUBLIC, code -> {
            code.getstatic(generatedDesc, field.name() + "$vh", CD_VarHandle);
            code.aload(0);
            code.getfield(generatedDesc, "segment", CD_MemorySegment);
            // Load the value (slot 1; long uses slots 1-2)
            emitFieldLoad(code, field.nativeType(), 1);
            // VarHandle.set is signature-polymorphic: descriptor is (MemorySegment, ValueType)V
            code.invokevirtual(CD_VarHandle, "set", MethodTypeDesc.of(CD_void, CD_MemorySegment, paramDesc));
            code.return_();
        });
    }

    // -------------------------------------------------------------------------
    // Dynamic layout body
    // -------------------------------------------------------------------------

    /**
     * Generates a dynamic-layout struct $Impl. The constructor accepts the struct's byte size and
     * per-field offsets, which are stored as instance fields. Getters and setters address memory
     * relative to those stored offsets rather than a compile-time-fixed layout.
     */
    private static void generateDynamicStructBody(java.lang.classfile.ClassBuilder cb, ClassDesc generatedDesc, List<FieldModel> fields) {
        for (FieldModel field : fields) {
            cb.withField(field.name() + "$offset", CD_long, fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.FINAL));
        }

        List<ClassDesc> ctorParams = new ArrayList<>();
        ctorParams.add(CD_long); // sizeof
        for (int i = 0; i < fields.size(); i++) {
            ctorParams.add(CD_long);
        }
        MethodTypeDesc ctorDesc = MethodTypeDesc.of(CD_void, ctorParams);

        cb.withMethodBody("<init>", ctorDesc, 0, init -> {
            init.aload(0);
            init.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));

            // this.segment = Arena.ofAuto().allocate(sizeof, 8)
            init.aload(0);
            init.invokestatic(CD_Arena, "ofAuto", MethodTypeDesc.of(CD_Arena), true);
            init.lload(1); // sizeof (first long param at slot 1)
            init.ldc(8L);  // alignment
            init.invokeinterface(CD_Arena, "allocate", MethodTypeDesc.of(CD_MemorySegment, CD_long, CD_long));
            init.putfield(generatedDesc, "segment", CD_MemorySegment);

            // Store each offset: slot 3, 5, 7, ... (each long takes 2 slots; sizeof at 1, offsets at 3,5,...)
            int slot = 3;
            for (FieldModel field : fields) {
                init.aload(0);
                init.lload(slot);
                init.putfield(generatedDesc, field.name() + "$offset", CD_long);
                slot += 2;
            }

            init.return_();
        });

        for (FieldModel field : fields) {
            emitDynamicStructGetter(cb, generatedDesc, field);
            if (field.hasSetter()) {
                emitDynamicStructSetter(cb, generatedDesc, field);
            }
        }
    }

    private static void emitDynamicStructGetter(java.lang.classfile.ClassBuilder cb, ClassDesc generatedDesc, FieldModel field) {
        ClassDesc returnDesc = fieldClassDesc(field.nativeType());
        cb.withMethodBody(field.name(), MethodTypeDesc.of(returnDesc), ClassFile.ACC_PUBLIC, code -> {
            code.aload(0);
            code.getfield(generatedDesc, "segment", CD_MemorySegment);
            emitValueLayout(code, layoutTypeForField(field));
            code.aload(0);
            code.getfield(generatedDesc, field.name() + "$offset", CD_long);
            // MemorySegment.get(ValueLayout.OfXxx, long)
            ClassDesc vlDesc = valueLayoutDesc(field.nativeType());
            code.invokeinterface(CD_MemorySegment, "get", MethodTypeDesc.of(returnDesc, vlDesc, CD_long));
            emitFieldReturn(code, field.nativeType());
        });
    }

    private static void emitDynamicStructSetter(java.lang.classfile.ClassBuilder cb, ClassDesc generatedDesc, FieldModel field) {
        ClassDesc paramDesc = fieldClassDesc(field.nativeType());
        cb.withMethodBody(field.name(), MethodTypeDesc.of(CD_void, paramDesc), ClassFile.ACC_PUBLIC, code -> {
            code.aload(0);
            code.getfield(generatedDesc, "segment", CD_MemorySegment);
            emitValueLayout(code, layoutTypeForField(field));
            code.aload(0);
            code.getfield(generatedDesc, field.name() + "$offset", CD_long);
            emitFieldLoad(code, field.nativeType(), 1);
            ClassDesc vlDesc = valueLayoutDesc(field.nativeType());
            ClassDesc elemDesc = fieldClassDesc(field.nativeType());
            code.invokeinterface(CD_MemorySegment, "set", MethodTypeDesc.of(CD_void, vlDesc, CD_long, elemDesc));
            code.return_();
        });
    }

    // -------------------------------------------------------------------------
    // Struct-specific helpers
    // -------------------------------------------------------------------------

    /** String, address, and array fields all occupy an address-sized slot in the native layout. */
    private static NativeType layoutTypeForField(FieldModel field) {
        return switch (field.nativeType()) {
            case ADDRESS, STRING -> NativeType.ADDRESS;
            case VOID -> throw new AssertionError("void cannot be a struct field type");
            default -> field.nativeType();
        };
    }

    private static ClassDesc valueLayoutDesc(NativeType type) {
        return switch (type) {
            case INT -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfInt;");
            case LONG -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfLong;");
            case SHORT -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfShort;");
            case BYTE -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfByte;");
            case BOOLEAN -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfBoolean;");
            case FLOAT -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfFloat;");
            case DOUBLE -> ClassDesc.ofDescriptor("Ljava/lang/foreign/ValueLayout$OfDouble;");
            case ADDRESS, STRING -> ClassDesc.ofDescriptor("Ljava/lang/foreign/AddressLayout;");
            case ARENA -> throw new AssertionError("ARENA cannot be a struct field type");
            case VOID -> throw new AssertionError("void cannot be a struct field type");
        };
    }

    private static void emitFieldLoad(java.lang.classfile.CodeBuilder cb, NativeType type, int slot) {
        switch (type) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.iload(slot);
            case LONG -> cb.lload(slot);
            case FLOAT -> cb.fload(slot);
            case DOUBLE -> cb.dload(slot);
            case ADDRESS, STRING -> cb.aload(slot);
            case ARENA -> throw new AssertionError("ARENA cannot be a struct field type");
            case VOID -> throw new AssertionError("void cannot be a struct field type");
        }
    }

    private static void emitFieldReturn(java.lang.classfile.CodeBuilder cb, NativeType type) {
        switch (type) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.ireturn();
            case LONG -> cb.lreturn();
            case FLOAT -> cb.freturn();
            case DOUBLE -> cb.dreturn();
            case ADDRESS, STRING -> cb.areturn();
            case ARENA -> throw new AssertionError("ARENA cannot be a struct field type");
            case VOID -> throw new AssertionError("void cannot be a struct field type");
        }
    }
}
