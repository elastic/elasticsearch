/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.processor.model.ArrayFieldModel;
import org.elasticsearch.foreign.processor.model.LibraryModel;
import org.elasticsearch.foreign.processor.model.NativeType;
import org.elasticsearch.foreign.processor.model.ScalarFieldModel;
import org.elasticsearch.foreign.processor.model.StructFieldModel;
import org.elasticsearch.foreign.processor.model.StructInterfaceModel;
import org.elasticsearch.foreign.processor.model.StructModel;

import java.lang.classfile.ClassBuilder;
import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.AccessFlag;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.processing.Filer;
import javax.lang.model.element.TypeElement;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Addressable;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Arena;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayoutPathElement;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegment;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegmentAdapter;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Object;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_StructLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_VarHandle;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_void;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_Arena_ofAuto;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_allocate_layout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_asSlice;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_byteSize;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_groupElement;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_structLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_varHandleWithoutOffset;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.fieldClassDesc;
import static org.elasticsearch.foreign.processor.StructLayoutUtil.LayoutField;
import static org.elasticsearch.foreign.processor.StructLayoutUtil.computeLayout;
import static org.elasticsearch.foreign.processor.StructLayoutUtil.emitStructLayoutArray;

/**
 * Generates the {@code $Impl} class for a {@code @StructSpecification} interface. The class
 * implements the interface and {@link org.elasticsearch.foreign.Addressable}, wraps a native
 * {@link java.lang.foreign.MemorySegment}, and exposes VarHandle-backed accessors for scalar
 * fields plus indexed accessors for {@code @ArrayField} methods.
 */
final class StructImplWriter {

    /** Name of the static VarHandle field generated for {@code field} on a struct {@code $Impl}. */
    static String varHandleFieldName(StructFieldModel field) {
        return switch (field) {
            case ScalarFieldModel scalar -> scalar.name() + "$vh";
            case ArrayFieldModel array -> array.name() + "$ptr$vh";
        };
    }

    private final Filer filer;
    private final int classFileVersion;

    StructImplWriter(Filer filer, int classFileVersion) {
        this.filer = filer;
        this.classFileVersion = classFileVersion;
    }

    /** Generates and writes the {@code $Impl} class for an interface struct. */
    void generate(LibraryModel model, StructInterfaceModel struct, TypeElement sourceElement) throws Exception {
        String prefix = qualifiedPrefix(model);
        String structImplQualifiedName = prefix + "$" + struct.simpleName() + "$Impl";
        ClassDesc structImplDesc = ClassDesc.of(structImplQualifiedName);
        ClassDesc structInterfaceDesc = ClassDesc.of(prefix + "$" + struct.simpleName());

        List<StructFieldModel> fields = struct.fields();
        List<LayoutField> layout = computeLayout(fields);

        byte[] classBytes = ClassFile.of().build(structImplDesc, cb -> {
            cb.withVersion(classFileVersion, 0);
            cb.withFlags(AccessFlag.FINAL, AccessFlag.SUPER);
            cb.withSuperclass(CD_Object);
            cb.withInterfaceSymbols(structInterfaceDesc, CD_Addressable);

            declareFields(cb, fields);
            emitClinit(cb, structImplDesc, fields, layout);
            emitConstructor(cb, structImplDesc);
            emitSegmentAccessor(cb, structImplDesc);
            emitFieldAccessors(cb, structImplDesc, model, prefix, fields);
        });

        try (var os = filer.createClassFile(structImplQualifiedName, sourceElement).openOutputStream()) {
            os.write(classBytes);
        }
    }

    /** Declares {@code LAYOUT}, one VarHandle per field, and the instance {@code segment} field. */
    private static void declareFields(ClassBuilder cb, List<StructFieldModel> fields) {
        cb.withField("LAYOUT", CD_StructLayout, fb -> fb.withFlags(AccessFlag.STATIC, AccessFlag.FINAL));
        for (StructFieldModel field : fields) {
            cb.withField(varHandleFieldName(field), CD_VarHandle, fb -> fb.withFlags(AccessFlag.STATIC, AccessFlag.FINAL));
        }
        cb.withField("segment", CD_MemorySegment, fb -> fb.withFlags(AccessFlag.FINAL));
    }

    /** Emits {@code <clinit>}: initialize {@code LAYOUT} and every VarHandle. */
    private static void emitClinit(ClassBuilder cb, ClassDesc structImplDesc, List<StructFieldModel> fields, List<LayoutField> layout) {
        cb.withMethodBody("<clinit>", MethodTypeDesc.of(CD_void), ClassFile.ACC_STATIC, clinit -> {
            emitStructLayoutArray(clinit, layout);
            clinit.invokestatic(CD_MemoryLayout, "structLayout", MTD_structLayout, true);
            clinit.putstatic(structImplDesc, "LAYOUT", CD_StructLayout);

            for (StructFieldModel field : fields) {
                clinit.getstatic(structImplDesc, "LAYOUT", CD_StructLayout);
                clinit.ldc(field.name());
                clinit.invokestatic(CD_MemoryLayoutPathElement, "groupElement", MTD_groupElement, true);
                clinit.invokestatic(CD_MemorySegmentAdapter, "varHandleWithoutOffset", MTD_varHandleWithoutOffset);
                clinit.putstatic(structImplDesc, varHandleFieldName(field), CD_VarHandle);
            }
            clinit.return_();
        });
    }

    /** Package-private no-arg constructor: {@code this.segment = Arena.ofAuto().allocate(LAYOUT);}. */
    private static void emitConstructor(ClassBuilder cb, ClassDesc structImplDesc) {
        cb.withMethodBody("<init>", MethodTypeDesc.of(CD_void), 0, init -> {
            init.aload(0);
            init.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
            init.aload(0);
            init.invokestatic(CD_Arena, "ofAuto", MTD_Arena_ofAuto, true);
            init.getstatic(structImplDesc, "LAYOUT", CD_StructLayout);
            init.invokeinterface(CD_Arena, "allocate", MTD_allocate_layout);
            init.putfield(structImplDesc, "segment", CD_MemorySegment);
            init.return_();
        });
    }

    /** Implements {@link org.elasticsearch.foreign.Addressable#segment()}: returns the instance field. */
    private static void emitSegmentAccessor(ClassBuilder cb, ClassDesc structImplDesc) {
        cb.withMethodBody("segment", MethodTypeDesc.of(CD_MemorySegment), ClassFile.ACC_PUBLIC, seg -> {
            seg.aload(0);
            seg.getfield(structImplDesc, "segment", CD_MemorySegment);
            seg.areturn();
        });
    }

    /** Emits one accessor method per declared field (scalar getter or {@code @ArrayField} indexed getter). */
    private static void emitFieldAccessors(
        ClassBuilder cb,
        ClassDesc structImplDesc,
        LibraryModel model,
        String packPrefix,
        List<StructFieldModel> fields
    ) {
        for (StructFieldModel field : fields) {
            switch (field) {
                case ScalarFieldModel scalar -> emitScalarFieldGetter(cb, structImplDesc, scalar);
                case ArrayFieldModel array -> {
                    List<StructFieldModel> elementFields = resolveElementFields(model, array.elementSimpleName());
                    emitArrayFieldGetter(cb, structImplDesc, packPrefix, array, elementFields);
                }
            }
        }
    }

    /** Emits a scalar-field getter: {@code return (<type>) name$vh.get(segment);}. */
    private static void emitScalarFieldGetter(ClassBuilder cb, ClassDesc structImplDesc, ScalarFieldModel field) {
        ClassDesc returnDesc = fieldClassDesc(field.type());
        MethodTypeDesc methodDesc = MethodTypeDesc.of(returnDesc);
        cb.withMethodBody(field.name(), methodDesc, ClassFile.ACC_PUBLIC, code -> {
            code.getstatic(structImplDesc, varHandleFieldName(field), CD_VarHandle);
            code.aload(0);
            code.getfield(structImplDesc, "segment", CD_MemorySegment);
            code.invokevirtual(CD_VarHandle, "get", MethodTypeDesc.of(returnDesc, CD_MemorySegment));
            emitTypedReturnScalar(code, field.type());
        });
    }

    /**
     * Emits an {@code @ArrayField} indexed getter. Loads the pointer via {@code name$ptr$vh},
     * slices to the requested element, reads each of the element record's fields via the
     * element's {@code $Pack} VarHandles, then constructs and returns the record.
     */
    private static void emitArrayFieldGetter(
        ClassBuilder cb,
        ClassDesc structImplDesc,
        String packPrefix,
        ArrayFieldModel arrayField,
        List<StructFieldModel> elementFields
    ) {
        ClassDesc elementRecordDesc = ClassDesc.of(packPrefix + "$" + arrayField.elementSimpleName());
        ClassDesc elementPackDesc = ClassDesc.of(packPrefix + "$" + arrayField.elementSimpleName() + "$Pack");
        MethodTypeDesc methodDesc = MethodTypeDesc.of(elementRecordDesc, ClassDesc.ofDescriptor("I"));

        cb.withMethodBody(arrayField.name(), methodDesc, ClassFile.ACC_PUBLIC, code -> {
            // slot 0 = this, slot 1 = index; slot 2 = ptr, slots 3-4 = elementSize, slot 5 = elementSeg
            emitLoadPointer(code, structImplDesc, arrayField);
            emitLoadElementSize(code, elementPackDesc);
            emitSliceElement(code);
            emitConstructElementRecord(code, elementRecordDesc, elementPackDesc, elementFields);
            code.areturn();
        });
    }

    /** Loads the array pointer from {@code name$ptr$vh.get(segment)} into slot 2. */
    private static void emitLoadPointer(CodeBuilder code, ClassDesc structImplDesc, ArrayFieldModel arrayField) {
        code.getstatic(structImplDesc, varHandleFieldName(arrayField), CD_VarHandle);
        code.aload(0);
        code.getfield(structImplDesc, "segment", CD_MemorySegment);
        code.invokevirtual(CD_VarHandle, "get", MethodTypeDesc.of(CD_MemorySegment, CD_MemorySegment));
        code.astore(2);
    }

    /** Loads {@code ElementPack.LAYOUT.byteSize()} into slots 3-4 (long). */
    private static void emitLoadElementSize(CodeBuilder code, ClassDesc elementPackDesc) {
        code.getstatic(elementPackDesc, "LAYOUT", CD_StructLayout);
        code.invokeinterface(CD_MemoryLayout, "byteSize", MTD_byteSize);
        code.lstore(3);
    }

    /** Computes {@code ptr.asSlice(index * elementSize, elementSize)} and stores in slot 5. */
    private static void emitSliceElement(CodeBuilder code) {
        code.aload(2); // ptr
        code.iload(1); // index
        code.i2l();
        code.lload(3); // elementSize
        code.lmul();
        code.lload(3); // elementSize
        code.invokeinterface(CD_MemorySegment, "asSlice", MTD_asSlice);
        code.astore(5);
    }

    /**
     * Emits {@code new ElementRecord(f0, f1, ...)} where each fN is read from
     * {@code ElementPack.<name>$vh.get(elementSeg)}. Leaves the constructed record on the stack.
     */
    private static void emitConstructElementRecord(
        CodeBuilder code,
        ClassDesc elementRecordDesc,
        ClassDesc elementPackDesc,
        List<StructFieldModel> elementFields
    ) {
        code.new_(elementRecordDesc);
        code.dup();
        List<ClassDesc> ctorParams = new ArrayList<>();
        for (StructFieldModel ef : elementFields) {
            ClassDesc efDesc = fieldClassDesc(ef.type());
            code.getstatic(elementPackDesc, ef.name() + "$vh", CD_VarHandle);
            code.aload(5); // elementSeg
            code.invokevirtual(CD_VarHandle, "get", MethodTypeDesc.of(efDesc, CD_MemorySegment));
            ctorParams.add(efDesc);
        }
        code.invokespecial(elementRecordDesc, "<init>", MethodTypeDesc.of(CD_void, ctorParams));
    }

    /** Emits the appropriate typed return instruction for a scalar field. */
    private static void emitTypedReturnScalar(CodeBuilder cb, NativeType type) {
        switch (type) {
            case INT, SHORT, BYTE, BOOLEAN -> cb.ireturn();
            case LONG -> cb.lreturn();
            case FLOAT -> cb.freturn();
            case DOUBLE -> cb.dreturn();
            case ADDRESS -> cb.areturn();
            case VOID, STRING, ADDRESSABLE -> throw new AssertionError("unexpected scalar field type: " + type);
        }
    }

    /** Looks up the field list of a nested struct in the same library by simple name. */
    private static List<StructFieldModel> resolveElementFields(LibraryModel model, String simpleName) {
        for (StructModel s : model.structs()) {
            if (s.simpleName().equals(simpleName)) {
                return s.fields();
            }
        }
        throw new AssertionError("no struct model for element type: " + simpleName);
    }

    private static String qualifiedPrefix(LibraryModel model) {
        return model.packageName().isEmpty() ? model.simpleName() : model.packageName() + "." + model.simpleName();
    }
}
