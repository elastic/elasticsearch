/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.processor.model.LibraryModel;
import org.elasticsearch.foreign.processor.model.StructFieldModel;
import org.elasticsearch.foreign.processor.model.StructRecordModel;

import java.lang.classfile.ClassBuilder;
import java.lang.classfile.ClassFile;
import java.lang.classfile.CodeBuilder;
import java.lang.constant.ClassDesc;
import java.lang.constant.MethodTypeDesc;
import java.lang.reflect.AccessFlag;
import java.util.List;

import javax.annotation.processing.Filer;
import javax.lang.model.element.TypeElement;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayoutPathElement;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegment;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemorySegmentAdapter;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_Object;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_StructLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_VarHandle;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_long;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_void;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_groupElement;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_structLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_varHandleWithoutOffset;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.emitValueLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.fieldClassDesc;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.valueLayoutClassDesc;
import static org.elasticsearch.foreign.processor.StructLayoutUtil.LayoutField;
import static org.elasticsearch.foreign.processor.StructLayoutUtil.computeLayout;
import static org.elasticsearch.foreign.processor.StructLayoutUtil.emitStructLayoutArray;

/**
 * Generates the {@code $Pack} companion class for a {@code @StructSpecification} record. The
 * class exposes:
 * <ul>
 *   <li>a package-private {@code static final StructLayout LAYOUT}</li>
 *   <li>one {@code private static final long <name>$offset} per record component</li>
 *   <li>one package-private {@code static final VarHandle <name>$vh} per record component (used
 *       by struct {@code $Impl} readers)</li>
 *   <li>a package-private {@code static void pack(RecordType src, MemorySegment dest, long baseOffset)} method</li>
 * </ul>
 */
final class StructPackWriter {

    private static final MethodTypeDesc MTD_byteOffset = MethodTypeDesc.of(
        CD_long,
        ClassDesc.ofDescriptor("[Ljava/lang/foreign/MemoryLayout$PathElement;")
    );

    private final Filer filer;
    private final int classFileVersion;

    StructPackWriter(Filer filer, int classFileVersion) {
        this.filer = filer;
        this.classFileVersion = classFileVersion;
    }

    /** Generates and writes the {@code $Pack} class for a record struct. */
    void generate(LibraryModel model, StructRecordModel struct, TypeElement sourceElement) throws Exception {
        String packQualifiedName = qualified(model, struct.simpleName()) + "$Pack";
        ClassDesc packDesc = ClassDesc.of(packQualifiedName);
        ClassDesc recordDesc = ClassDesc.of(qualified(model, struct.simpleName()));
        List<StructFieldModel> fields = struct.fields();
        List<LayoutField> layout = computeLayout(fields);

        byte[] classBytes = ClassFile.of().build(packDesc, cb -> {
            cb.withVersion(classFileVersion, 0);
            cb.withFlags(AccessFlag.FINAL, AccessFlag.SUPER);
            cb.withSuperclass(CD_Object);

            declareFields(cb, fields);
            emitClinit(cb, packDesc, fields, layout);
            emitPrivateConstructor(cb);
            emitPackMethod(cb, packDesc, recordDesc, fields);
        });

        try (var os = filer.createClassFile(packQualifiedName, sourceElement).openOutputStream()) {
            os.write(classBytes);
        }
    }

    /** Declares LAYOUT, one offset field per component, and one VarHandle field per component. */
    private static void declareFields(ClassBuilder cb, List<StructFieldModel> fields) {
        cb.withField("LAYOUT", CD_StructLayout, fb -> fb.withFlags(AccessFlag.STATIC, AccessFlag.FINAL));
        for (StructFieldModel field : fields) {
            cb.withField(field.name() + "$offset", CD_long, fb -> fb.withFlags(AccessFlag.PRIVATE, AccessFlag.STATIC, AccessFlag.FINAL));
        }
        for (StructFieldModel field : fields) {
            cb.withField(field.name() + "$vh", CD_VarHandle, fb -> fb.withFlags(AccessFlag.STATIC, AccessFlag.FINAL));
        }
    }

    /** Emits {@code <clinit>}: initialize LAYOUT, all offsets, and all VarHandles. */
    private static void emitClinit(ClassBuilder cb, ClassDesc packDesc, List<StructFieldModel> fields, List<LayoutField> layout) {
        cb.withMethodBody("<clinit>", MethodTypeDesc.of(CD_void), ClassFile.ACC_STATIC, clinit -> {
            emitStructLayoutArray(clinit, layout);
            clinit.invokestatic(CD_MemoryLayout, "structLayout", MTD_structLayout, true);
            clinit.putstatic(packDesc, "LAYOUT", CD_StructLayout);

            for (StructFieldModel field : fields) {
                emitOffsetInit(clinit, packDesc, field);
            }
            for (StructFieldModel field : fields) {
                emitVarHandleInit(clinit, packDesc, field);
            }
            clinit.return_();
        });
    }

    /** Initializes {@code <name>$offset} to {@code LAYOUT.byteOffset(PathElement.groupElement(name))}. */
    private static void emitOffsetInit(CodeBuilder clinit, ClassDesc packDesc, StructFieldModel field) {
        clinit.getstatic(packDesc, "LAYOUT", CD_StructLayout);
        clinit.loadConstant(1);
        clinit.anewarray(CD_MemoryLayoutPathElement);
        clinit.dup();
        clinit.loadConstant(0);
        clinit.ldc(field.name());
        clinit.invokestatic(CD_MemoryLayoutPathElement, "groupElement", MTD_groupElement, true);
        clinit.aastore();
        clinit.invokeinterface(CD_MemoryLayout, "byteOffset", MTD_byteOffset);
        clinit.putstatic(packDesc, field.name() + "$offset", CD_long);
    }

    /** Initializes {@code <name>$vh} to {@code MemorySegmentAdapter.varHandleWithoutOffset(LAYOUT, groupElement(name))}. */
    private static void emitVarHandleInit(CodeBuilder clinit, ClassDesc packDesc, StructFieldModel field) {
        clinit.getstatic(packDesc, "LAYOUT", CD_StructLayout);
        clinit.ldc(field.name());
        clinit.invokestatic(CD_MemoryLayoutPathElement, "groupElement", MTD_groupElement, true);
        clinit.invokestatic(CD_MemorySegmentAdapter, "varHandleWithoutOffset", MTD_varHandleWithoutOffset);
        clinit.putstatic(packDesc, field.name() + "$vh", CD_VarHandle);
    }

    private static void emitPrivateConstructor(ClassBuilder cb) {
        cb.withMethodBody("<init>", MethodTypeDesc.of(CD_void), ClassFile.ACC_PRIVATE, init -> {
            init.aload(0);
            init.invokespecial(CD_Object, "<init>", MethodTypeDesc.of(CD_void));
            init.return_();
        });
    }

    /**
     * Emits {@code static void pack(RecordType src, MemorySegment dest, long baseOffset)}. For
     * each component the body reads the record accessor and writes the value into {@code dest} at
     * {@code baseOffset + <name>$offset} using {@code MemorySegment.set(ValueLayout, long, value)}.
     */
    private static void emitPackMethod(ClassBuilder cb, ClassDesc packDesc, ClassDesc recordDesc, List<StructFieldModel> fields) {
        MethodTypeDesc packMethodDesc = MethodTypeDesc.of(CD_void, List.of(recordDesc, CD_MemorySegment, CD_long));
        cb.withMethodBody("pack", packMethodDesc, ClassFile.ACC_STATIC, pack -> {
            // slot 0 = src, slot 1 = dest, slot 2 = baseOffset (long, takes 2 slots)
            for (StructFieldModel field : fields) {
                emitPackField(pack, packDesc, recordDesc, field);
            }
            pack.return_();
        });
    }

    /** Emits one field of the {@code pack} body: {@code dest.set(VL, baseOffset + $offset, src.name());}. */
    private static void emitPackField(CodeBuilder pack, ClassDesc packDesc, ClassDesc recordDesc, StructFieldModel field) {
        pack.aload(1); // dest
        emitValueLayout(pack, field.type().layoutType());
        pack.lload(2); // baseOffset
        pack.getstatic(packDesc, field.name() + "$offset", CD_long);
        pack.ladd();
        pack.aload(0); // src
        pack.invokevirtual(recordDesc, field.name(), MethodTypeDesc.of(fieldClassDesc(field.type())));

        ClassDesc valueLayoutDesc = valueLayoutClassDesc(field.type());
        ClassDesc fieldJavaDesc = fieldClassDesc(field.type());
        MethodTypeDesc setDesc = MethodTypeDesc.of(CD_void, valueLayoutDesc, CD_long, fieldJavaDesc);
        pack.invokeinterface(CD_MemorySegment, "set", setDesc);
    }

    private static String qualified(LibraryModel model, String structSimpleName) {
        return model.packageName().isEmpty()
            ? model.simpleName() + "$" + structSimpleName
            : model.packageName() + "." + model.simpleName() + "$" + structSimpleName;
    }
}
