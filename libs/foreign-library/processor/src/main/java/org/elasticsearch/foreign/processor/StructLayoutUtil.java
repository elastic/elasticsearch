/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.processor.model.InlineArrayFieldModel;
import org.elasticsearch.foreign.processor.model.InlineStringFieldModel;
import org.elasticsearch.foreign.processor.model.NativeType;
import org.elasticsearch.foreign.processor.model.StructFieldModel;
import org.elasticsearch.foreign.processor.model.StructModel;

import java.lang.classfile.CodeBuilder;
import java.lang.constant.MethodTypeDesc;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_PaddingLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_String;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_long;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_sequenceLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.emitValueLayout;

/**
 * Derives the emit-time {@code MemoryLayout.structLayout(...)} shape (named field layouts plus the
 * padding gaps between them) from a struct model whose fields already carry their resolved absolute
 * offsets, and emits that argument array as bytecode. Offset computation itself lives in the parser.
 */
final class StructLayoutUtil {

    // paddingLayout returns PaddingLayout (a subtype of MemoryLayout); invokestatic requires the exact descriptor.
    private static final MethodTypeDesc MTD_paddingLayout = MethodTypeDesc.of(CD_PaddingLayout, CD_long);
    private static final MethodTypeDesc MTD_withName = MethodTypeDesc.of(CD_MemoryLayout, CD_String);

    private StructLayoutUtil() {}

    /** A field along with the padding (in bytes) that precedes it in the struct layout. */
    record LayoutField(StructFieldModel field, long paddingBefore) {}

    /**
     * Derives the per-field layout entries (each with the padding gap that precedes it) from a
     * model whose fields carry absolute offsets. The gap before field {@code i} is
     * {@code offset(i) - end(i-1)}.
     */
    static List<LayoutField> deriveLayout(List<StructFieldModel> fields) {
        List<LayoutField> result = new ArrayList<>();
        long cursor = 0;
        for (StructFieldModel field : fields) {
            result.add(new LayoutField(field, field.offset() - cursor));
            cursor = field.offset() + field.byteSize();
        }
        return result;
    }

    /** Trailing padding between the end of the last field and {@code model.byteSize()}. */
    static long trailingPadding(StructModel model) {
        long end = 0;
        for (StructFieldModel field : model.fields()) {
            end = field.offset() + field.byteSize();
        }
        return model.byteSize() - end;
    }

    /**
     * Emits bytecode that constructs the {@code MemoryLayout[]} array for
     * {@code MemoryLayout.structLayout(...)}, including named field layouts and any inline
     * padding layouts. The array is left on the operand stack.
     */
    static void emitStructLayoutArray(CodeBuilder cb, List<LayoutField> layout) {
        emitStructLayoutArray(cb, layout, 0);
    }

    /**
     * Emits bytecode that constructs the {@code MemoryLayout[]} array for
     * {@code MemoryLayout.structLayout(...)}, including named field layouts, any inline padding
     * layouts before fields, and optional trailing padding. The array is left on the operand stack.
     *
     * @param trailingPadding bytes of padding to emit after the last named field (0 = none)
     */
    static void emitStructLayoutArray(CodeBuilder cb, List<LayoutField> layout, long trailingPadding) {
        int arraySize = layout.size();
        for (LayoutField lf : layout) {
            if (lf.paddingBefore() > 0) {
                arraySize++;
            }
        }
        if (trailingPadding > 0) {
            arraySize++;
        }
        cb.loadConstant(arraySize);
        cb.anewarray(CD_MemoryLayout);
        int arrayIndex = 0;
        for (LayoutField lf : layout) {
            if (lf.paddingBefore() > 0) {
                cb.dup();
                cb.loadConstant(arrayIndex++);
                cb.loadConstant(lf.paddingBefore());
                cb.invokestatic(CD_MemoryLayout, "paddingLayout", MTD_paddingLayout, true);
                cb.aastore();
            }
            cb.dup();
            cb.loadConstant(arrayIndex++);
            emitFieldLayout(cb, lf.field());
            cb.ldc(lf.field().name());
            cb.invokeinterface(CD_MemoryLayout, "withName", MTD_withName);
            cb.aastore();
        }
        if (trailingPadding > 0) {
            cb.dup();
            cb.loadConstant(arrayIndex);
            cb.loadConstant(trailingPadding);
            cb.invokestatic(CD_MemoryLayout, "paddingLayout", MTD_paddingLayout, true);
            cb.aastore();
        }
    }

    /**
     * Emits the base layout for a field (before {@code withName}): a scalar {@code ValueLayout} for
     * scalar and array-pointer fields, or a {@code sequenceLayout(length, elementLayout)} for inline
     * array and inline string fields.
     */
    private static void emitFieldLayout(CodeBuilder cb, StructFieldModel field) {
        switch (field) {
            case InlineArrayFieldModel inlineArray -> {
                cb.loadConstant((long) inlineArray.length());
                emitValueLayout(cb, inlineArray.elementType());
                cb.invokestatic(CD_MemoryLayout, "sequenceLayout", MTD_sequenceLayout, true);
            }
            case InlineStringFieldModel inlineString -> {
                cb.loadConstant((long) inlineString.length());
                emitValueLayout(cb, NativeType.BYTE);
                cb.invokestatic(CD_MemoryLayout, "sequenceLayout", MTD_sequenceLayout, true);
            }
            default -> emitValueLayout(cb, field.type());
        }
    }
}
