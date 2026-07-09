/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import org.elasticsearch.foreign.processor.model.NativeType;
import org.elasticsearch.foreign.processor.model.StructFieldModel;

import java.lang.classfile.CodeBuilder;
import java.lang.constant.MethodTypeDesc;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_PaddingLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_String;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_long;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.emitValueLayout;

/**
 * Computes C natural-alignment struct layouts from a {@link StructFieldModel} list, and emits the
 * matching {@code MemoryLayout.structLayout(...)} argument array as bytecode.
 */
final class StructLayoutUtil {

    // paddingLayout returns PaddingLayout (a subtype of MemoryLayout); invokestatic requires the exact descriptor.
    private static final MethodTypeDesc MTD_paddingLayout = MethodTypeDesc.of(CD_PaddingLayout, CD_long);
    private static final MethodTypeDesc MTD_withName = MethodTypeDesc.of(CD_MemoryLayout, CD_String);

    private StructLayoutUtil() {}

    /** A field along with the padding (in bytes) that precedes it in the struct layout. */
    record LayoutField(StructFieldModel field, long paddingBefore) {}

    /**
     * Computes per-field padding using C natural-alignment rules, assuming a 64-bit ADDRESS
     * (8 bytes). Every field is aligned to its own size; padding is inserted before any field
     * whose alignment isn't satisfied by the running offset.
     */
    static List<LayoutField> computeLayout(List<StructFieldModel> fields) {
        List<LayoutField> result = new ArrayList<>();
        long offset = 0;
        for (StructFieldModel field : fields) {
            long align = alignmentOf(field.type());
            long padding = (offset % align == 0) ? 0 : (align - offset % align);
            result.add(new LayoutField(field, padding));
            offset += padding + sizeOf(field.type());
        }
        return result;
    }

    /** Static byte size for a native type, assuming a 64-bit ABI. */
    static long sizeOf(NativeType type) {
        return switch (type) {
            case BOOLEAN, BYTE -> 1;
            case SHORT -> 2;
            case INT, FLOAT -> 4;
            case LONG, DOUBLE, ADDRESS -> 8;
            case VOID, STRING, ADDRESSABLE -> throw new AssertionError("no size for type: " + type);
        };
    }

    /** Natural alignment for a native type, equal to its size for all supported types. */
    static long alignmentOf(NativeType type) {
        return sizeOf(type);
    }

    /**
     * Emits bytecode that constructs the {@code MemoryLayout[]} array for
     * {@code MemoryLayout.structLayout(...)}, including named field layouts and any inline
     * padding layouts. The array is left on the operand stack.
     */
    static void emitStructLayoutArray(CodeBuilder cb, List<LayoutField> layout) {
        int arraySize = layout.size();
        for (LayoutField lf : layout) {
            if (lf.paddingBefore() > 0) {
                arraySize++;
            }
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
            emitValueLayout(cb, lf.field().type());
            cb.ldc(lf.field().name());
            cb.invokeinterface(CD_MemoryLayout, "withName", MTD_withName);
            cb.aastore();
        }
    }
}
