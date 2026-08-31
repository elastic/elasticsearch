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

import java.lang.classfile.CodeBuilder;
import java.lang.constant.MethodTypeDesc;
import java.lang.foreign.GroupLayout;
import java.lang.foreign.MemoryLayout;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_MemoryLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_PaddingLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_String;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.CD_long;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.MTD_sequenceLayout;
import static org.elasticsearch.foreign.processor.ClassWriterUtil.emitValueLayout;

/**
 * Emits the {@code MemoryLayout[]} argument array for {@code MemoryLayout.structLayout(...)} that
 * reconstructs, at runtime, a struct's resolved {@link MemoryLayout} (already built by the parser).
 * The model layout is walked member-by-member: a named member is a field (re-emitted from its shape
 * plus {@code withName}), an unnamed member is a {@code paddingLayout} gap.
 */
final class StructLayoutUtil {

    // paddingLayout returns PaddingLayout (a subtype of MemoryLayout); invokestatic requires the exact descriptor.
    private static final MethodTypeDesc MTD_paddingLayout = MethodTypeDesc.of(CD_PaddingLayout, CD_long);
    private static final MethodTypeDesc MTD_withName = MethodTypeDesc.of(CD_MemoryLayout, CD_String);

    private StructLayoutUtil() {}

    /**
     * Emits bytecode constructing the {@code MemoryLayout[]} array whose elements are, in order, the
     * member layouts of {@code layout} — each named member re-emitted from its field shape (looked up
     * in {@code fieldsByName}) with {@code withName}, each unnamed member as a {@code paddingLayout}.
     * The array is left on the operand stack for a following {@code structLayout(...)} call.
     */
    static void emitStructLayoutArray(CodeBuilder cb, MemoryLayout layout, Map<String, StructFieldModel> fieldsByName) {
        // structLayout(...) produces a GroupLayout; walk its members (named fields + padding gaps).
        List<MemoryLayout> members = ((GroupLayout) layout).memberLayouts();
        cb.loadConstant(members.size());
        cb.anewarray(CD_MemoryLayout);
        int arrayIndex = 0;
        for (MemoryLayout member : members) {
            cb.dup();
            cb.loadConstant(arrayIndex++);
            if (member.name().isPresent()) {
                StructFieldModel field = fieldsByName.get(member.name().get());
                emitFieldLayout(cb, field);
                cb.ldc(field.name());
                cb.invokeinterface(CD_MemoryLayout, "withName", MTD_withName);
            } else {
                cb.loadConstant(member.byteSize());
                cb.invokestatic(CD_MemoryLayout, "paddingLayout", MTD_paddingLayout, true);
            }
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
