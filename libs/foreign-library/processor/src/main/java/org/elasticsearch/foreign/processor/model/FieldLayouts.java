/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;

/**
 * Layout arithmetic derived purely from a field's shape (a 64-bit ABI): its byte size and natural
 * alignment, the {@code ValueLayout}/sequence layout it contributes, and — given each field's
 * resolved absolute offset — the whole {@code MemoryLayout.structLayout(...)}. Kept off
 * {@link StructFieldModel} so that type stays plain shape data.
 */
final class FieldLayouts {

    private FieldLayouts() {}

    /** Total size in bytes a field occupies in the struct layout. */
    static long byteSize(StructFieldModel field) {
        return switch (field) {
            case ScalarFieldModel scalar -> scalar.type().byteSize();
            case ArrayFieldModel array -> array.type().byteSize();
            case InlineArrayFieldModel inlineArray -> (long) inlineArray.length() * inlineArray.elementType().byteSize();
            case InlineStringFieldModel inlineString -> inlineString.length();
        };
    }

    /** Natural alignment of a field, used to place it in dense mode. */
    static long alignment(StructFieldModel field) {
        return switch (field) {
            case ScalarFieldModel scalar -> scalar.type().byteSize();
            case ArrayFieldModel array -> array.type().byteSize();
            case InlineArrayFieldModel inlineArray -> inlineArray.elementType().byteSize();
            case InlineStringFieldModel ignored -> 1;
        };
    }

    /**
     * Builds the struct's {@link MemoryLayout} for one platform: each field's named value/sequence
     * layout at its resolved absolute offset, {@code paddingLayout} for the gaps between fields, and a
     * trailing {@code paddingLayout} when {@code totalByteSize} exceeds the end of the last field.
     *
     * @param offsets absolute byte offsets, index-aligned with {@code fields}
     */
    static MemoryLayout structLayout(List<StructFieldModel> fields, List<Long> offsets, long totalByteSize) {
        List<MemoryLayout> members = new ArrayList<>();
        long cursor = 0;
        for (int i = 0; i < fields.size(); i++) {
            StructFieldModel field = fields.get(i);
            long offset = offsets.get(i);
            if (offset > cursor) {
                members.add(MemoryLayout.paddingLayout(offset - cursor));
            }
            members.add(memberLayout(field).withName(field.name()));
            cursor = offset + byteSize(field);
        }
        if (totalByteSize > cursor) {
            members.add(MemoryLayout.paddingLayout(totalByteSize - cursor));
        }
        return MemoryLayout.structLayout(members.toArray(MemoryLayout[]::new));
    }

    /** The (unnamed) value or sequence layout a field contributes to the struct layout. */
    private static MemoryLayout memberLayout(StructFieldModel field) {
        return switch (field) {
            case ScalarFieldModel scalar -> valueLayout(scalar.type());
            case ArrayFieldModel array -> valueLayout(array.type());
            case InlineArrayFieldModel inlineArray -> MemoryLayout.sequenceLayout(
                inlineArray.length(),
                valueLayout(inlineArray.elementType())
            );
            case InlineStringFieldModel inlineString -> MemoryLayout.sequenceLayout(inlineString.length(), ValueLayout.JAVA_BYTE);
        };
    }

    private static ValueLayout valueLayout(NativeType type) {
        return switch (type) {
            case INT -> ValueLayout.JAVA_INT;
            case LONG -> ValueLayout.JAVA_LONG;
            case SHORT -> ValueLayout.JAVA_SHORT;
            case BYTE -> ValueLayout.JAVA_BYTE;
            case BOOLEAN -> ValueLayout.JAVA_BOOLEAN;
            case FLOAT -> ValueLayout.JAVA_FLOAT;
            case DOUBLE -> ValueLayout.JAVA_DOUBLE;
            case ADDRESS, STRING -> ValueLayout.ADDRESS;
            case ADDRESSABLE -> throw new AssertionError("ADDRESSABLE cannot be a struct field type");
            case VOID -> throw new AssertionError("void cannot be a struct field type");
        };
    }
}
