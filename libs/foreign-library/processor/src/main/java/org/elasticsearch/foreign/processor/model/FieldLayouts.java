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
 * Builds a struct's {@link MemoryLayout} from its field shapes plus resolved placement — a dense
 * layout (fields in declaration order with C natural-alignment padding) or a sparse layout (fields at
 * absolute offsets). Each field's size and alignment come from its own member layout
 * ({@link #memberLayout}), so they are never computed here.
 */
final class FieldLayouts {

    private FieldLayouts() {}

    /**
     * Builds a dense struct layout: fields in declaration order with C natural-alignment padding
     * inserted automatically before any field the running offset does not already leave aligned.
     * There is no trailing padding — the total size is the end of the last field.
     */
    static MemoryLayout denseStructLayout(List<StructFieldModel> fields) {
        List<MemoryLayout> members = new ArrayList<>();
        long cursor = 0;
        for (StructFieldModel field : fields) {
            MemoryLayout member = memberLayout(field);
            long pad = naturalPadding(cursor, member.byteAlignment());
            if (pad > 0) {
                members.add(MemoryLayout.paddingLayout(pad));
                cursor += pad;
            }
            members.add(member.withName(field.name()));
            cursor += member.byteSize();
        }
        return MemoryLayout.structLayout(members.toArray(MemoryLayout[]::new));
    }

    private static long naturalPadding(long cursor, long alignment) {
        return cursor % alignment == 0 ? 0 : alignment - cursor % alignment;
    }

    /**
     * Builds a sparse struct layout: each field placed at its absolute {@code offset}, with the gap
     * before it and any trailing padding derived from a running total of the member layouts added so
     * far. The members carry their own sizes, so none are computed here.
     *
     * @param offsets absolute byte offsets, index-aligned with {@code fields}
     * @param totalByteSize the struct's total size (its resolved {@code @StructSize})
     */
    static MemoryLayout sparseStructLayout(List<StructFieldModel> fields, List<Long> offsets, long totalByteSize) {
        List<MemoryLayout> members = new ArrayList<>();
        long cursor = 0;
        for (int i = 0; i < fields.size(); i++) {
            StructFieldModel field = fields.get(i);
            long gap = offsets.get(i) - cursor;
            if (gap > 0) {
                members.add(MemoryLayout.paddingLayout(gap));
                cursor += gap;
            }
            MemoryLayout member = memberLayout(field).withName(field.name());
            members.add(member);
            cursor += member.byteSize();
        }
        if (totalByteSize > cursor) {
            members.add(MemoryLayout.paddingLayout(totalByteSize - cursor));
        }
        return MemoryLayout.structLayout(members.toArray(MemoryLayout[]::new));
    }

    /**
     * The (unnamed) value or sequence layout a field contributes to the struct layout. Its
     * {@code byteSize()} and {@code byteAlignment()} are the field's size and natural alignment, so
     * callers read those from the returned layout rather than computing them.
     */
    static MemoryLayout memberLayout(StructFieldModel field) {
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
            case ADDRESS -> ValueLayout.ADDRESS;
            // Marshaling-only types, not struct-field layout types; the parser rejects them as fields
            // before layout, so this is unreachable.
            case VOID, STRING, ADDRESSABLE, UPCALL -> throw new AssertionError(type + " is not a struct field layout type");
        };
    }
}
