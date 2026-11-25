/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

/**
 * A fixed-size null-terminated C string {@link StructFieldModel} declared via
 * {@code @InlineStringField(length = N)}. The field contributes a
 * {@code sequenceLayout(length, JAVA_BYTE)} to the struct layout. No {@code VarHandle} is used;
 * accessors operate directly on the segment slice using {@code MemorySegmentAdapter}.
 *
 * <p>A field may have a getter, a setter, or both — but at least one must be present.
 *
 * @param length total byte length of the fixed-size field, including any NUL terminator; always in bytes,
 *        regardless of {@code wide}
 * @param wide whether the field is UTF-16LE-encoded (e.g. a Windows {@code WCHAR name[N]}) rather than the
 *        implicit UTF-8 default (e.g. a POSIX {@code char name[N]})
 */
public record InlineStringFieldModel(String name, int length, boolean wide, boolean hasGetter, boolean hasSetter)
    implements
        StructFieldModel {
    @Override
    public NativeType type() {
        return NativeType.BYTE;
    }
}
