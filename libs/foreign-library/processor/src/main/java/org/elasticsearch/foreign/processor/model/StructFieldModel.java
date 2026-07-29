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
 * A single field of a {@code @StructSpecification} struct — a scalar value, an array pointer, a
 * fixed-size inline primitive array, or a fixed-size inline C string. Every variant exposes a
 * field name and the native layout type used to read or write the field.
 */
public sealed interface StructFieldModel permits ScalarFieldModel, ArrayFieldModel, InlineArrayFieldModel, InlineStringFieldModel {
    /** Field name (method name for interfaces, component name for records). */
    String name();

    /** The native layout type used to read or write this field. */
    NativeType type();

    /** Absolute byte offset of this field within the struct, resolved for one platform. */
    long offset();

    /** Returns a copy of this field placed at the given absolute byte offset. */
    StructFieldModel withOffset(long offset);

    /** Total size in bytes this field occupies in the struct layout (a 64-bit ABI). */
    default long byteSize() {
        return switch (this) {
            case ScalarFieldModel scalar -> scalar.type().byteSize();
            case ArrayFieldModel array -> array.type().byteSize();
            case InlineArrayFieldModel inlineArray -> (long) inlineArray.length() * inlineArray.elementType().byteSize();
            case InlineStringFieldModel inlineString -> inlineString.length();
        };
    }

    /** Natural alignment in bytes of this field in the struct layout (a 64-bit ABI). */
    default long alignment() {
        return switch (this) {
            case ScalarFieldModel scalar -> scalar.type().byteSize();
            case ArrayFieldModel array -> array.type().byteSize();
            case InlineArrayFieldModel inlineArray -> inlineArray.elementType().byteSize();
            case InlineStringFieldModel ignored -> 1;
        };
    }
}
