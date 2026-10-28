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
 * A fixed-size inline primitive array {@link StructFieldModel} declared via
 * {@code @InlineArrayField(length = N)}. The field contributes a
 * {@code sequenceLayout(length, elementLayout)} to the struct layout. A {@code VarHandle} is
 * built with a {@code groupElement(name) + sequenceElement()} path for indexed access.
 *
 * <p>A field may have a getter, a setter, or both — but at least one must be present.
 */
public record InlineArrayFieldModel(String name, NativeType elementType, int length, boolean hasGetter, boolean hasSetter)
    implements
        StructFieldModel {
    @Override
    public NativeType type() {
        return elementType;
    }
}
