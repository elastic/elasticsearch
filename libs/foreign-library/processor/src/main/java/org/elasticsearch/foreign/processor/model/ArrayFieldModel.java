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
 * An array-pointer {@link StructFieldModel} — a native pointer field naming a separately-allocated
 * array of element records. Every array field carries the simple name of the element type and the
 * name of the sibling scalar length field on the same struct.
 */
public record ArrayFieldModel(String name, String elementSimpleName, String lengthFieldName) implements StructFieldModel {
    @Override
    public NativeType type() {
        return NativeType.ADDRESS;
    }
}
