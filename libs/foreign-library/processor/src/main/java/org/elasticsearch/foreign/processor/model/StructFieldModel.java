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
 * Models a single field of a {@code @StructSpecification} type, either a scalar value or a
 * native array pointer.
 *
 * <p>Scalar fields carry a primitive or address {@link NativeType}. Array fields always use
 * {@link NativeType#ADDRESS} (the pointer) and carry the element type simple name and the name
 * of the sibling scalar length field on the same struct.
 *
 * @param name              field name (method name for interfaces, component name for records)
 * @param type              native layout type; always {@link NativeType#ADDRESS} for array fields
 * @param elementSimpleName simple name of the array element type; {@code null} for scalar fields
 * @param lengthFieldName   name of the sibling scalar length field on the same struct;
 *                          {@code null} for scalar fields
 */
public record StructFieldModel(String name, NativeType type, String elementSimpleName, String lengthFieldName) {

    public boolean isArray() {
        return elementSimpleName != null;
    }

    public static StructFieldModel scalar(String name, NativeType type) {
        return new StructFieldModel(name, type, null, null);
    }

    public static StructFieldModel array(String name, String elementSimpleName, String lengthFieldName) {
        return new StructFieldModel(name, NativeType.ADDRESS, elementSimpleName, lengthFieldName);
    }
}
