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
 * A single field of a {@code @StructSpecification} struct, either a {@link ScalarFieldModel scalar
 * value} or an {@link ArrayFieldModel array pointer}. Every variant exposes a field name and the
 * native layout type used to read or write the field.
 */
public sealed interface StructFieldModel permits ScalarFieldModel, ArrayFieldModel {
    /** Field name (method name for interfaces, component name for records). */
    String name();

    /** The native layout type used to read or write this field. */
    NativeType type();
}
