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
 * Models a single field in a {@code @Struct}-annotated interface.
 *
 * @param name the field name (taken from the getter method name)
 * @param nativeType the native type that maps to a {@code ValueLayout} constant
 * @param paddingBefore bytes of padding to insert before this field in the struct layout (0 if none)
 * @param arrayOf fully-qualified class name of the element type for {@code @ArrayOf} fields, or null
 * @param arrayLengthFor name of the {@code @ArrayOf} field whose count this field stores, or null
 * @param hasSetter whether this field has an accompanying {@code @Setter}-annotated method
 */
public record FieldModel(String name, NativeType nativeType, int paddingBefore, String arrayOf, String arrayLengthFor, boolean hasSetter) {}
