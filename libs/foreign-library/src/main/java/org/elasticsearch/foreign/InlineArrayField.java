/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares an indexed accessor for a fixed-size inline primitive array on a
 * {@link StructSpecification @StructSpecification} interface. The annotated method takes one
 * {@code int} argument (the element index) and returns a primitive element type. The field
 * contributes a {@code sequenceLayout(length, elementLayout)} to the struct layout.
 *
 * <p>A companion setter {@code void fieldName(int index, T value)} is allowed and will share
 * the same field layout and {@code VarHandle}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface InlineArrayField {
    /** Number of elements in the fixed-size inline array. Must be positive. */
    int length();
}
