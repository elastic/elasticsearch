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
 * Declares a fixed-size null-terminated C string field on a
 * {@link StructSpecification @StructSpecification} interface. The annotated method must return
 * {@code String} and take no parameters. The field contributes a
 * {@code sequenceLayout(length, JAVA_BYTE)} to the struct layout.
 *
 * <p>A companion setter {@code void fieldName(String value)} is allowed, which writes the string
 * into the fixed-size field, zero-padding to {@code length} bytes.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface InlineStringField {
    /** Total byte length of the fixed-size character array (including any NUL terminator). Must be positive. */
    int length();
}
