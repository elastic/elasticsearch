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
 * Declares an indexed accessor for a native array field on a
 * {@link StructSpecification @StructSpecification} interface. The annotated method takes one
 * {@code int} argument (the element index) and returns the element type, which must be a
 * {@link StructSpecification @StructSpecification} record declared in the same
 * {@link LibrarySpecification @LibrarySpecification} interface. Each call returns a fresh record
 * read from the native array at the requested index.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface ArrayField {
    /**
     * Name of the sibling scalar-getter method on the same {@code @StructSpecification} interface
     * that holds the element count of this array (e.g. {@code "len"} referring to a
     * {@code short len()} method).
     */
    String lengthField();
}
