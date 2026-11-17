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
 * Marks a method on a {@link LibrarySpecification @LibrarySpecification} interface as a native
 * struct factory: it allocates a new native struct instance and populates its fields from the
 * supplied Java-side values. The return type must be a
 * {@link StructSpecification @StructSpecification} interface enclosed in the same
 * {@code @LibrarySpecification}.
 *
 * <p>The parameter list supplies the field values. When a field is declared with
 * {@link ArrayField @ArrayField}, the corresponding parameter is a Java array of the element
 * type — the array itself carries the element count, so no separate length parameter is needed.
 *
 * <p>No {@link Function @Function} annotation is required or allowed on factory methods.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface StructFactory {
}
