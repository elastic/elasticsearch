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
 * Marks a record or interface nested in a {@link LibrarySpecification @LibrarySpecification}
 * interface as a native struct specification.
 *
 * <p>When applied to a record, the record is a Java-side value type describing a C struct
 * layout; instances can be copied into native memory (typically via a
 * {@link StructFactory @StructFactory} method).
 *
 * <p>When applied to an interface, the interface represents a struct that lives in native memory
 * and is accessed field-by-field via VarHandles. Such interfaces must extend {@link Addressable},
 * and their native array pointer fields are declared with {@link ArrayField @ArrayField}.
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.TYPE)
public @interface StructSpecification {
    /**
     * When {@code true}, the struct is in sparse mode: every field must declare an {@link Offset
     * @Offset}, and the type must declare a {@link StructSize @StructSize}. Fields are placed at
     * their resolved offsets; unmodeled bytes become padding in the layout.
     *
     * <p>When {@code false} (the default), the struct is in dense mode: fields lay out sequentially
     * in declaration order. {@link Padding @Padding} is allowed; {@link Offset @Offset} and
     * {@link StructSize @StructSize} are compile errors.
     */
    boolean sparse() default false;
}
