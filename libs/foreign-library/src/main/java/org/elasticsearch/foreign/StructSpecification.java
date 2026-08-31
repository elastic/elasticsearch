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
 * and is accessed field-by-field via VarHandle-backed accessors; the generated implementation is
 * {@link Addressable}. Native array pointer fields are declared with {@link ArrayField @ArrayField}.
 *
 * <p>Example — a dense struct, whose fields lay out in declaration order with natural alignment:
 *
 * <pre>{@code
 * @StructSpecification
 * interface Timespec {
 *     long tvSec();  void tvSec(long v);
 *     long tvNsec(); void tvNsec(long v);
 * }
 * }</pre>
 *
 * <p>Example — a sparse struct, placing fields at explicit offsets (see {@link Offset} and
 * {@link StructSize}) for a layout defined by the platform ABI:
 *
 * <pre>{@code
 * @StructSpecification(sparse = true)
 * @StructSize(144)
 * interface Stat64 {
 *     @Offset(48) long stSize();   void stSize(long v);
 *     @Offset(56) long stBlocks(); void stBlocks(long v);
 * }
 * }</pre>
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
     * in declaration order with C natural-alignment padding inserted automatically. {@link Offset
     * @Offset} and {@link StructSize @StructSize} are compile errors.
     */
    boolean sparse() default false;
}
