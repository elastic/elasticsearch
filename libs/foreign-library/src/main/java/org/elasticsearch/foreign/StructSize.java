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
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Sets the total struct size in bytes for a {@link StructSpecification @StructSpecification}
 * interface. Required in sparse mode ({@link StructSpecification#sparse()} = true); a compile error
 * in dense mode.
 *
 * <p>When multiple {@code @StructSize} annotations appear on the same type, each one specifies the
 * struct size for particular platforms. A bare {@code @StructSize(N)} (empty {@code platforms}) is
 * a universal fallback for any platform without a specific entry. Every supported platform must
 * resolve to exactly one value; overlapping coverage and duplicate universals are compile errors.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
@Repeatable(StructSize.List.class)
public @interface StructSize {
    /** Total struct size in bytes. */
    int value();

    /** Platforms this size applies to. Empty means all platforms. */
    Platform[] platforms() default {};

    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.SOURCE)
    @interface List {
        StructSize[] value();
    }
}
