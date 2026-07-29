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
 * Inserts a padding gap <em>before</em> the annotated field in a dense
 * {@link StructSpecification @StructSpecification}. Used for alignment padding or reserved slots in
 * an otherwise sequential layout. {@code @Padding} is a compile error in sparse mode.
 *
 * <p>When multiple {@code @Padding} annotations appear on the same element, each one specifies the
 * padding for particular platforms. A bare {@code @Padding(N)} (empty {@code platforms}) is a
 * universal fallback for any platform without a specific entry. Every supported platform must
 * resolve to exactly one value; overlapping coverage and duplicate universals are compile errors.
 *
 * <p>When a field has both a getter and a setter, {@code @Padding} must be placed on the
 * first-declared accessor; placing it on the second accessor is a compile error.
 */
@Target({ ElementType.METHOD, ElementType.RECORD_COMPONENT })
@Retention(RetentionPolicy.SOURCE)
@Repeatable(Padding.List.class)
public @interface Padding {
    /** Bytes of padding to insert before this field. */
    int value();

    /** Platforms this padding applies to. Empty means all platforms. */
    Platform[] platforms() default {};

    @Target({ ElementType.METHOD, ElementType.RECORD_COMPONENT })
    @Retention(RetentionPolicy.SOURCE)
    @interface List {
        Padding[] value();
    }
}
