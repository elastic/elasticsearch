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
 * Places a {@link StructSpecification @StructSpecification} field at an absolute byte offset within
 * the struct. Required on every field in sparse mode ({@link StructSpecification#sparse()} = true).
 *
 * <p>When multiple {@code @Offset} annotations appear on the same element, each one specifies the
 * offset for particular platforms. A bare {@code @Offset(N)} (empty {@code platforms}) is the
 * fallback for any platform without a specific entry. Every supported platform must resolve to
 * exactly one value; overlapping coverage and more than one platform-independent {@code @Offset} are
 * compile errors.
 *
 * <p>When a field has both a getter and a setter, {@code @Offset} must be placed on the
 * first-declared accessor; placing it on the second accessor is a compile error.
 *
 * <p>Example — placing each field of a sparse struct at its ABI offset:
 *
 * <pre>{@code
 * @StructSpecification(sparse = true)
 * @StructSize(144)
 * interface Stat64 {
 *     @Offset(48) long stSize();   void stSize(long v);
 *     @Offset(56) long stBlocks(); void stBlocks(long v);
 * }
 * }</pre>
 *
 * <p>Example — a field at a different offset per platform (the bare {@code @Offset(96)} is the
 * fallback for platforms without a specific entry):
 *
 * <pre>{@code
 * @Offset(value = 48, platforms = { Platform.LINUX_X64, Platform.LINUX_AARCH64 })
 * @Offset(96)
 * long stSize();
 * }</pre>
 */
@Target({ ElementType.METHOD, ElementType.RECORD_COMPONENT })
@Retention(RetentionPolicy.SOURCE)
@Repeatable(Offset.List.class)
public @interface Offset {
    /** Byte offset of this field within the struct. */
    int value();

    /**
     * Platforms this offset applies to. Empty means "all platforms" — the fallback for any platform
     * not covered by another per-platform variant.
     */
    Platform[] platforms() default {};

    @Target({ ElementType.METHOD, ElementType.RECORD_COMPONENT })
    @Retention(RetentionPolicy.SOURCE)
    @interface List {
        Offset[] value();
    }
}
