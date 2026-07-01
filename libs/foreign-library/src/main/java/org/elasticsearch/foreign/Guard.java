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
 * Adds a precondition check before each call to a {@link Function @Function} binding.
 * The processor generates a direct {@code invokestatic} call to the checker method at the
 * start of the generated method body, before the native downcall.
 *
 * <p>The checker method must be {@code public static}, return {@code void}, and accept
 * exactly the same parameter types as the annotated native method. It should return
 * normally on success or throw an exception on failure (like {@link java.util.Objects#checkFromIndexSize}).
 *
 * <pre>{@code
 * @Function("native_op")
 * @Guard(checkerClass = Checks.class, checkerMethod = "checkOp")
 * int op(MemorySegment a, MemorySegment b, int length);
 *
 * // Checks.checkOp(MemorySegment a, MemorySegment b, int length) -> void
 * }</pre>
 */
@Retention(RetentionPolicy.SOURCE)
@Target(ElementType.METHOD)
public @interface Guard {

    /**
     * Class containing the {@code public static} check method.
     */
    Class<?> checkerClass();

    /**
     * Name of the check method on {@link #checkerClass()}.
     */
    String checkerMethod();
}
