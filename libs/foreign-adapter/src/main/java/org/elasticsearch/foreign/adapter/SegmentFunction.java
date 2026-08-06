/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.adapter;

import java.lang.foreign.MemorySegment;

/**
 * A function applied to a {@link MemorySegment} that is allowed to throw a checked exception.
 *
 * @param <R> the result type
 * @param <E> the type of exception the function may throw
 */
@FunctionalInterface
public interface SegmentFunction<R, E extends Exception> {

    /**
     * Applies this function to the given segment.
     *
     * @param segment a memory segment that is valid only for the duration of this call
     */
    R apply(MemorySegment segment) throws E;
}
