/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import java.lang.invoke.MethodHandle;

/**
 * Utility interface providing vector similarity functions.
 *
 * <p> MethodHandles are returned to avoid a static reference to MemorySegment,
 * which is not in the currently lowest compile version, JDK 17. Code consuming
 * the method handles will, by definition, require access to MemorySegment.
 */
public interface VectorSimilarityFunctions {
    MethodHandle dotProductHandle();

    MethodHandle squareDistanceHandle();
}
