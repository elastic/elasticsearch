/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal.vectorization;

import org.elasticsearch.simdvec.VectorComparisonUtils;

/** Noddy implementation, you likely do not want to use this in production, check that vectorLength > 1. */
public class DefaultVectorComparisonUtils implements VectorComparisonUtils {

    public static DefaultVectorComparisonUtils INSTANCE = new DefaultVectorComparisonUtils();

    private DefaultVectorComparisonUtils() {}

    @Override
    public long equalMask(byte[] array, int offset, byte value) {
        return array[offset] == value ? 1L : 0L;
    }

    @Override
    public int byteVectorLanes() {
        return 1;
    }

    @Override
    public long equalMask(long[] array, int offset, long value) {
        return array[offset] == value ? 1L : 0L;
    }

    @Override
    public int longVectorLanes() {
        return 1;
    }
}
