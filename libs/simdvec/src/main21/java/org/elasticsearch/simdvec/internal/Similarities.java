/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.VectorSimilarityFunctions;

import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;

public class Similarities {

    static final VectorSimilarityFunctions DISTANCE_FUNCS = NativeAccess.instance()
        .getVectorSimilarityFunctions()
        .orElseThrow(AssertionError::new);

    static final MethodHandle DOT_PRODUCT_7U = DISTANCE_FUNCS.dotProductHandle7u();
    static final MethodHandle SQUARE_DISTANCE_7U = DISTANCE_FUNCS.squareDistanceHandle7u();
    static final MethodHandle COS_PRODUCT_FLOAT32 = DISTANCE_FUNCS.cosineHandleFloat32();
    static final MethodHandle DOT_PRODUCT_FLOAT32 = DISTANCE_FUNCS.dotProductHandleFloat32();
    static final MethodHandle SQR_PRODUCT_FLOAT32 = DISTANCE_FUNCS.squareDistanceHandleFloat32();

    static int dotProduct7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) DOT_PRODUCT_7U.invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static int squareDistance7u(MemorySegment a, MemorySegment b, int length) {
        try {
            return (int) SQUARE_DISTANCE_7U.invokeExact(a, b, length);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static float cosineFloat32(MemorySegment a, MemorySegment b, int elementCount) {
        try {
            return (float) COS_PRODUCT_FLOAT32.invokeExact(a, b, elementCount);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static float dotProductFloat32(MemorySegment a, MemorySegment b, int elementCount) {
        try {
            return (float) DOT_PRODUCT_FLOAT32.invokeExact(a, b, elementCount);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    static float squareDistanceFloat32(MemorySegment a, MemorySegment b, int elementCount) {
        try {
            return (float) SQR_PRODUCT_FLOAT32.invokeExact(a, b, elementCount);
        } catch (Throwable e) {
            if (e instanceof Error err) {
                throw err;
            } else if (e instanceof RuntimeException re) {
                throw re;
            } else {
                throw new RuntimeException(e);
            }
        }
    }
}
