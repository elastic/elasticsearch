/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data.arrow;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FixedWidthVector;

public class ArrowUtils {
    private ArrowUtils() {}

    public static void releaseBuffers(ArrowBuf... buffers) {
        for (ArrowBuf buf : buffers) {
            if (buf != null) {
                buf.getReferenceManager().release();
            }
        }
    }

    public static void retainBuffers(ArrowBuf... buffers) {
        for (ArrowBuf buf : buffers) {
            if (buf != null) {
                buf.getReferenceManager().retain();
            }
        }
    }

    public static void checkItemSize(FixedWidthVector vec, int byteSize) {
        int valueSize = vec.getBufferSizeFor(1) - 1; // 1 for validity
        if (byteSize != valueSize) {
            throw new IllegalArgumentException(
                "Expecting value size[ " + byteSize + "] but got [" + valueSize + "] for " + vec.getClass().getSimpleName()
            );
        }
    }
}
