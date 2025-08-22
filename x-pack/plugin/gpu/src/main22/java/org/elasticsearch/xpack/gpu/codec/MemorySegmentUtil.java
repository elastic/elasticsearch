/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import java.lang.foreign.MemorySegment;

/**
 * Utility methods to act on MemorySegment apis which have changed in subsequent JDK releases.
 */
class MemorySegmentUtil {
    static String getString(MemorySegment segment, long offset) {
        return segment.getString(offset);
    }
}
