/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec.internal;

import org.elasticsearch.simdvec.Offsets;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * {@link Offsets} stored in off-heap memory.
 */
public final class NativeOffsets extends Offsets {

    private final MemorySegment segment;

    public NativeOffsets(int capacity) {
        this.segment = Arena.ofAuto().allocate((long) capacity * Integer.BYTES, ValueLayout.JAVA_INT.byteAlignment());
    }

    @Override
    public MemorySegment getSegment() {
        return segment;
    }

    @Override
    protected int read(int index) {
        return segment.getAtIndex(ValueLayout.JAVA_INT, index);
    }

    @Override
    protected void write(int index, int position) {
        segment.setAtIndex(ValueLayout.JAVA_INT, index, position);
    }
}
