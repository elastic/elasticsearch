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

import java.lang.foreign.MemorySegment;

/**
 * {@link Offsets} for Java scorers.
 */
public final class HeapOffsets extends Offsets {

    private final int[] offsets;

    public HeapOffsets(int capacity) {
        this.offsets = new int[capacity];
    }

    @Override
    public MemorySegment getSegment() {
        return MemorySegment.ofArray(offsets);
    }

    @Override
    protected int read(int index) {
        return offsets[index];
    }

    @Override
    protected void write(int index, int position) {
        offsets[index] = position;
    }
}
