/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import java.lang.foreign.MemorySegment;

/**
 * The positions of the vectors to score.
 * <p>
 * Instances are created by the scorer that reads them ({@link ES940OSQVectorsScorer#createOffsetStorage()},
 * {@link AshScorer#createOffsetStorage(int)}); each scorer uses the most suitable storage. E.g. native scorers
 * hand it to native code as is, so they use a native segment.
 */
public abstract class Offsets {

    private int count;

    protected Offsets() {}

    /** Appends {@code position}, which must be greater than every position added since the last {@link #reset()}. */
    public final void add(int position) {
        assert count == 0 || read(count - 1) < position : "position " + position + " is not after " + read(count - 1);
        write(count++, position);
    }

    public final int get(int index) {
        assert index < count : "index " + index + " >= count " + count;
        return read(index);
    }

    public final int count() {
        return count;
    }

    public final void reset() {
        count = 0;
    }

    public abstract MemorySegment getSegment();

    protected abstract int read(int index);

    protected abstract void write(int index, int position);
}
