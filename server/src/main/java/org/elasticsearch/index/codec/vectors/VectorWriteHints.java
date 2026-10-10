/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NoReuseHint;

/**
 * How a format writes the raw vectors it keeps in a flat format, matching {@link VectorReadHints}. The field they hold is
 * said by {@link FieldKnnVectorsFormat}.
 */
public final class VectorWriteHints {

    private VectorWriteHints() {}

    /** Raw vectors kept only to rescore: written sequentially and not reused. */
    public static SegmentWriteState writtenToRescore(SegmentWriteState state) {
        return withHints(state, DataAccessHint.SEQUENTIAL, NoReuseHint.INSTANCE);
    }

    /** Raw vectors searches scan: written sequentially, and read back by searches rather than by the merge writing them. */
    public static SegmentWriteState writtenToScan(SegmentWriteState state) {
        return withHints(state, DataAccessHint.SEQUENTIAL);
    }

    private static SegmentWriteState withHints(SegmentWriteState state, IOContext.FileOpenHint... hints) {
        return new SegmentWriteState(state, state.context.union(hints));
    }
}
