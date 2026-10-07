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
import org.apache.lucene.store.NoReuseHint;

/** How a format writes the raw vectors it keeps in a flat format, matching {@link VectorReadHints}. */
public final class VectorWriteHints {

    private VectorWriteHints() {}

    /** Raw vectors kept only to rescore: written sequentially and not reused. */
    public static SegmentWriteState writtenToRescore(SegmentWriteState state) {
        return new SegmentWriteState(state, state.context.union(DataAccessHint.SEQUENTIAL, NoReuseHint.INSTANCE));
    }
}
