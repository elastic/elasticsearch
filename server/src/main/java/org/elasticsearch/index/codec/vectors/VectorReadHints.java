/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.NoReuseHint;

/**
 * How the vectors a format reads through a flat format are read. The format on top says so, because the same raw vectors
 * are walked by a graph in one format and only read to rescore in another. Quantized vectors are never marked as not
 * reused: they are small and every search reads them. The directory decides the read advice from these hints, see
 * {@link org.elasticsearch.index.store.FsDirectoryFactory#getReadAdviceFunc()}.
 */
public final class VectorReadHints {

    private VectorReadHints() {}

    /** Vectors walked by a graph: read at random, and again by every search, so they stay cached. */
    public static SegmentReadState walkedByGraph(SegmentReadState state) {
        return state.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM);
    }

    /** Raw vectors a quantized format keeps only to rescore: read at random, a few at a time, and not reused. */
    public static SegmentReadState readToRescore(SegmentReadState state) {
        return state.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM, NoReuseHint.INSTANCE);
    }
}
