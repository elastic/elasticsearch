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
 * How a format reads the vectors it keeps in a flat format: the same raw vectors may be walked by a graph or only read to
 * rescore. The directory derives the read advice from these hints, see
 * {@link org.elasticsearch.index.store.FsDirectoryFactory#getReadAdviceFunc()}.
 */
public final class VectorReadHints {

    private VectorReadHints() {}

    /** Vectors a graph walks: read at random and reused. */
    public static SegmentReadState walkedByGraph(SegmentReadState state) {
        return state.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM);
    }

    /** Raw vectors kept only to rescore: read at random and not reused. */
    public static SegmentReadState readToRescore(SegmentReadState state) {
        return state.withHints(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM, NoReuseHint.INSTANCE);
    }
}
