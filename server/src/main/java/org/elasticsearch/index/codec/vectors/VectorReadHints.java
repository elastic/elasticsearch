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
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NoReuseHint;
import org.elasticsearch.index.store.VectorFieldHint;

import java.util.stream.Stream;

/**
 * How a format reads the vectors it keeps in a flat format, and which field they hold: the same raw vectors may be walked
 * by a graph or only read to rescore. The directory decides how to open the files from these hints, see
 * {@link org.elasticsearch.index.store.FsDirectoryFactory}.
 */
public final class VectorReadHints {

    private VectorReadHints() {}

    /** Vectors a graph walks: read at random and reused. */
    public static SegmentReadState walkedByGraph(SegmentReadState state) {
        return state.withHints(hints(state, DataAccessHint.RANDOM));
    }

    /** Raw vectors kept only to rescore: read at random and not reused. */
    public static SegmentReadState readToRescore(SegmentReadState state) {
        return state.withHints(hints(state, DataAccessHint.RANDOM, NoReuseHint.INSTANCE));
    }

    private static IOContext.FileOpenHint[] hints(SegmentReadState state, IOContext.FileOpenHint... access) {
        VectorFieldHint field = VectorFieldHint.forSuffix(state.fieldInfos, state.segmentSuffix);
        return Stream.concat(
            Stream.of(FileTypeHint.DATA, FileDataHint.KNN_VECTORS),
            Stream.concat(Stream.of(access), Stream.ofNullable(field))
        ).toArray(IOContext.FileOpenHint[]::new);
    }
}
