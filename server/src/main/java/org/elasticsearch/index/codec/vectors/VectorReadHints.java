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
 * {@link org.elasticsearch.index.store.FsDirectoryFactory}. A reader's own files are opened for searches even when a merge
 * opens the reader, since Lucene hands that reader to the searches that follow; a merge reads through files its merge
 * instance opens for itself.
 */
public final class VectorReadHints {

    private VectorReadHints() {}

    /** Vectors a graph walks: read at random and reused. */
    public static SegmentReadState walkedByGraph(SegmentReadState state) {
        return forSearches(state, hints(state, DataAccessHint.RANDOM));
    }

    /** Raw vectors kept only to rescore: read at random and not reused. */
    public static SegmentReadState readToRescore(SegmentReadState state) {
        return forSearches(state, hints(state, DataAccessHint.RANDOM, NoReuseHint.INSTANCE));
    }

    /** {@code state} with {@code hints}, as searches open the reader's files. */
    public static SegmentReadState forSearches(SegmentReadState state, IOContext.FileOpenHint... hints) {
        IOContext context = state.context.context() == IOContext.Context.MERGE ? IOContext.DEFAULT : state.context;
        return new SegmentReadState(state, context.withHints(hints));
    }

    private static IOContext.FileOpenHint[] hints(SegmentReadState state, IOContext.FileOpenHint... access) {
        VectorFieldHint field = VectorFieldHint.forSuffix(state.fieldInfos, state.segmentSuffix);
        return Stream.concat(
            Stream.of(FileTypeHint.DATA, FileDataHint.KNN_VECTORS),
            Stream.concat(Stream.of(access), Stream.ofNullable(field))
        ).toArray(IOContext.FileOpenHint[]::new);
    }
}
