/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MergeInfo;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.codec.vectors.DirectIOCapableFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.MergeReaderWrapper;
import org.elasticsearch.index.codec.vectors.es818.DirectIOHint;
import org.elasticsearch.index.store.FsDirectoryFactory;

import java.io.IOException;
import java.util.Set;

public class DirectIOCapableLucene99FlatVectorsFormat extends DirectIOCapableFlatVectorsFormat {

    static final String NAME = "Lucene99FlatVectorsFormat";

    private final FlatVectorsScorer vectorsScorer;

    /** Constructs a format */
    public DirectIOCapableLucene99FlatVectorsFormat(FlatVectorsScorer vectorsScorer) {
        super(NAME);
        this.vectorsScorer = vectorsScorer;
    }

    @Override
    protected FlatVectorsScorer flatVectorsScorer() {
        return vectorsScorer;
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99FlatVectorsWriter(state, vectorsScorer);
    }

    static boolean canUseDirectIO(SegmentReadState state) {
        return FsDirectoryFactory.isHybridFs(state.directory);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return fieldsReader(state, false);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state, boolean useDirectIO) throws IOException {
        if (state.context.context() == IOContext.Context.DEFAULT && useDirectIO && canUseDirectIO(state)) {
            // only override the context for the random-access use case
            SegmentReadState directIOState = new SegmentReadState(
                state.directory,
                state.segmentInfo,
                state.fieldInfos,
                new DirectIOContext(state.context.hints()),
                state.segmentSuffix
            );
            // Use mmap for merges and direct I/O for searches.
            return new MergeReaderWrapper(
                new Lucene99FlatVectorsReader(directIOState, vectorsScorer),
                new Lucene99FlatVectorsReader(state, vectorsScorer)
            );
        } else {
            return new Lucene99FlatVectorsReader(state, vectorsScorer);
        }
    }

    static class DirectIOContext implements IOContext {

        final Set<FileOpenHint> hints;

        DirectIOContext(Set<FileOpenHint> hints) {
            // always add DirectIOHint to the hints given
            this.hints = Sets.union(hints, Set.of(DirectIOHint.INSTANCE));
        }

        @Override
        public Context context() {
            return Context.DEFAULT;
        }

        @Override
        public MergeInfo mergeInfo() {
            return null;
        }

        @Override
        public FlushInfo flushInfo() {
            return null;
        }

        @Override
        public Set<FileOpenHint> hints() {
            return hints;
        }

        @Override
        public IOContext withHints(FileOpenHint... hints) {
            return new DirectIOContext(Set.of(hints));
        }
    }
}
