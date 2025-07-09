/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
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
import org.elasticsearch.index.store.FsDirectoryFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Copied from Lucene99FlatVectorsFormat in Lucene 10.1
 *
 * This is copied to change the implementation of {@link #fieldsReader} only.
 * The codec format itself is not changed, so we keep the original {@link #NAME}
 */
public class DirectIOLucene99FlatVectorsFormat extends FlatVectorsFormat {

    static final String NAME = "Lucene99FlatVectorsFormat";
    static final String META_CODEC_NAME = "Lucene99FlatVectorsFormatMeta";
    static final String VECTOR_DATA_CODEC_NAME = "Lucene99FlatVectorsFormatData";
    static final String META_EXTENSION = "vemf";
    static final String VECTOR_DATA_EXTENSION = "vec";

    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;

    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;
    private final FlatVectorsScorer vectorsScorer;

    /** Constructs a format */
    public DirectIOLucene99FlatVectorsFormat(FlatVectorsScorer vectorsScorer) {
        super(NAME);
        this.vectorsScorer = vectorsScorer;
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99FlatVectorsWriter(state, vectorsScorer);
    }

    static boolean shouldUseDirectIO(SegmentReadState state) {
        return FsDirectoryFactory.isHybridFs(state.directory);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        if (shouldUseDirectIO(state) && state.context.context() == IOContext.Context.DEFAULT) {
            // only override the context for the random-access use case
            SegmentReadState directIOState = new SegmentReadState(
                state.directory,
                state.segmentInfo,
                state.fieldInfos,
                new DirectIOContext(state.context.hints()),
                state.segmentSuffix
            );
            // Use mmap for merges and direct I/O for searches.
            // TODO: Open the mmap file with sequential access instead of random (current behavior).
            return new MergeReaderWrapper(
                new Lucene99FlatVectorsReader(directIOState, vectorsScorer),
                new Lucene99FlatVectorsReader(state, vectorsScorer)
            );
        } else {
            return new Lucene99FlatVectorsReader(state, vectorsScorer);
        }
    }

    @Override
    public String toString() {
        return "Lucene99FlatVectorsFormat(" + "vectorsScorer=" + vectorsScorer + ')';
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
