/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.codec.vectors.VectorReadHints;
import org.elasticsearch.index.codec.vectors.VectorWriteHints;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.store.VectorFieldHint;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.index.codec.vectors.VectorScoringUtils.scoreAndCollectAll;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

public class ES93FlatVectorFormat extends KnnVectorsFormat {

    static final String NAME = "ES93FlatVectorFormat";

    private final FlatVectorsFormat format;

    /**
     * Sole constructor
     */
    public ES93FlatVectorFormat() {
        super(NAME);
        format = new ES93GenericFlatVectorsFormat();
    }

    public ES93FlatVectorFormat(DenseVectorFieldMapper.ElementType elementType) {
        super(NAME);
        format = new ES93GenericFlatVectorsFormat(elementType);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return format.fieldsWriter(VectorWriteHints.writtenToScan(state));
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        SegmentReadState searchState = VectorReadHints.forSearches(state, state.context.hints().toArray(IOContext.FileOpenHint[]::new));
        return new ES93FlatVectorReader(format, searchState, format.fieldsReader(searchState));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }

    @Override
    public String toString() {
        return getName() + "(name=" + getName() + ", innerFormat=" + format + ")";
    }

    static class ES93FlatVectorReader extends KnnVectorsReader {

        private final FlatVectorsFormat format;
        private final SegmentReadState state;
        private final FlatVectorsReader reader;
        // the search reader a merge instance comes from
        private final ES93FlatVectorReader original;

        ES93FlatVectorReader(FlatVectorsFormat format, SegmentReadState state, FlatVectorsReader reader) {
            this.format = format;
            this.state = state;
            this.reader = reader;
            this.original = this;
        }

        /** A merge instance of {@code original}, reading through {@code reader}, which it closes when the merge finishes. */
        private ES93FlatVectorReader(ES93FlatVectorReader original, FlatVectorsReader reader) {
            this.format = original.format;
            this.state = original.state;
            this.reader = reader;
            this.original = original;
        }

        /**
         * A merge streams the vectors that searches scan, so it opens them for itself, reading them sequentially: the
         * directory then decides how a merge reads the field's vectors. A merge reads through this reader instead when the
         * files hold several fields, which no one mapping applies to, or when they are gone, since an open reader outlives
         * them.
         */
        @Override
        public KnnVectorsReader getMergeInstance() throws IOException {
            assert original == this : "a merge instance is not merged";
            VectorFieldHint field = VectorFieldHint.forSuffix(state.fieldInfos, state.segmentSuffix);
            if (field == null) {
                return this;
            }
            IOContext.FileOpenHint[] hints = Stream.concat(
                state.context.hints().stream().filter(hint -> hint instanceof DataAccessHint == false),
                Stream.of(DataAccessHint.SEQUENTIAL, field)
            ).toArray(IOContext.FileOpenHint[]::new);
            try {
                return new ES93FlatVectorReader(this, format.fieldsReader(new SegmentReadState(state, IOContext.merge().withHints(hints))));
            } catch (FileNotFoundException | NoSuchFileException e) {
                return this;
            }
        }

        /** Closes the reader this merge instance opened; a no-op on the search reader. */
        @Override
        public void finishMerge() throws IOException {
            if (original != this) {
                reader.close();
            }
        }

        @Override
        public void checkIntegrity() throws IOException {
            reader.checkIntegrity();
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            return reader.getFloatVectorValues(field);
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            return reader.getByteVectorValues(field);
        }

        @Override
        public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
            scoreAndCollectAll(knnCollector, acceptDocs, reader.getFloatVectorValues(field).scorer(target));
        }

        @Override
        public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
            scoreAndCollectAll(knnCollector, acceptDocs, reader.getByteVectorValues(field).scorer(target));
        }

        @Override
        public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
            return reader.getOffHeapByteSize(fieldInfo);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
