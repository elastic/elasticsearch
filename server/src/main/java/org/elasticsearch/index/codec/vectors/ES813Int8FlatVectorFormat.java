/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

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

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.codec.vectors.VectorScoringUtils.scoreAndCollectAll;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

public class ES813Int8FlatVectorFormat extends KnnVectorsFormat {

    static final String NAME = "ES813Int8FlatVectorFormat";

    final FlatVectorsFormat format;

    public ES813Int8FlatVectorFormat() {
        this(null, 7, false);
    }

    /**
     * Sole constructor
     */
    public ES813Int8FlatVectorFormat(Float confidenceInterval, int bits, boolean compress) {
        super(NAME);
        this.format = new ES814ScalarQuantizedVectorsFormat(confidenceInterval, bits, compress);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES813FlatVectorReader(format.fieldsReader(state));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }

    @Override
    public String toString() {
        return NAME + "(name=" + NAME + ", innerFormat=" + format + ")";
    }

    public static class ES813FlatVectorReader extends KnnVectorsReader {

        private final FlatVectorsReader reader;

        public ES813FlatVectorReader(FlatVectorsReader reader) {
            super();
            this.reader = reader;
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
            scoreAndCollectAll(knnCollector, acceptDocs, reader.getRandomVectorScorer(field, target));
        }

        @Override
        public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
            scoreAndCollectAll(knnCollector, acceptDocs, reader.getRandomVectorScorer(field, target));
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
