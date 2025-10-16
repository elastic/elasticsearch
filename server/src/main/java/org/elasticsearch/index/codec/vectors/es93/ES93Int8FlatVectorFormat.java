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
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.OrdinalTranslatedKnnCollector;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

public class ES93Int8FlatVectorFormat extends KnnVectorsFormat {

    static final String NAME = "ES93Int8FlatVectorFormat";

    private final FlatVectorsFormat format;

    public ES93Int8FlatVectorFormat() {
        this(false, null, 7, false);
    }

    public ES93Int8FlatVectorFormat(boolean useBFloat16) {
        this(useBFloat16, null, 7, false);
    }

    public ES93Int8FlatVectorFormat(boolean useBFloat16, Float confidenceInterval, int bits, boolean compress) {
        super(NAME);
        this.format = new ES93ScalarQuantizedVectorsFormat(useBFloat16, confidenceInterval, bits, compress, false);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return format.fieldsWriter(state);
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
            collectAllMatchingDocs(knnCollector, acceptDocs, reader.getRandomVectorScorer(field, target));
        }

        private void collectAllMatchingDocs(KnnCollector knnCollector, AcceptDocs acceptDocs, RandomVectorScorer scorer)
            throws IOException {
            OrdinalTranslatedKnnCollector collector = new OrdinalTranslatedKnnCollector(knnCollector, scorer::ordToDoc);
            Bits acceptedOrds = scorer.getAcceptOrds(acceptDocs.bits());
            for (int i = 0; i < scorer.maxOrd(); i++) {
                if (acceptedOrds == null || acceptedOrds.get(i)) {
                    collector.collect(i, scorer.score(i));
                    collector.incVisitedCount(1);
                }
            }
            assert collector.earlyTerminated() == false;
        }

        @Override
        public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
            collectAllMatchingDocs(knnCollector, acceptDocs, reader.getRandomVectorScorer(field, target));
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
