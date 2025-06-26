/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.OrdinalTranslatedKnnCollector;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.index.codec.vectors.reflect.OffHeapByteSizeUtils;
import org.elasticsearch.index.codec.vectors.reflect.OffHeapStats;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

public class ES813FlatVectorFormat extends KnnVectorsFormat {

    static final String NAME = "ES813FlatVectorFormat";

    private static final FlatVectorsFormat format = new Lucene99FlatVectorsFormat(DefaultFlatVectorScorer.INSTANCE);

    /**
     * Sole constructor
     */
    public ES813FlatVectorFormat() {
        super(NAME);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES813FlatVectorWriter(format.fieldsWriter(state));
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES813FlatVectorReader(format.fieldsReader(state));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }

    static class ES813FlatVectorWriter extends KnnVectorsWriter {

        private final FlatVectorsWriter writer;

        ES813FlatVectorWriter(FlatVectorsWriter writer) {
            super();
            this.writer = writer;
        }

        @Override
        public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
            return writer.addField(fieldInfo);
        }

        @Override
        public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
            writer.flush(maxDoc, sortMap);
        }

        @Override
        public void finish() throws IOException {
            writer.finish();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }

        @Override
        public long ramBytesUsed() {
            return writer.ramBytesUsed();
        }

        @Override
        public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
            writer.mergeOneField(fieldInfo, mergeState);
        }
    }

    static class ES813FlatVectorReader extends KnnVectorsReader implements OffHeapStats {

        private final FlatVectorsReader reader;

        ES813FlatVectorReader(FlatVectorsReader reader) {
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
        public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
            collectAllMatchingDocs(knnCollector, acceptDocs, reader.getRandomVectorScorer(field, target));
        }

        private void collectAllMatchingDocs(KnnCollector knnCollector, Bits acceptDocs, RandomVectorScorer scorer) throws IOException {
            OrdinalTranslatedKnnCollector collector = new OrdinalTranslatedKnnCollector(knnCollector, scorer::ordToDoc);
            Bits acceptedOrds = scorer.getAcceptOrds(acceptDocs);
            for (int i = 0; i < scorer.maxOrd(); i++) {
                if (acceptedOrds == null || acceptedOrds.get(i)) {
                    collector.collect(i, scorer.score(i));
                    collector.incVisitedCount(1);
                }
            }
            assert collector.earlyTerminated() == false;
        }

        @Override
        public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) throws IOException {
            collectAllMatchingDocs(knnCollector, acceptDocs, reader.getRandomVectorScorer(field, target));
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
            return OffHeapByteSizeUtils.getOffHeapByteSize(reader, fieldInfo);
        }
    }
}
