/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
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

import java.io.IOException;

public class ES813Int8FlatVectorFormat extends KnnVectorsFormat {

    static final String NAME = "ES813Int8FlatVectorFormat";

    private final FlatVectorsFormat format;

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
        return new ES813FlatVectorWriter(format.fieldsWriter(state));
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES813FlatVectorReader(format.fieldsReader(state));
    }

    @Override
    public String toString() {
        return NAME + "(name=" + NAME + ", innerFormat=" + format + ")";
    }

    public static class ES813FlatVectorWriter extends KnnVectorsWriter {

        private final FlatVectorsWriter writer;

        public ES813FlatVectorWriter(FlatVectorsWriter writer) {
            super();
            this.writer = writer;
        }

        @Override
        public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
            return writer.addField(fieldInfo, null);
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
        public long ramBytesUsed() {
            return reader.ramBytesUsed();
        }

    }
}
