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
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsWriter;
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

public class ES93ScalarQuantizedVectorsFormat extends KnnVectorsFormat {

    static final String NAME = "ES93ScalarQuantizedVectorsFormat";

    static final Lucene104ScalarQuantizedVectorScorer flatVectorScorer = new Lucene104ScalarQuantizedVectorScorer(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    private final Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding;
    private final FlatVectorsFormat rawVectorFormat;

    public ES93ScalarQuantizedVectorsFormat() {
        this(ES93GenericFlatVectorsFormat.ElementType.STANDARD, Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.SEVEN_BIT);
    }

    public ES93ScalarQuantizedVectorsFormat(ES93GenericFlatVectorsFormat.ElementType elementType) {
        this(elementType, Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.SEVEN_BIT);
    }

    public ES93ScalarQuantizedVectorsFormat(
        ES93GenericFlatVectorsFormat.ElementType elementType,
        Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding
    ) {
        super(NAME);
        assert elementType != ES93GenericFlatVectorsFormat.ElementType.BIT : "BIT should not be used with scalar quantization";
        this.encoding = encoding;
        this.rawVectorFormat = new ES93GenericFlatVectorsFormat(elementType, false);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene104ScalarQuantizedVectorsWriter(state, encoding, rawVectorFormat.fieldsWriter(state), flatVectorScorer) {
        };
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES93FlatVectorsReader(
            new Lucene104ScalarQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state), flatVectorScorer)
        );
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }

    @Override
    public String toString() {
        return NAME
            + "(name="
            + NAME
            + ", encoding="
            + encoding
            + ", flatVectorScorer="
            + flatVectorScorer
            + ", rawVectorFormat="
            + rawVectorFormat
            + ")";
    }

    public static class ES93FlatVectorsReader extends KnnVectorsReader {

        private final FlatVectorsReader reader;

        public ES93FlatVectorsReader(FlatVectorsReader reader) {
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
