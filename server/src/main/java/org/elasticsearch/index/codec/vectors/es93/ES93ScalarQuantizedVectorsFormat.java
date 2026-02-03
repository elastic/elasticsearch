/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;
import java.util.Map;

import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.DYNAMIC_CONFIDENCE_INTERVAL;
import static org.elasticsearch.index.codec.vectors.VectorScoringUtils.scoreAndCollectAll;

public class ES93ScalarQuantizedVectorsFormat extends FlatVectorsFormat {

    static final String NAME = "ES93ScalarQuantizedVectorsFormat";
    private static final int ALLOWED_BITS = (1 << 7) | (1 << 4);

    static final FlatVectorsScorer flatVectorScorer = new ESQuantizedFlatVectorsScorer(
        new ScalarQuantizedVectorScorer(ES93FlatVectorScorer.INSTANCE)
    );

    /** The minimum confidence interval */
    private static final float MINIMUM_CONFIDENCE_INTERVAL = 0.9f;

    /** The maximum confidence interval */
    private static final float MAXIMUM_CONFIDENCE_INTERVAL = 1f;

    private final FlatVectorsFormat rawVectorFormat;

    /**
     * Controls the confidence interval used to scalar quantize the vectors the default value is
     * calculated as `1-1/(vector_dimensions + 1)`
     */
    public final Float confidenceInterval;

    private final byte bits;
    private final boolean compress;

    public ES93ScalarQuantizedVectorsFormat() {
        this(DenseVectorFieldMapper.ElementType.FLOAT, null, 7, false, false);
    }

    public ES93ScalarQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType elementType) {
        this(elementType, null, 7, false, false);
    }

    public ES93ScalarQuantizedVectorsFormat(
        DenseVectorFieldMapper.ElementType elementType,
        Float confidenceInterval,
        int bits,
        boolean compress,
        boolean useDirectIO
    ) {
        super(NAME);
        if (confidenceInterval != null
            && confidenceInterval != DYNAMIC_CONFIDENCE_INTERVAL
            && (confidenceInterval < MINIMUM_CONFIDENCE_INTERVAL || confidenceInterval > MAXIMUM_CONFIDENCE_INTERVAL)) {
            throw new IllegalArgumentException(
                "confidenceInterval must be between "
                    + MINIMUM_CONFIDENCE_INTERVAL
                    + " and "
                    + MAXIMUM_CONFIDENCE_INTERVAL
                    + "; confidenceInterval="
                    + confidenceInterval
            );
        }
        if (bits < 1 || bits > 8 || (ALLOWED_BITS & (1 << bits)) == 0) {
            throw new IllegalArgumentException("bits must be one of: 4, 7; bits=" + bits);
        }
        assert elementType != DenseVectorFieldMapper.ElementType.BIT : "BIT should not be used with scalar quantization";

        this.rawVectorFormat = new ES93GenericFlatVectorsFormat(elementType, useDirectIO);
        this.confidenceInterval = confidenceInterval;
        this.bits = (byte) bits;
        this.compress = compress;
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99ScalarQuantizedVectorsWriter(
            state,
            confidenceInterval,
            bits,
            compress,
            rawVectorFormat.fieldsWriter(state),
            flatVectorScorer
        );
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES93FlatVectorReader(
            new Lucene99ScalarQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state), flatVectorScorer)
        );
    }

    @Override
    public String toString() {
        return NAME
            + "(name="
            + NAME
            + ", confidenceInterval="
            + confidenceInterval
            + ", bits="
            + bits
            + ", compressed="
            + compress
            + ", flatVectorScorer="
            + flatVectorScorer
            + ", rawVectorFormat="
            + rawVectorFormat
            + ")";
    }

    private static class ES93FlatVectorReader extends FlatVectorsReader implements QuantizedVectorsReader {

        private final Lucene99ScalarQuantizedVectorsReader reader;

        private ES93FlatVectorReader(Lucene99ScalarQuantizedVectorsReader reader) {
            super(reader.getFlatVectorScorer());
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
        public FlatVectorsScorer getFlatVectorScorer() {
            return reader.getFlatVectorScorer();
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
        public void finishMerge() throws IOException {
            reader.finishMerge();
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
            return reader.getRandomVectorScorer(field, target);
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
            return reader.getRandomVectorScorer(field, target);
        }

        @Override
        public FlatVectorsReader getMergeInstance() throws IOException {
            return reader.getMergeInstance();
        }

        @Override
        public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
            return reader.getOffHeapByteSize(fieldInfo);
        }

        @Override
        public long ramBytesUsed() {
            return reader.ramBytesUsed();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public QuantizedByteVectorValues getQuantizedVectorValues(String fieldName) throws IOException {
            return reader.getQuantizedVectorValues(fieldName);
        }

        @Override
        public ScalarQuantizer getQuantizationState(String fieldName) {
            return reader.getQuantizationState(fieldName);
        }
    }

    private static final class ESQuantizedFlatVectorsScorer implements FlatVectorsScorer {

        final FlatVectorsScorer delegate;
        final VectorScorerFactory factory;

        private ESQuantizedFlatVectorsScorer(FlatVectorsScorer delegate) {
            this.delegate = delegate;
            factory = VectorScorerFactory.instance().orElse(null);
        }

        @Override
        public String toString() {
            return "ESQuantizedFlatVectorsScorer(" + "delegate=" + delegate + ", factory=" + factory + ')';
        }

        @Override
        public RandomVectorScorerSupplier getRandomVectorScorerSupplier(VectorSimilarityFunction sim, KnnVectorValues values)
            throws IOException {
            if (values instanceof QuantizedByteVectorValues qValues && qValues.getSlice() != null) {
                // TODO: optimize int4 quantization
                if (qValues.getScalarQuantizer().getBits() != 7) {
                    return delegate.getRandomVectorScorerSupplier(sim, values);
                }
                if (factory != null) {
                    var scorer = factory.getInt7SQVectorScorerSupplier(
                        VectorSimilarityType.of(sim),
                        qValues.getSlice(),
                        qValues,
                        qValues.getScalarQuantizer().getConstantMultiplier()
                    );
                    if (scorer.isPresent()) {
                        return scorer.get();
                    }
                }
            }
            return delegate.getRandomVectorScorerSupplier(sim, values);
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(VectorSimilarityFunction sim, KnnVectorValues values, float[] query)
            throws IOException {
            if (values instanceof QuantizedByteVectorValues qValues && qValues.getSlice() != null) {
                // TODO: optimize int4 quantization
                if (qValues.getScalarQuantizer().getBits() != 7) {
                    return delegate.getRandomVectorScorer(sim, values, query);
                }
                if (factory != null) {
                    var scorer = factory.getInt7SQVectorScorer(sim, qValues, query);
                    if (scorer.isPresent()) {
                        return scorer.get();
                    }
                }
            }
            return delegate.getRandomVectorScorer(sim, values, query);
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(VectorSimilarityFunction sim, KnnVectorValues values, byte[] query)
            throws IOException {
            return delegate.getRandomVectorScorer(sim, values, query);
        }
    }
}
