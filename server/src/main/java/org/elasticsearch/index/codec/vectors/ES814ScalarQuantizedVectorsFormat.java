/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;
import java.util.Map;

import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.DYNAMIC_CONFIDENCE_INTERVAL;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

public class ES814ScalarQuantizedVectorsFormat extends FlatVectorsFormat {

    static final String NAME = "ES814ScalarQuantizedVectorsFormat";
    private static final int ALLOWED_BITS = (1 << 8) | (1 << 7) | (1 << 4);

    private static final FlatVectorsFormat rawVectorFormat = new Lucene99FlatVectorsFormat(DefaultFlatVectorScorer.INSTANCE);

    static final FlatVectorsScorer flatVectorScorer = new ESFlatVectorsScorer(
        new ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE)
    );

    /** The minimum confidence interval */
    private static final float MINIMUM_CONFIDENCE_INTERVAL = 0.9f;

    /** The maximum confidence interval */
    private static final float MAXIMUM_CONFIDENCE_INTERVAL = 1f;

    /**
     * Controls the confidence interval used to scalar quantize the vectors the default value is
     * calculated as `1-1/(vector_dimensions + 1)`
     */
    public final Float confidenceInterval;

    private final byte bits;
    private final boolean compress;

    public ES814ScalarQuantizedVectorsFormat(Float confidenceInterval, int bits, boolean compress) {
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
            throw new IllegalArgumentException("bits must be one of: 4, 7, 8; bits=" + bits);
        }
        this.confidenceInterval = confidenceInterval;
        this.bits = (byte) bits;
        this.compress = compress;
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

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES814ScalarQuantizedVectorsWriter(
            new Lucene99ScalarQuantizedVectorsWriter(
                state,
                confidenceInterval,
                bits,
                compress,
                rawVectorFormat.fieldsWriter(state),
                flatVectorScorer
            )
        );
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES814ScalarQuantizedVectorsReader(
            new Lucene99ScalarQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state), flatVectorScorer)
        );
    }

    static final class ES814ScalarQuantizedVectorsWriter extends FlatVectorsWriter {

        final Lucene99ScalarQuantizedVectorsWriter delegate;

        ES814ScalarQuantizedVectorsWriter(Lucene99ScalarQuantizedVectorsWriter delegate) {
            super(delegate.getFlatVectorScorer());
            this.delegate = delegate;
        }

        @Override
        public FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
            return delegate.addField(fieldInfo);
        }

        @Override
        public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
            delegate.mergeOneField(fieldInfo, mergeState);
        }

        @Override
        public CloseableRandomVectorScorerSupplier mergeOneFieldToIndex(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
            return delegate.mergeOneFieldToIndex(fieldInfo, mergeState);
        }

        @Override
        public void finish() throws IOException {
            delegate.finish();
        }

        @Override
        public void flush(int i, Sorter.DocMap docMap) throws IOException {
            delegate.flush(i, docMap);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public long ramBytesUsed() {
            return delegate.ramBytesUsed();
        }
    }

    static final class ES814ScalarQuantizedVectorsReader extends FlatVectorsReader implements QuantizedVectorsReader {

        final Lucene99ScalarQuantizedVectorsReader delegate;

        ES814ScalarQuantizedVectorsReader(Lucene99ScalarQuantizedVectorsReader delegate) {
            super(delegate.getFlatVectorScorer());
            this.delegate = delegate;
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
            return delegate.getRandomVectorScorer(field, target);
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
            return delegate.getRandomVectorScorer(field, target);
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            return delegate.getFloatVectorValues(field);
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            return delegate.getByteVectorValues(field);
        }

        @Override
        public QuantizedByteVectorValues getQuantizedVectorValues(String fieldName) throws IOException {
            return delegate.getQuantizedVectorValues(fieldName);
        }

        @Override
        public ScalarQuantizer getQuantizationState(String fieldName) {
            return delegate.getQuantizationState(fieldName);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public long ramBytesUsed() {
            return delegate.ramBytesUsed();
        }

        @Override
        public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
            return delegate.getOffHeapByteSize(fieldInfo);
        }
    }

    static final class ESFlatVectorsScorer implements FlatVectorsScorer {

        final FlatVectorsScorer delegate;
        final VectorScorerFactory factory;

        ESFlatVectorsScorer(FlatVectorsScorer delegate) {
            this.delegate = delegate;
            factory = VectorScorerFactory.instance().orElse(null);
        }

        @Override
        public String toString() {
            return "ESFlatVectorsScorer(" + "delegate=" + delegate + ", factory=" + factory + ')';
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

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }
}
