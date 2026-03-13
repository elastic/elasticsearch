/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es94;

import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsWriter;
import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorScorer;
import org.elasticsearch.index.codec.vectors.es93.ES93GenericFlatVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.simdvec.VectorScorerFactory;
import org.elasticsearch.simdvec.VectorSimilarityType;

import java.io.IOException;

public class ES94ScalarQuantizedVectorsFormat extends FlatVectorsFormat {

    static final String NAME = "ES94ScalarQuantizedVectorsFormat";
    private static final int ALLOWED_BITS = (1 << 7) | (1 << 4) | (1 << 2) | (1 << 1);

    static final Lucene104ScalarQuantizedVectorScorer flatVectorScorer = new ESQuantizedFlatVectorsScorer(ES93FlatVectorScorer.INSTANCE);
    private final FlatVectorsFormat rawVectorFormat;
    private final Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding encoding;

    public ES94ScalarQuantizedVectorsFormat() {
        this(DenseVectorFieldMapper.ElementType.FLOAT, 7, false);
    }

    public ES94ScalarQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType elementType) {
        this(elementType, 7, false);
    }

    public ES94ScalarQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType elementType, int bits, boolean useDirectIO) {
        super(NAME);
        if (bits < 1 || bits > 8 || (ALLOWED_BITS & (1 << bits)) == 0) {
            throw new IllegalArgumentException("bits must be one of: 1, 2, 4, 7; bits=" + bits);
        }
        assert elementType != DenseVectorFieldMapper.ElementType.BIT : "BIT should not be used with scalar quantization";

        this.rawVectorFormat = new ES93GenericFlatVectorsFormat(elementType, useDirectIO);
        this.encoding = switch (bits) {
            case 1 -> Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.SINGLE_BIT_QUERY_NIBBLE;
            case 2 -> Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.DIBIT_QUERY_NIBBLE;
            case 4 -> Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.PACKED_NIBBLE;
            case 7 -> Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.SEVEN_BIT;
            default -> throw new IllegalArgumentException("bits must be one of: 1, 2, 4, 7; bits=" + bits);
        };
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene104ScalarQuantizedVectorsWriter(state, encoding, rawVectorFormat.fieldsWriter(state), flatVectorScorer);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene104ScalarQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state), flatVectorScorer);
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

    static final class ESQuantizedFlatVectorsScorer extends Lucene104ScalarQuantizedVectorScorer {

        final FlatVectorsScorer delegate;
        final VectorScorerFactory factory;

        ESQuantizedFlatVectorsScorer(FlatVectorsScorer delegate) {
            super(delegate);
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
            if (values instanceof QuantizedByteVectorValues quantizedValues && quantizedValues.getSlice() != null) {
                // TODO: optimize int4, 2, and single bit quantization
                if (quantizedValues.getScalarEncoding() != Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.SEVEN_BIT) {
                    return super.getRandomVectorScorerSupplier(sim, values);
                }
                if (factory != null) {
                    var scorer = factory.getInt7uOSQVectorScorerSupplier(
                        VectorSimilarityType.of(sim),
                        quantizedValues.getSlice(),
                        quantizedValues
                    );
                    if (scorer.isPresent()) {
                        return scorer.get();
                    }
                }
            }
            return super.getRandomVectorScorerSupplier(sim, values);
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(VectorSimilarityFunction sim, KnnVectorValues values, float[] query)
            throws IOException {
            if (values instanceof QuantizedByteVectorValues quantizedValues && quantizedValues.getSlice() != null) {
                // TODO: optimize int4 quantization
                if (quantizedValues.getScalarEncoding() != Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.SEVEN_BIT) {
                    return super.getRandomVectorScorer(sim, values, query);
                }
                if (factory != null) {
                    OptimizedScalarQuantizer scalarQuantizer = new OptimizedScalarQuantizer(sim);
                    float[] residualScratch = new float[query.length];
                    int[] quantizedQuery = new int[query.length];
                    var correctiveComponents = scalarQuantizer.scalarQuantize(
                        query,
                        residualScratch,
                        quantizedQuery,
                        quantizedValues.getScalarEncoding().getQueryBits(),
                        quantizedValues.getCentroid()
                    );
                    byte[] quantizedQueryBytes = new byte[quantizedQuery.length];
                    for (int i = 0; i < quantizedQuery.length; i++) {
                        quantizedQueryBytes[i] = (byte) quantizedQuery[i];
                    }

                    var scorer = factory.getInt7uOSQVectorScorer(
                        sim,
                        quantizedValues,
                        quantizedQueryBytes,
                        correctiveComponents.lowerInterval(),
                        correctiveComponents.upperInterval(),
                        correctiveComponents.additionalCorrection(),
                        correctiveComponents.quantizedComponentSum()
                    );
                    if (scorer.isPresent()) {
                        return scorer.get();
                    }
                }
            }
            return super.getRandomVectorScorer(sim, values, query);
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(VectorSimilarityFunction sim, KnnVectorValues values, byte[] query)
            throws IOException {
            return super.getRandomVectorScorer(sim, values, query);
        }

        @Override
        public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
            VectorSimilarityFunction similarityFunction,
            QuantizedByteVectorValues scoringVectors,
            QuantizedByteVectorValues targetVectors
        ) {
            // TODO improve merge-times for HNSW through off-heap optimized search
            return super.getRandomVectorScorerSupplier(similarityFunction, scoringVectors, targetVectors);
        }

    }
}
