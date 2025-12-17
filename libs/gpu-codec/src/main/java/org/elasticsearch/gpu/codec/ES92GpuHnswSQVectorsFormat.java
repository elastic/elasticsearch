/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gpu.codec;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.es93.ES93GenericFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93ScalarQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.function.Supplier;

import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.DYNAMIC_CONFIDENCE_INTERVAL;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

/**
 * Codec format for GPU-accelerated scalar quantized HNSW vector indexes.
 * HNSW graph is built on GPU, while scalar quantization and search is performed on CPU.
 */
public class ES92GpuHnswSQVectorsFormat extends KnnVectorsFormat {
    public static final String NAME = "Lucene99HnswVectorsFormat";
    static final int MAXIMUM_MAX_CONN = 512;
    static final int MAXIMUM_BEAM_WIDTH = 3200;
    private static final int ALLOWED_BITS = (1 << 7) | (1 << 4);

    /** The minimum confidence interval */
    private static final float MINIMUM_CONFIDENCE_INTERVAL = 0.9f;

    /** The maximum confidence interval */
    private static final float MAXIMUM_CONFIDENCE_INTERVAL = 1f;

    static final FlatVectorsScorer flatVectorScorer = ES93ScalarQuantizedVectorsFormat.flatVectorScorer;

    private final int maxConn;
    private final int beamWidth;

    /** The format for storing, reading, merging vectors on disk */
    private final FlatVectorsFormat rawVectorFormat;

    /**
     * Controls the confidence interval used to scalar quantize the vectors the default value is
     * calculated as `1-1/(vector_dimensions + 1)`
     */
    private final Float confidenceInterval;

    private final byte bits;
    private final boolean compress;
    private final Supplier<CuVSResourceManager> cuVSResourceManagerSupplier;

    public ES92GpuHnswSQVectorsFormat() {
        this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, null, 7, false);
    }

    public ES92GpuHnswSQVectorsFormat(int maxConn, int beamWidth, Float confidenceInterval, int bits, boolean compress) {
        super(NAME);
        this.cuVSResourceManagerSupplier = CuVSResourceManager::pooling;
        if (maxConn <= 0 || maxConn > MAXIMUM_MAX_CONN) {
            throw new IllegalArgumentException(
                "maxConn must be positive and less than or equal to " + MAXIMUM_MAX_CONN + "; maxConn=" + maxConn
            );
        }
        if (beamWidth <= 0 || beamWidth > MAXIMUM_BEAM_WIDTH) {
            throw new IllegalArgumentException(
                "beamWidth must be positive and less than or equal to " + MAXIMUM_BEAM_WIDTH + "; beamWidth=" + beamWidth
            );
        }
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
        this.maxConn = maxConn;
        this.beamWidth = beamWidth;
        this.confidenceInterval = confidenceInterval;
        this.bits = (byte) bits;
        this.compress = compress;
        this.rawVectorFormat = new ES93GenericFlatVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, false);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES92GpuHnswVectorsWriter(
            cuVSResourceManagerSupplier.get(),
            state,
            maxConn,
            beamWidth,
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
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(
            state,
            new Lucene99ScalarQuantizedVectorsReader(state, rawVectorFormat.fieldsReader(state), flatVectorScorer)
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
            + ", maxConn="
            + maxConn
            + ", beamWidth="
            + beamWidth
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
}
