/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.AbstractHnswVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;
import static org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorsFormat.DYNAMIC_CONFIDENCE_INTERVAL;

public class ES93HnswScalarQuantizedVectorsFormat extends AbstractHnswVectorsFormat {

    static final String NAME = "ES93HnswScalarQuantizedVectorsFormat";
    private static final int ALLOWED_BITS = (1 << 7) | (1 << 4);

    /** The minimum confidence interval */
    private static final float MINIMUM_CONFIDENCE_INTERVAL = 0.9f;

    /** The maximum confidence interval */
    private static final float MAXIMUM_CONFIDENCE_INTERVAL = 1f;

    static final FlatVectorsScorer flatVectorScorer = new ES93ScalarQuantizedVectorsFormat.ESQuantizedFlatVectorsScorer(
        new ScalarQuantizedVectorScorer(FlatVectorScorerUtil.getLucene99FlatVectorsScorer())
    );

    private final FlatVectorsFormat rawVectorFormat;

    /**
     * Controls the confidence interval used to scalar quantize the vectors the default value is
     * calculated as `1-1/(vector_dimensions + 1)`
     */
    public final Float confidenceInterval;

    private final byte bits;
    private final boolean compress;

    public ES93HnswScalarQuantizedVectorsFormat() {
        super(NAME);
        this.rawVectorFormat = new ES93GenericFlatVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT, false);
        this.confidenceInterval = null;
        this.bits = 7;
        this.compress = false;
    }

    public ES93HnswScalarQuantizedVectorsFormat(
        int maxConn,
        int beamWidth,
        DenseVectorFieldMapper.ElementType elementType,
        Float confidenceInterval,
        int bits,
        boolean compress,
        boolean useDirectIO
    ) {
        this(maxConn, beamWidth, elementType, confidenceInterval, bits, compress, useDirectIO, DEFAULT_NUM_MERGE_WORKER, null);
    }

    public ES93HnswScalarQuantizedVectorsFormat(
        int maxConn,
        int beamWidth,
        DenseVectorFieldMapper.ElementType elementType,
        Float confidenceInterval,
        int bits,
        boolean compress,
        boolean useDirectIO,
        int numMergeWorkers,
        ExecutorService mergeExec
    ) {
        super(NAME, maxConn, beamWidth, numMergeWorkers, mergeExec);

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
    protected FlatVectorsFormat flatVectorsFormat() {
        return rawVectorFormat;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99HnswVectorsWriter(
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
            ),
            numMergeWorkers,
            mergeExec
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
            + ", flatVectorFormat="
            + rawVectorFormat
            + ")";
    }
}
