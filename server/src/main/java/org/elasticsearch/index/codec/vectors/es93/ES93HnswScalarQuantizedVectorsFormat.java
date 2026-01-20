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
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.AbstractHnswVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class ES93HnswScalarQuantizedVectorsFormat extends AbstractHnswVectorsFormat {

    static final String NAME = "ES93HnswScalarQuantizedVectorsFormat";

    private final FlatVectorsFormat flatVectorFormat;

    public ES93HnswScalarQuantizedVectorsFormat() {
        super(NAME);
        flatVectorFormat = new ES93ScalarQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT);
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
        super(NAME, maxConn, beamWidth);
        flatVectorFormat = new ES93ScalarQuantizedVectorsFormat(elementType, confidenceInterval, bits, compress, useDirectIO);
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
        flatVectorFormat = new ES93ScalarQuantizedVectorsFormat(elementType, confidenceInterval, bits, compress, useDirectIO);
    }

    @Override
    protected FlatVectorsFormat flatVectorsFormat() {
        return flatVectorFormat;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99HnswVectorsWriter(state, maxConn, beamWidth, flatVectorFormat.fieldsWriter(state), numMergeWorkers, mergeExec);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(state, flatVectorFormat.fieldsReader(state));
    }
}
