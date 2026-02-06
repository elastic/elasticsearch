/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es94;

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

public class ES94HnswScalarQuantizedVectorsFormat extends AbstractHnswVectorsFormat {

    static final String NAME = "ES94HnswScalarQuantizedVectorsFormat";

    private final FlatVectorsFormat flatVectorFormat;

    public ES94HnswScalarQuantizedVectorsFormat() {
        super(NAME);
        flatVectorFormat = new ES94ScalarQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.FLOAT);
    }

    public ES94HnswScalarQuantizedVectorsFormat(
        int maxConn,
        int beamWidth,
        DenseVectorFieldMapper.ElementType elementType,
        int bits,
        boolean useDirectIO
    ) {
        super(NAME, maxConn, beamWidth);
        flatVectorFormat = new ES94ScalarQuantizedVectorsFormat(elementType, bits, useDirectIO);
    }

    public ES94HnswScalarQuantizedVectorsFormat(
        int maxConn,
        int beamWidth,
        DenseVectorFieldMapper.ElementType elementType,
        int bits,
        boolean useDirectIO,
        int numMergeWorkers,
        ExecutorService mergeExec
    ) {
        super(NAME, maxConn, beamWidth, numMergeWorkers, mergeExec);
        flatVectorFormat = new ES94ScalarQuantizedVectorsFormat(elementType, bits, useDirectIO);
    }

    @Override
    protected FlatVectorsFormat flatVectorsFormat() {
        return flatVectorFormat;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99HnswVectorsWriter(
            state,
            maxConn,
            beamWidth,
            flatVectorFormat.fieldsWriter(state),
            numMergeWorkers,
            mergeExec,
            0
        );
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(state, flatVectorFormat.fieldsReader(state));
    }
}
