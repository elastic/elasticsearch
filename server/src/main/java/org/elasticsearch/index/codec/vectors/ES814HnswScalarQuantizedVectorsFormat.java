/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;

public class ES814HnswScalarQuantizedVectorsFormat extends AbstractHnswVectorsFormat {

    static final String NAME = "ES814HnswScalarQuantizedVectorsFormat";

    /** The format for storing, reading, merging vectors on disk */
    final FlatVectorsFormat flatVectorsFormat;

    public ES814HnswScalarQuantizedVectorsFormat() {
        this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, null, 7, false);
    }

    public ES814HnswScalarQuantizedVectorsFormat(int maxConn, int beamWidth, Float confidenceInterval, int bits, boolean compress) {
        this(maxConn, beamWidth, confidenceInterval, bits, compress, 1, null);
    }

    public ES814HnswScalarQuantizedVectorsFormat(
        int maxConn,
        int beamWidth,
        Float confidenceInterval,
        int bits,
        boolean compress,
        int numMergeWorkers,
        ExecutorService mergeExec
    ) {
        super(NAME, maxConn, beamWidth, numMergeWorkers, mergeExec);
        this.flatVectorsFormat = new ES814ScalarQuantizedVectorsFormat(confidenceInterval, bits, compress);
    }

    @Override
    protected FlatVectorsFormat flatVectorsFormat() {
        return flatVectorsFormat;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(state, flatVectorsFormat.fieldsReader(state));
    }
}
