/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es818;

import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public class ES818HnswBinaryQuantizedRWVectorsFormat extends ES818HnswBinaryQuantizedVectorsFormat {
    private static final FlatVectorsFormat flatVectorsFormat = new ES818BinaryQuantizedRWVectorsFormat();

    public ES818HnswBinaryQuantizedRWVectorsFormat() {}

    public ES818HnswBinaryQuantizedRWVectorsFormat(int maxConn, int beamWidth) {
        super(maxConn, beamWidth);
    }

    public ES818HnswBinaryQuantizedRWVectorsFormat(int maxConn, int beamWidth, int numMergeWorkers, ExecutorService mergeExec) {
        super(maxConn, beamWidth, numMergeWorkers, mergeExec);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99HnswVectorsWriter(state, maxConn, beamWidth, flatVectorsFormat.fieldsWriter(state), numMergeWorkers, mergeExec);
    }
}
