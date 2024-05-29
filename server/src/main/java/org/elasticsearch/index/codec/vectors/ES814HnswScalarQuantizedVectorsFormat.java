/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.TaskExecutor;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_NUM_MERGE_WORKER;

public final class ES814HnswScalarQuantizedVectorsFormat extends KnnVectorsFormat {

    static final String NAME = "ES814HnswScalarQuantizedVectorsFormat";

    static final int MAXIMUM_MAX_CONN = 512;
    static final int MAXIMUM_BEAM_WIDTH = 3200;

    private final int maxConn;

    private final int beamWidth;

    /** The format for storing, reading, merging vectors on disk */
    private final ES814ScalarQuantizedVectorsFormat flatVectorsFormat;

    private final int numMergeWorkers;
    private final TaskExecutor mergeExec;

    public ES814HnswScalarQuantizedVectorsFormat() {
        this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, DEFAULT_NUM_MERGE_WORKER, null, null);
    }

    public ES814HnswScalarQuantizedVectorsFormat(
        int maxConn,
        int beamWidth,
        int numMergeWorkers,
        Float confidenceInterval,
        ExecutorService mergeExec
    ) {
        super(NAME);
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
        this.maxConn = maxConn;
        this.beamWidth = beamWidth;
        if (numMergeWorkers > 1 && mergeExec == null) {
            throw new IllegalArgumentException("No executor service passed in when " + numMergeWorkers + " merge workers are requested");
        }
        if (numMergeWorkers == 1 && mergeExec != null) {
            throw new IllegalArgumentException("No executor service is needed as we'll use single thread to merge");
        }
        this.numMergeWorkers = numMergeWorkers;
        if (mergeExec != null) {
            this.mergeExec = new TaskExecutor(mergeExec);
        } else {
            this.mergeExec = null;
        }
        this.flatVectorsFormat = new ES814ScalarQuantizedVectorsFormat(confidenceInterval);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99HnswVectorsWriter(state, maxConn, beamWidth, flatVectorsFormat.fieldsWriter(state), numMergeWorkers, mergeExec);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(state, flatVectorsFormat.fieldsReader(state));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return 1024;
    }

    @Override
    public String toString() {
        return "ES814HnswScalarQuantizedVectorsFormat(name=ES814HnswScalarQuantizedVectorsFormat, maxConn="
            + maxConn
            + ", beamWidth="
            + beamWidth
            + ", flatVectorFormat="
            + flatVectorsFormat
            + ")";
    }
}
