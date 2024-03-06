/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors.codec;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.util.hnsw.HnswGraph;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

public final class ESLucene99HnswScalarQuantizedVectorsFormat extends KnnVectorsFormat {

    static final int MAXIMUM_MAX_CONN = 512;
    static final int MAXIMUM_BEAM_WIDTH = 3200;

    /**
     * Controls how many of the nearest neighbor candidates are connected to the new node. Defaults to
     * {@link Lucene99HnswVectorsFormat#DEFAULT_MAX_CONN}. See {@link HnswGraph} for more details.
     */
    private final int maxConn;

    /**
     * The number of candidate neighbors to track while searching the graph for each newly inserted
     * node. Defaults to {@link Lucene99HnswVectorsFormat#DEFAULT_BEAM_WIDTH}. See {@link HnswGraph}
     * for details.
     */
    private final int beamWidth;

    /** The format for storing, reading, merging vectors on disk */
    private final ESLucene99ScalarQuantizedVectorsFormat flatVectorsFormat;

    private final int numMergeWorkers;
    private final TaskExecutor mergeExec;

    public ESLucene99HnswScalarQuantizedVectorsFormat(
        int maxConn,
        int beamWidth,
        int numMergeWorkers,
        Float confidenceInterval,
        ExecutorService mergeExec
    ) {
        super("Lucene99HnswScalarQuantizedVectorsFormat");  // TODO: rename to ESXXXXX
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
        this.flatVectorsFormat = new ESLucene99ScalarQuantizedVectorsFormat(confidenceInterval);
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
        return "Lucene99HnswScalarQuantizedVectorsFormat(name=Lucene99HnswScalarQuantizedVectorsFormat, maxConn="
            + maxConn
            + ", beamWidth="
            + beamWidth
            + ", flatVectorFormat="
            + flatVectorsFormat
            + ")";
    }
}
