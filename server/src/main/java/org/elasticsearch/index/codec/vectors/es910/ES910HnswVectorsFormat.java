/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors.es910;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.hnsw.HnswGraph;

import java.io.IOException;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.MAXIMUM_BEAM_WIDTH;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat.MAXIMUM_MAX_CONN;

/**
 * Copied from Lucene, replace with Lucene's implementation sometime after Lucene 10.3.0
 */
public class ES910HnswVectorsFormat extends KnnVectorsFormat {

    static final String NAME = "ES910HnswVectorsFormat";

    static final String META_CODEC_NAME = "Lucene99HnswVectorsFormatMeta";
    static final String VECTOR_INDEX_CODEC_NAME = "Lucene99HnswVectorsFormatIndex";
    static final String META_EXTENSION = "vem";
    static final String VECTOR_INDEX_EXTENSION = "vex";
    static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

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

    /** The format for storing, reading, and merging vectors on disk. */
    private static final FlatVectorsFormat flatVectorsFormat = new Lucene99FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    /** Constructs a format using default graph construction parameters */
    public ES910HnswVectorsFormat() {
        this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);
    }

    /**
     * Constructs a format using the given graph construction parameters.
     *
     * @param maxConn the maximum number of connections to a node in the HNSW graph
     * @param beamWidth the size of the queue maintained during graph construction.
     */
    public ES910HnswVectorsFormat(int maxConn, int beamWidth) {
        super(ES910HnswVectorsFormat.NAME);
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
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES910HnswVectorsWriter(state, maxConn, beamWidth, flatVectorsFormat.fieldsWriter(state));
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(state, flatVectorsFormat.fieldsReader(state));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return Lucene99HnswVectorsFormat.DEFAULT_MAX_DIMENSIONS;
    }

    @Override
    public String toString() {
        return "ES910HnswReducedHeapVectorsFormat(name=ES910HnswReducedHeapVectorsFormat, maxConn="
            + maxConn
            + ", beamWidth="
            + beamWidth
            + ", flatVectorFormat="
            + flatVectorsFormat
            + ")";
    }
}
