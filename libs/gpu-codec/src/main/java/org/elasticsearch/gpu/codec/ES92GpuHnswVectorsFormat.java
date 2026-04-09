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
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

/**
 * Codec format for GPU-accelerated vector indexes. This format is designed to
 * leverage GPU processing capabilities for vector search operations.
 */
public class ES92GpuHnswVectorsFormat extends KnnVectorsFormat {
    public static final String NAME = "Lucene99HnswVectorsFormat";
    public static final int VERSION_GROUPVARINT = 1;

    static final String LUCENE99_HNSW_META_CODEC_NAME = "Lucene99HnswVectorsFormatMeta";
    static final String LUCENE99_HNSW_VECTOR_INDEX_CODEC_NAME = "Lucene99HnswVectorsFormatIndex";
    static final String LUCENE99_HNSW_META_EXTENSION = "vem";
    static final String LUCENE99_HNSW_VECTOR_INDEX_EXTENSION = "vex";
    static final int LUCENE99_VERSION_CURRENT = VERSION_GROUPVARINT;

    public static final int DEFAULT_MAX_CONN = (2 + Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN * 2 / 3); // graph degree
    public static final int DEFAULT_BEAM_WIDTH = Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN + Lucene99HnswVectorsFormat.DEFAULT_MAX_CONN
        * Lucene99HnswVectorsFormat.DEFAULT_BEAM_WIDTH / 256; // intermediate graph degree
    static final int MIN_NUM_VECTORS_FOR_GPU_BUILD = 2;

    private static final FlatVectorsFormat flatVectorsFormat = new Lucene99FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    // How many nodes each node in the graph is connected to in the final graph
    private final int maxConn;
    // Intermediate graph degree, the number of connections for each node before pruning
    private final int beamWidth;
    private final Supplier<CuVSResourceManager> cuVSResourceManagerSupplier;

    public ES92GpuHnswVectorsFormat() {
        this(CuVSResourceManager::pooling, DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH);
    }

    public ES92GpuHnswVectorsFormat(int maxConn, int beamWidth) {
        this(CuVSResourceManager::pooling, maxConn, beamWidth);
    };

    public ES92GpuHnswVectorsFormat(Supplier<CuVSResourceManager> cuVSResourceManagerSupplier, int maxConn, int beamWidth) {
        super(NAME);
        this.cuVSResourceManagerSupplier = cuVSResourceManagerSupplier;
        this.maxConn = maxConn;
        this.beamWidth = beamWidth;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES92GpuHnswVectorsWriter(
            cuVSResourceManagerSupplier.get(),
            state,
            maxConn,
            beamWidth,
            flatVectorsFormat.fieldsWriter(state)
        );
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(state, flatVectorsFormat.fieldsReader(state));
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
            + ", flatVectorFormat="
            + flatVectorsFormat.getName()
            + ")";
    }
}
