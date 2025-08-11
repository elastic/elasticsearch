/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu.codec;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;

/**
 * Codec format for GPU-accelerated vector indexes. This format is designed to
 * leverage GPU processing capabilities for vector search operations.
 */
public class GPUVectorsFormat extends KnnVectorsFormat {

    private static final Logger LOG = LogManager.getLogger(GPUVectorsFormat.class);

    public static final String NAME = "GPUVectorsFormat";
    public static final int VERSION_START = 0;

    static final String LUCENE99_HNSW_META_CODEC_NAME = "Lucene99HnswVectorsFormatMeta";
    static final String LUCENE99_HNSW_VECTOR_INDEX_CODEC_NAME = "Lucene99HnswVectorsFormatIndex";
    static final String LUCENE99_HNSW_META_EXTENSION = "vem";
    static final String LUCENE99_HNSW_VECTOR_INDEX_EXTENSION = "vex";
    static final int LUCENE99_VERSION_CURRENT = VERSION_START;

    static final int DEFAULT_MAX_CONN = 16;
    static final int DEFAULT_BEAM_WIDTH = 100;
    static final int MIN_NUM_VECTORS_FOR_GPU_BUILD = 2;

    private static final FlatVectorsFormat flatVectorsFormat = new Lucene99FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    final CuVSResourceManager cuVSResourceManager;

    public GPUVectorsFormat() {
        this(CuVSResourceManager.pooling());
    }

    public GPUVectorsFormat(CuVSResourceManager cuVSResourceManager) {
        super(NAME);
        this.cuVSResourceManager = cuVSResourceManager;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new GPUToHNSWVectorsWriter(
            cuVSResourceManager,
            state,
            DEFAULT_MAX_CONN,
            DEFAULT_BEAM_WIDTH,
            flatVectorsFormat.fieldsWriter(state)
        );
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new Lucene99HnswVectorsReader(state, flatVectorsFormat.fieldsReader(state));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return 4096;
    }

    @Override
    public String toString() {
        return NAME + "()";
    }
}
