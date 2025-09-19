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
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.ES814ScalarQuantizedVectorsFormat;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;
import static org.elasticsearch.xpack.gpu.codec.ESGpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.elasticsearch.xpack.gpu.codec.ESGpuHnswVectorsFormat.DEFAULT_MAX_CONN;

/**
 * Codec format for GPU-accelerated scalar quantized HNSW vector indexes.
 * HNSW graph is built on GPU, while scalar quantization and search is performed on CPU.
 */
public class ESGpuHnswSQVectorsFormat extends KnnVectorsFormat {
    public static final String NAME = "ESGPUHnswScalarQuantizedVectorsFormat";
    static final int MAXIMUM_MAX_CONN = 512;
    static final int MAXIMUM_BEAM_WIDTH = 3200;
    private final int maxConn;
    private final int beamWidth;

    /** The format for storing, reading, merging vectors on disk */
    private final FlatVectorsFormat flatVectorsFormat;
    private final Supplier<CuVSResourceManager> cuVSResourceManagerSupplier;

    public ESGpuHnswSQVectorsFormat() {
        this(DEFAULT_MAX_CONN, DEFAULT_BEAM_WIDTH, null, 7, false);
    }

    public ESGpuHnswSQVectorsFormat(int maxConn, int beamWidth, Float confidenceInterval, int bits, boolean compress) {
        super(NAME);
        this.cuVSResourceManagerSupplier = CuVSResourceManager::pooling;
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
        this.flatVectorsFormat = new ES814ScalarQuantizedVectorsFormat(confidenceInterval, bits, compress);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ESGpuHnswVectorsWriter(
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
            + flatVectorsFormat
            + ")";
    }
}
