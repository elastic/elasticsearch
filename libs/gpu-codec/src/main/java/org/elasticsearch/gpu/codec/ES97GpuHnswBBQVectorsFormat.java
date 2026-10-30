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
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.gpu.CuVSGPUSupport;
import org.elasticsearch.index.codec.vectors.es93.ES93BinaryQuantizedVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.function.Supplier;

import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_BEAM_WIDTH;
import static org.elasticsearch.gpu.codec.ES92GpuHnswVectorsFormat.DEFAULT_MAX_CONN;
import static org.elasticsearch.index.codec.vectors.es93.ES93HnswBinaryQuantizedVectorsFormat.BBQ_HNSW_GRAPH_THRESHOLD;
import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

/**
 * Codec format for GPU-accelerated binary quantized (BBQ) HNSW vector indexes.
 * <p>
 * The HNSW graph is built on GPU, while binary quantization and search are performed on CPU.
 * The on-disk format is identical to {@link ES93HnswBinaryQuantizedVectorsFormat}, and the format reuses its name,
 * so segments written on a GPU node are read (and merged) by the CPU format on nodes without a GPU.
 * <p>
 * Unlike {@link ES92GpuHnswSQVectorsFormat}, the graph is always built from the raw float32 vectors:
 * 1-bit codes with per-vector corrective terms can't be used directly as a CAGRA dataset.
 * <p>
 * Like the CPU format, it skips the graph for small segments, see
 * {@link ES93HnswBinaryQuantizedVectorsFormat#BBQ_HNSW_GRAPH_THRESHOLD}.
 */
public class ES97GpuHnswBBQVectorsFormat extends KnnVectorsFormat {
    public static final String NAME = ES93HnswBinaryQuantizedVectorsFormat.NAME;
    static final int MAXIMUM_MAX_CONN = 512;
    static final int MAXIMUM_BEAM_WIDTH = 3200;
    private final int maxConn;
    private final int beamWidth;
    private final int hnswGraphThreshold;

    /** The format for storing, reading, merging vectors on disk */
    private final FlatVectorsFormat flatVectorsFormat;
    private final Supplier<CuVSResourceManager> cuVSResourceManagerSupplier;
    private final long totalDeviceMemory;

    public ES97GpuHnswBBQVectorsFormat() {
        this(
            CuVSResourceManager::pooling,
            CuVSGPUSupport.instance().getTotalGpuMemory(),
            DEFAULT_MAX_CONN,
            DEFAULT_BEAM_WIDTH,
            false,
            -1,
            false
        );
    }

    public ES97GpuHnswBBQVectorsFormat(int maxConn, int beamWidth) {
        this(CuVSResourceManager::pooling, CuVSGPUSupport.instance().getTotalGpuMemory(), maxConn, beamWidth, false, -1, false);
    }

    /**
     * @param useDirectIO whether to use direct IO when reading raw vectors (the field's {@code on_disk_rescore} option)
     * @param hnswGraphThreshold the minimum expected search cost before building an HNSW graph (the field's
     *                           {@code flat_index_threshold} option); if negative, use the default
     * @param onDiskMerge whether merges read the source raw vectors with direct I/O (the field's {@code on_disk_merge} option);
     *                    the merged raw vectors are always written through the page cache
     */
    public ES97GpuHnswBBQVectorsFormat(
        long totalDeviceMemory,
        int maxConn,
        int beamWidth,
        boolean useDirectIO,
        int hnswGraphThreshold,
        boolean onDiskMerge
    ) {
        this(CuVSResourceManager::pooling, totalDeviceMemory, maxConn, beamWidth, useDirectIO, hnswGraphThreshold, onDiskMerge);
    }

    ES97GpuHnswBBQVectorsFormat(
        Supplier<CuVSResourceManager> cuVSResourceManagerSupplier,
        long totalDeviceMemory,
        int maxConn,
        int beamWidth,
        boolean useDirectIO,
        int hnswGraphThreshold,
        boolean onDiskMerge
    ) {
        super(NAME);
        this.totalDeviceMemory = totalDeviceMemory;
        this.cuVSResourceManagerSupplier = cuVSResourceManagerSupplier;
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
        this.hnswGraphThreshold = hnswGraphThreshold >= 0 ? hnswGraphThreshold : BBQ_HNSW_GRAPH_THRESHOLD;
        // merges write the raw vectors through the page cache even with on_disk_merge: the writer reads the merged
        // raw vectors straight back (mmapped) to build the graph on GPU, and a direct write would make that read cold
        this.flatVectorsFormat = ES93BinaryQuantizedVectorsFormat.withBufferedMergeWrites(
            DenseVectorFieldMapper.ElementType.FLOAT,
            useDirectIO,
            onDiskMerge
        );
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES92GpuHnswVectorsWriter(
            cuVSResourceManagerSupplier.get(),
            totalDeviceMemory,
            state,
            maxConn,
            beamWidth,
            flatVectorsFormat,
            flatVectorsFormat.fieldsWriter(state),
            hnswGraphThreshold
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
            + (hnswGraphThreshold > 0 ? ", hnswGraphThreshold=" + hnswGraphThreshold : "")
            + ", flatVectorFormat="
            + flatVectorsFormat
            + ")";
    }
}
