/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.es95;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.TaskExecutor;
import org.elasticsearch.index.codec.vectors.DirectIOCapableFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.es93.DirectIOCapableLucene99FlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BFloat16FlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93GenericFlatVectorScorer;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Codec format for Inverted File Vector indexes. This index expects to break the dimensional space
 * into clusters and assign each vector to a cluster generating a posting list of vectors. Clusters
 * are represented by centroids.
 * The vector quantization format used here is a per-vector optimized scalar quantization. Also see {@link
 * org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer}. Some of key features are:
 * The format is stored in three files:
 *
 * <h2>.cenivf (centroid data) file</h2>
 *  <p> Which stores the raw and quantized centroid vectors.
 *
 * <h2>.clivf (cluster data) file</h2>
 *
 * <p> Stores the quantized vectors for each cluster, inline and stored in blocks. Additionally, the docIds of
 *  each vector is stored.
 *
 * <h2>.mivf (centroid metadata) file</h2>
 *
 * <p> Stores metadata including the number of centroids and their offsets in the clivf file</p>
 *
 */
public class ES950DiskBBQVectorsFormat extends KnnVectorsFormat {

    public static final String NAME = "ES950DiskBBQVectorsFormat";
    // centroid ordinals -> centroid values, offsets
    public static final String CENTROID_EXTENSION = "cenivf";
    // offsets contained in cen_ivf, [vector ordinals, actually just docIds](long varint), quantized vectors
    public static final String CLUSTER_EXTENSION = "clivf";
    public static final String IVF_META_EXTENSION = "mivf";

    public static final int VERSION_START = 1;
    public static final int VERSION_DIRECT_IO = VERSION_START;
    public static final int VERSION_CURRENT = VERSION_START;
    public static final float DYNAMIC_VISIT_RATIO = 0.0f;

    private static final DirectIOCapableFlatVectorsFormat float32VectorFormat = new DirectIOCapableLucene99FlatVectorsFormat(
        ES93GenericFlatVectorScorer.INSTANCE
    );
    private static final DirectIOCapableFlatVectorsFormat bfloat16VectorFormat = new ES93BFloat16FlatVectorsFormat(
        ES93GenericFlatVectorScorer.INSTANCE
    );
    private static final Map<String, DirectIOCapableFlatVectorsFormat> supportedFormats = Map.of(
        float32VectorFormat.getName(),
        float32VectorFormat,
        bfloat16VectorFormat.getName(),
        bfloat16VectorFormat
    );

    public static final int DEFAULT_VECTORS_PER_CLUSTER = 384;
    private static final int DEFAULT_FLAT_VECTOR_THRESHOLD_MULTIPLIER = 3;

    /**
     * Returns the default flat index threshold for the given cluster size.
     * @param configuredClusterSize the configured cluster size
     * @return the default flat index threshold
     */
    public static int defaultFlatThreshold(int configuredClusterSize) {
        return configuredClusterSize * DEFAULT_FLAT_VECTOR_THRESHOLD_MULTIPLIER;
    }

    public static final int MIN_VECTORS_PER_CLUSTER = 64;
    public static final int MAX_VECTORS_PER_CLUSTER = 1 << 16; // 65536
    public static final int DEFAULT_CENTROIDS_PER_PARENT_CLUSTER = 16;
    public static final int MIN_CENTROIDS_PER_PARENT_CLUSTER = 2;
    public static final int MAX_CENTROIDS_PER_PARENT_CLUSTER = DEFAULT_VECTORS_PER_CLUSTER; // 384
    public static final int DEFAULT_PRECONDITIONING_BLOCK_DIMENSION = 32;
    public static final int MIN_PRECONDITIONING_BLOCK_DIMS = 8;
    public static final int MAX_PRECONDITIONING_BLOCK_DIMS = 384;
    public static final int MAX_DIMENSIONS = 4096;

    private final QuantEncoding quantEncoding;
    private final int vectorPerCluster;
    private final int centroidsPerParentCluster;
    private final boolean useDirectIO;
    private final DirectIOCapableFlatVectorsFormat rawVectorFormat;
    private final TaskExecutor mergeExec;
    private final int numMergeWorkers;
    private final boolean doPrecondition;
    private final int preconditioningBlockDimension;
    private final int flatVectorThreshold;
    private final IvfFlushConfigSource ivfFlushConfigSource;
    private final IvfMergeConfigResolver ivfMergeConfigResolver;

    public ES950DiskBBQVectorsFormat(int vectorPerCluster, int centroidsPerParentCluster) {
        this(QuantEncoding.ONE_BIT_4BIT_QUERY, vectorPerCluster, centroidsPerParentCluster);
    }

    public ES950DiskBBQVectorsFormat(QuantEncoding quantEncoding, int vectorPerCluster, int centroidsPerParentCluster) {
        this(
            quantEncoding,
            vectorPerCluster,
            centroidsPerParentCluster,
            DenseVectorFieldMapper.ElementType.FLOAT,
            false,
            null,
            1,
            false,
            DEFAULT_PRECONDITIONING_BLOCK_DIMENSION,
            defaultFlatThreshold(vectorPerCluster),
            IvfFlushConfigSource.empty(),
            IvfMergeConfigResolver.useCodecDefault()
        );
    }

    public ES950DiskBBQVectorsFormat(
        QuantEncoding quantEncoding,
        int vectorPerCluster,
        int centroidsPerParentCluster,
        DenseVectorFieldMapper.ElementType elementType,
        boolean useDirectIO,
        ExecutorService mergingExecutorService,
        int maxMergingWorkers,
        boolean doPrecondition,
        int preconditioningBlockDimension
    ) {
        this(
            quantEncoding,
            vectorPerCluster,
            centroidsPerParentCluster,
            elementType,
            useDirectIO,
            mergingExecutorService,
            maxMergingWorkers,
            doPrecondition,
            preconditioningBlockDimension,
            defaultFlatThreshold(vectorPerCluster),
            IvfFlushConfigSource.empty(),
            IvfMergeConfigResolver.useCodecDefault()
        );
    }

    public ES950DiskBBQVectorsFormat(
        QuantEncoding quantEncoding,
        int vectorPerCluster,
        int centroidsPerParentCluster,
        DenseVectorFieldMapper.ElementType elementType,
        boolean useDirectIO,
        ExecutorService mergingExecutorService,
        int maxMergingWorkers,
        boolean doPrecondition,
        int preconditioningBlockDimension,
        int flatVectorThreshold
    ) {
        this(
            quantEncoding,
            vectorPerCluster,
            centroidsPerParentCluster,
            elementType,
            useDirectIO,
            mergingExecutorService,
            maxMergingWorkers,
            doPrecondition,
            preconditioningBlockDimension,
            flatVectorThreshold,
            IvfFlushConfigSource.empty(),
            IvfMergeConfigResolver.useCodecDefault()
        );
    }

    /**
     * @param ivfFlushConfigSource optional per-field config on flush ({@code null} uses writer default)
     * @param ivfMergeConfigResolver optional merged config on merge ({@code null} uses writer default)
     */
    public ES950DiskBBQVectorsFormat(
        QuantEncoding quantEncoding,
        int vectorPerCluster,
        int centroidsPerParentCluster,
        DenseVectorFieldMapper.ElementType elementType,
        boolean useDirectIO,
        ExecutorService mergingExecutorService,
        int maxMergingWorkers,
        boolean doPrecondition,
        int preconditioningBlockDimension,
        int flatVectorThreshold,
        IvfFlushConfigSource ivfFlushConfigSource,
        IvfMergeConfigResolver ivfMergeConfigResolver
    ) {
        super(NAME);
        if (vectorPerCluster < MIN_VECTORS_PER_CLUSTER || vectorPerCluster > MAX_VECTORS_PER_CLUSTER) {
            throw new IllegalArgumentException(
                "vectorsPerCluster must be between "
                    + MIN_VECTORS_PER_CLUSTER
                    + " and "
                    + MAX_VECTORS_PER_CLUSTER
                    + ", got: "
                    + vectorPerCluster
            );
        }
        if (centroidsPerParentCluster < MIN_CENTROIDS_PER_PARENT_CLUSTER || centroidsPerParentCluster > MAX_CENTROIDS_PER_PARENT_CLUSTER) {
            throw new IllegalArgumentException(
                "centroidsPerParentCluster must be between "
                    + MIN_CENTROIDS_PER_PARENT_CLUSTER
                    + " and "
                    + MAX_CENTROIDS_PER_PARENT_CLUSTER
                    + ", got: "
                    + centroidsPerParentCluster
            );
        }
        if (doPrecondition
            && (preconditioningBlockDimension < MIN_PRECONDITIONING_BLOCK_DIMS
                || preconditioningBlockDimension > MAX_PRECONDITIONING_BLOCK_DIMS)) {
            throw new IllegalArgumentException(
                "preconditioningBlockDimension must be between "
                    + MIN_PRECONDITIONING_BLOCK_DIMS
                    + " and "
                    + MAX_PRECONDITIONING_BLOCK_DIMS
                    + ", got: "
                    + preconditioningBlockDimension
            );
        }
        if (flatVectorThreshold < -1) {
            throw new IllegalArgumentException(
                "flatVectorThreshold must be -1 (dynamic), 0 (disabled), or > 0, got: " + flatVectorThreshold
            );
        }
        this.vectorPerCluster = vectorPerCluster;
        this.centroidsPerParentCluster = centroidsPerParentCluster;
        this.quantEncoding = quantEncoding;
        this.rawVectorFormat = switch (elementType) {
            case FLOAT -> float32VectorFormat;
            case BFLOAT16 -> bfloat16VectorFormat;
            default -> throw new IllegalArgumentException("Unsupported element type " + elementType);
        };
        this.useDirectIO = useDirectIO;
        this.mergeExec = mergingExecutorService == null ? null : new TaskExecutor(mergingExecutorService);
        this.numMergeWorkers = maxMergingWorkers;
        this.preconditioningBlockDimension = preconditioningBlockDimension;
        this.doPrecondition = doPrecondition;
        this.flatVectorThreshold = flatVectorThreshold == -1 ? defaultFlatThreshold(vectorPerCluster) : flatVectorThreshold;
        this.ivfFlushConfigSource = ivfFlushConfigSource;
        this.ivfMergeConfigResolver = ivfMergeConfigResolver;
    }

    /** Constructs a format using the given graph construction parameters and scalar quantization. */
    public ES950DiskBBQVectorsFormat() {
        this(DEFAULT_VECTORS_PER_CLUSTER, DEFAULT_CENTROIDS_PER_PARENT_CLUSTER);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES950DiskBBQVectorsWriter(
            state,
            rawVectorFormat.getName(),
            useDirectIO,
            rawVectorFormat.fieldsWriter(state),
            quantEncoding,
            vectorPerCluster,
            centroidsPerParentCluster,
            mergeExec,
            numMergeWorkers,
            preconditioningBlockDimension,
            doPrecondition,
            flatVectorThreshold,
            ivfFlushConfigSource,
            ivfMergeConfigResolver
        );
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES950DiskBBQVectorsReader(state, (f, dio) -> {
            var format = supportedFormats.get(f);
            if (format == null) return null;
            return format.fieldsReader(state, dio);
        });
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMENSIONS;
    }

    @Override
    public String toString() {
        return "ES950DiskBBQVectorsFormat(" + "vectorPerCluster=" + vectorPerCluster + ", " + "mergeExec=" + (mergeExec != null) + ')';
    }

}
