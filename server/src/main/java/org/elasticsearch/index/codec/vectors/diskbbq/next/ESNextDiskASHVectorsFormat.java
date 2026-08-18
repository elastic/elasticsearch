/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.TaskExecutor;
import org.elasticsearch.index.codec.vectors.DirectIOCapableFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfFlushConfigSource;
import org.elasticsearch.index.codec.vectors.diskbbq.IvfMergeConfigResolver;
import org.elasticsearch.index.codec.vectors.es93.DirectIOCapableLucene99FlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BFloat16FlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93GenericFlatVectorScorer;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Codec format for Asymmetric Scalar Hashing (ASH) based Inverted File Vector indexes.
 * This format uses ASH quantization to encode document vectors and score them against
 * query vectors projected through a learned projection matrix.
 *
 * <p>The format shares the same file extensions as the BBQ format:
 * <ul>
 *   <li>{@code .cenivf} — centroid data</li>
 *   <li>{@code .clivf} — cluster (posting list) data</li>
 *   <li>{@code .mivf} — IVF metadata</li>
 * </ul>
 */
public class ESNextDiskASHVectorsFormat extends KnnVectorsFormat {

    public static final String NAME = "ESNextDiskASHVectorsFormat";
    public static final String CENTROID_EXTENSION = "cenivf";
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
    public static final int MAX_DIMENSIONS = 4096;

    private final int vectorPerCluster;
    private final int centroidsPerParentCluster;
    private final boolean useDirectIO;
    private final DirectIOCapableFlatVectorsFormat rawVectorFormat;
    private final TaskExecutor mergeExec;
    private final int numMergeWorkers;
    private final int flatVectorThreshold;
    private final String sliceField;
    private final IvfFlushConfigSource ivfFlushConfigSource;
    private final IvfMergeConfigResolver ivfMergeConfigResolver;

    /** No-arg constructor for SPI. */
    public ESNextDiskASHVectorsFormat() {
        this(DEFAULT_VECTORS_PER_CLUSTER, DEFAULT_CENTROIDS_PER_PARENT_CLUSTER, null);
    }

    public ESNextDiskASHVectorsFormat(int vectorPerCluster, int centroidsPerParentCluster, String sliceField) {
        this(
            vectorPerCluster,
            centroidsPerParentCluster,
            DenseVectorFieldMapper.ElementType.FLOAT,
            false,
            null,
            1,
            defaultFlatThreshold(vectorPerCluster),
            sliceField,
            IvfFlushConfigSource.empty(),
            IvfMergeConfigResolver.useCodecDefault()
        );
    }

    public ESNextDiskASHVectorsFormat(
        int vectorPerCluster,
        int centroidsPerParentCluster,
        DenseVectorFieldMapper.ElementType elementType,
        boolean useDirectIO,
        ExecutorService mergingExecutorService,
        int maxMergingWorkers,
        int flatVectorThreshold,
        String sliceField,
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
        if (flatVectorThreshold < -1) {
            throw new IllegalArgumentException(
                "flatVectorThreshold must be -1 (dynamic), 0 (disabled), or > 0, got: " + flatVectorThreshold
            );
        }
        this.vectorPerCluster = vectorPerCluster;
        this.centroidsPerParentCluster = centroidsPerParentCluster;
        this.rawVectorFormat = switch (elementType) {
            case FLOAT, BYTE -> float32VectorFormat;
            case BFLOAT16 -> bfloat16VectorFormat;
            default -> throw new IllegalArgumentException("Unsupported element type " + elementType);
        };
        this.useDirectIO = useDirectIO;
        this.mergeExec = mergingExecutorService == null ? null : new TaskExecutor(mergingExecutorService);
        this.numMergeWorkers = maxMergingWorkers;
        this.flatVectorThreshold = flatVectorThreshold == -1 ? defaultFlatThreshold(vectorPerCluster) : flatVectorThreshold;
        this.sliceField = sliceField;
        this.ivfFlushConfigSource = ivfFlushConfigSource;
        this.ivfMergeConfigResolver = ivfMergeConfigResolver;
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ESNextDiskASHVectorsWriter(
            state,
            rawVectorFormat.getName(),
            useDirectIO,
            rawVectorFormat.fieldsWriter(state),
            vectorPerCluster,
            centroidsPerParentCluster,
            mergeExec,
            numMergeWorkers,
            flatVectorThreshold,
            sliceField,
            ivfFlushConfigSource,
            ivfMergeConfigResolver
        );
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ESNextDiskASHVectorsReader(state, (f, dio) -> {
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
        return "ESNextDiskASHVectorsFormat("
            + "vectorPerCluster="
            + vectorPerCluster
            + ", "
            + "mergeExec="
            + (mergeExec != null)
            + ", "
            + "sliceField="
            + sliceField
            + ')';
    }
}
