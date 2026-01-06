/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.DirectIOCapableFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.es93.DirectIOCapableLucene99FlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BFloat16FlatVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;
import java.util.Map;

/**
 * Codec format for Inverted File Vector indexes. This index expects to break the dimensional space
 * into clusters and assign each vector to a cluster generating a posting list of vectors. Clusters
 * are represented by centroids.
 * The vector quantization format used here is a per-vector optimized scalar quantization. Also see {@link
 * OptimizedScalarQuantizer}. Some of key features are:
 *
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
public class ES920DiskBBQVectorsFormat extends KnnVectorsFormat {

    public static final String NAME = "ES920DiskBBQVectorsFormat";
    // centroid ordinals -> centroid values, offsets
    public static final String CENTROID_EXTENSION = "cenivf";
    // offsets contained in cen_ivf, [vector ordinals, actually just docIds](long varint), quantized
    // vectors (OSQ bit)
    public static final String CLUSTER_EXTENSION = "clivf";
    static final String IVF_META_EXTENSION = "mivf";

    public static final int VERSION_START = 0;
    public static final int VERSION_DIRECT_IO = 1;
    public static final int VERSION_CURRENT = VERSION_DIRECT_IO;

    private static final DirectIOCapableFlatVectorsFormat float32VectorFormat = new DirectIOCapableLucene99FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );
    private static final DirectIOCapableFlatVectorsFormat bfloat16VectorFormat = new ES93BFloat16FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );
    private static final Map<String, DirectIOCapableFlatVectorsFormat> supportedFormats = Map.of(
        float32VectorFormat.getName(),
        float32VectorFormat,
        bfloat16VectorFormat.getName(),
        bfloat16VectorFormat
    );

    // This dynamically sets the cluster probe based on the `k` requested and the number of clusters.
    // useful when searching with 'efSearch' type parameters instead of requiring a specific ratio.
    public static final float DYNAMIC_VISIT_RATIO = 0.0f;
    public static final int DEFAULT_VECTORS_PER_CLUSTER = 384;
    public static final int MIN_VECTORS_PER_CLUSTER = 64;
    public static final int MAX_VECTORS_PER_CLUSTER = 1 << 16; // 65536
    public static final int DEFAULT_CENTROIDS_PER_PARENT_CLUSTER = 16;
    public static final int MIN_CENTROIDS_PER_PARENT_CLUSTER = 2;
    public static final int MAX_CENTROIDS_PER_PARENT_CLUSTER = 1 << 8; // 256

    private final int vectorPerCluster;
    private final int centroidsPerParentCluster;
    private final DirectIOCapableFlatVectorsFormat rawVectorFormat;
    private final boolean useDirectIO;

    public ES920DiskBBQVectorsFormat(int vectorPerCluster, int centroidsPerParentCluster) {
        this(vectorPerCluster, centroidsPerParentCluster, DenseVectorFieldMapper.ElementType.FLOAT, false);
    }

    public ES920DiskBBQVectorsFormat(
        int vectorPerCluster,
        int centroidsPerParentCluster,
        DenseVectorFieldMapper.ElementType elementType,
        boolean useDirectIO
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
        this.vectorPerCluster = vectorPerCluster;
        this.centroidsPerParentCluster = centroidsPerParentCluster;
        this.rawVectorFormat = switch (elementType) {
            case FLOAT -> float32VectorFormat;
            case BFLOAT16 -> bfloat16VectorFormat;
            default -> throw new IllegalArgumentException("Unsupported element type " + elementType);
        };
        this.useDirectIO = useDirectIO;
    }

    /** Constructs a format using the given graph construction parameters and scalar quantization. */
    public ES920DiskBBQVectorsFormat() {
        this(DEFAULT_VECTORS_PER_CLUSTER, DEFAULT_CENTROIDS_PER_PARENT_CLUSTER);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ES920DiskBBQVectorsWriter(
            state,
            rawVectorFormat.getName(),
            useDirectIO,
            rawVectorFormat.fieldsWriter(state),
            vectorPerCluster,
            centroidsPerParentCluster
        );
    }

    // for testing
    KnnVectorsWriter version0FieldsWriter(SegmentWriteState state) throws IOException {
        return new ES920DiskBBQVectorsWriter(
            state,
            rawVectorFormat.getName(),
            null,
            rawVectorFormat.fieldsWriter(state),
            vectorPerCluster,
            centroidsPerParentCluster,
            VERSION_START
        );
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ES920DiskBBQVectorsReader(state, (f, dio) -> {
            var format = supportedFormats.get(f);
            if (format == null) return null;
            return format.fieldsReader(state, dio);
        });
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return 4096;
    }

    @Override
    public String toString() {
        return "ES920DiskBBQVectorsFormat(" + "vectorPerCluster=" + vectorPerCluster + ')';
    }

}
