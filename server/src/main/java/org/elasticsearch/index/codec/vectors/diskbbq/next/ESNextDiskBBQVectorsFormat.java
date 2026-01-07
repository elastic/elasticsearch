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
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.index.codec.vectors.DirectIOCapableFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.es93.DirectIOCapableLucene99FlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.es93.ES93BFloat16FlatVectorsFormat;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Map;

/**
 * Codec format for Inverted File Vector indexes. This index expects to break the dimensional space
 * into clusters and assign each vector to a cluster generating a posting list of vectors. Clusters
 * are represented by centroids.
 * The vector quantization format used here is a per-vector optimized scalar quantization. Also see {@link
 * OptimizedScalarQuantizer}. Some of key features are:
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
public class ESNextDiskBBQVectorsFormat extends KnnVectorsFormat {

    public static final String NAME = "ESNextDiskBBQVectorsFormat";

    public static final int VERSION_START = 1;
    public static final int VERSION_CURRENT = VERSION_START;

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

    public static final int DEFAULT_VECTORS_PER_CLUSTER = 384;
    public static final int MIN_VECTORS_PER_CLUSTER = 64;
    public static final int MAX_VECTORS_PER_CLUSTER = 1 << 16; // 65536
    public static final int DEFAULT_CENTROIDS_PER_PARENT_CLUSTER = 16;
    public static final int MIN_CENTROIDS_PER_PARENT_CLUSTER = 2;
    public static final int MAX_CENTROIDS_PER_PARENT_CLUSTER = 1 << 8; // 256

    public enum QuantEncoding {
        ONE_BIT_4BIT_QUERY(0, (byte) 1, (byte) 4) {
            @Override
            public void pack(int[] quantized, byte[] destination) {
                ESVectorUtil.packAsBinary(quantized, destination);
            }

            @Override
            public void packQuery(int[] quantized, byte[] destination) {
                ESVectorUtil.transposeHalfByte(quantized, destination);
            }
        },
        TWO_BIT_4BIT_QUERY(1, (byte) 2, (byte) 4) {
            @Override
            public void pack(int[] quantized, byte[] destination) {
                ESVectorUtil.packDibit(quantized, destination);
            }

            @Override
            public void packQuery(int[] quantized, byte[] destination) {
                ESVectorUtil.transposeHalfByte(quantized, destination);
            }

            @Override
            public int discretizedDimensions(int dimensions) {
                int queryDiscretized = (dimensions * 4 + 7) / 8 * 8 / 4;
                // we want to force dibit packing to byte boundaries assuming single bit striping
                // so we discretize to the same as single bit encoding
                int docDiscretized = (dimensions + 7) / 8 * 8;
                int maxDiscretized = Math.max(queryDiscretized, docDiscretized);
                assert maxDiscretized % (8.0 / 4) == 0 : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
                assert maxDiscretized % (8.0 / 2) == 0 : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
                return maxDiscretized;
            }

            @Override
            public int getDocPackedLength(int dimensions) {
                // discretized to single bit encoding, but we assume dibit packing (2 bits per value)
                // so we need twice as many bytes as single bit encoding
                int discretized = discretizedDimensions(dimensions);
                return 2 * ((discretized + 7) / 8);
            }
        },
        FOUR_BIT_SYMMETRIC(2, (byte) 4, (byte) 4) {
            @Override
            public void packQuery(int[] quantized, byte[] destination) {
                ESVectorUtil.transposeHalfByte(quantized, destination);
            }

            @Override
            public void pack(int[] quantized, byte[] destination) {
                ESVectorUtil.transposeHalfByte(quantized, destination);
            }

            @Override
            public int getDocPackedLength(int dimensions) {
                int discretized = discretizedDimensions(dimensions);
                return 4 * ((discretized + 7) / 8);
            }

            @Override
            public int getQueryPackedLength(int dimensions) {
                return getDocPackedLength(dimensions);
            }

            @Override
            public int discretizedDimensions(int dimensions) {
                int totalBits = dimensions * 4;
                return (totalBits + 7) / 8 * 8 / 4;
            }
        };

        private final int id;
        private final byte bits, queryBits;

        QuantEncoding(int id, byte bits, byte queryBits) {
            this.id = id;
            this.bits = bits;
            this.queryBits = queryBits;
        }

        public abstract void pack(int[] quantized, byte[] destination);

        public abstract void packQuery(int[] quantized, byte[] destination);

        public int id() {
            return id;
        }

        public byte bits() {
            return bits;
        }

        public byte queryBits() {
            return queryBits;
        }

        public int discretizedDimensions(int dimensions) {
            if (queryBits == bits) {
                int totalBits = dimensions * bits;
                return (totalBits + 7) / 8 * 8 / bits;
            }
            int queryDiscretized = (dimensions * queryBits + 7) / 8 * 8 / queryBits;
            int docDiscretized = (dimensions * bits + 7) / 8 * 8 / bits;
            int maxDiscretized = Math.max(queryDiscretized, docDiscretized);
            assert maxDiscretized % (8.0 / queryBits) == 0 : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
            assert maxDiscretized % (8.0 / bits) == 0 : "bad discretized=" + maxDiscretized + " for dim=" + dimensions;
            return maxDiscretized;
        }

        /** Return the number of bytes required to store a packed vector of the given dimensions. */
        public int getDocPackedLength(int dimensions) {
            int discretized = discretizedDimensions(dimensions);
            // how many bytes do we need to store the quantized vector?
            int totalBits = discretized * bits;
            return (totalBits + 7) / 8;
        }

        public int getQueryPackedLength(int dimensions) {
            int discretized = discretizedDimensions(dimensions);
            // how many bytes do we need to store the quantized vector?
            int totalBits = discretized * queryBits;
            return (totalBits + 7) / 8;
        }

        public static QuantEncoding fromId(int id) {
            for (QuantEncoding encoding : values()) {
                if (encoding.id == id) {
                    return encoding;
                }
            }
            throw new IllegalArgumentException("Unknown QuantEncoding id: " + id);
        }
    }

    private final QuantEncoding quantEncoding;
    private final int vectorPerCluster;
    private final int centroidsPerParentCluster;
    private final boolean useDirectIO;
    private final DirectIOCapableFlatVectorsFormat rawVectorFormat;

    public ESNextDiskBBQVectorsFormat(int vectorPerCluster, int centroidsPerParentCluster) {
        this(QuantEncoding.ONE_BIT_4BIT_QUERY, vectorPerCluster, centroidsPerParentCluster);
    }

    public ESNextDiskBBQVectorsFormat(QuantEncoding quantEncoding, int vectorPerCluster, int centroidsPerParentCluster) {
        this(quantEncoding, vectorPerCluster, centroidsPerParentCluster, DenseVectorFieldMapper.ElementType.FLOAT, false);
    }

    public ESNextDiskBBQVectorsFormat(
        QuantEncoding quantEncoding,
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
        this.quantEncoding = quantEncoding;
        this.rawVectorFormat = switch (elementType) {
            case FLOAT -> float32VectorFormat;
            case BFLOAT16 -> bfloat16VectorFormat;
            default -> throw new IllegalArgumentException("Unsupported element type " + elementType);
        };
        this.useDirectIO = useDirectIO;
    }

    /** Constructs a format using the given graph construction parameters and scalar quantization. */
    public ESNextDiskBBQVectorsFormat() {
        this(DEFAULT_VECTORS_PER_CLUSTER, DEFAULT_CENTROIDS_PER_PARENT_CLUSTER);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new ESNextDiskBBQVectorsWriter(
            state,
            rawVectorFormat.getName(),
            useDirectIO,
            rawVectorFormat.fieldsWriter(state),
            quantEncoding,
            vectorPerCluster,
            centroidsPerParentCluster
        );
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new ESNextDiskBBQVectorsReader(state, (f, dio) -> {
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
        return "ESNextDiskBBQVectorsFormat(" + "vectorPerCluster=" + vectorPerCluster + ')';
    }

}
