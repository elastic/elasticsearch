/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

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
public class IVFVectorsFormat extends KnnVectorsFormat {

    public static final String IVF_VECTOR_COMPONENT = "IVF";
    public static final String NAME = "IVFVectorsFormat";
    // centroid ordinals -> centroid values, offsets
    public static final String CENTROID_EXTENSION = "cenivf";
    // offsets contained in cen_ivf, [vector ordinals, actually just docIds](long varint), quantized
    // vectors (OSQ bit)
    public static final String CLUSTER_EXTENSION = "clivf";
    static final String IVF_META_EXTENSION = "mivf";

    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;

    private static final FlatVectorsFormat rawVectorFormat = new Lucene99FlatVectorsFormat(
        FlatVectorScorerUtil.getLucene99FlatVectorsScorer()
    );

    public static final int DYNAMIC_NPROBE = -1;
    public static final int DEFAULT_VECTORS_PER_CLUSTER = 384;
    public static final int MIN_VECTORS_PER_CLUSTER = 64;
    public static final int MAX_VECTORS_PER_CLUSTER = 1 << 16; // 65536

    private final int vectorPerCluster;

    public IVFVectorsFormat(int vectorPerCluster) {
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
        this.vectorPerCluster = vectorPerCluster;
    }

    /** Constructs a format using the given graph construction parameters and scalar quantization. */
    public IVFVectorsFormat() {
        this(DEFAULT_VECTORS_PER_CLUSTER);
    }

    @Override
    public KnnVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new DefaultIVFVectorsWriter(state, rawVectorFormat.fieldsWriter(state), vectorPerCluster);
    }

    @Override
    public KnnVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        return new DefaultIVFVectorsReader(state, rawVectorFormat.fieldsReader(state));
    }

    @Override
    public int getMaxDimensions(String fieldName) {
        return 4096;
    }

    @Override
    public String toString() {
        return "IVFVectorsFormat(" + "vectorPerCluster=" + vectorPerCluster + ')';
    }

    static IVFVectorsReader getIVFReader(KnnVectorsReader vectorsReader, String fieldName) {
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
            vectorsReader = candidateReader.getFieldReader(fieldName);
        }
        if (vectorsReader instanceof IVFVectorsReader reader) {
            return reader;
        }
        return null;
    }
}
