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
import org.elasticsearch.common.util.FeatureFlag;

import java.io.IOException;

/**
 * Codec format for Inverted File Vector indexes. This index expects to break the dimensional space
 * into clusters and assign each vector to a cluster generating a posting list of vectors. Clusters
 * are represented by centroids.
 * <p>
 * Each posting list is individual quantized and stored in-line allowing for fast block scoring.
 *
 * <p>THe index is searcher by looking for the closest centroids to our vector query and then
 * scoring the vectors in the posting list of the closest centroids.
 */
public class IVFVectorsFormat extends KnnVectorsFormat {

    static final FeatureFlag IVF_FORMAT_FEATURE_FLAG = new FeatureFlag("ivf_format");
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

    private static final int DEFAULT_VECTORS_PER_CLUSTER = 1000;

    private final int vectorPerCluster;

    public IVFVectorsFormat(int vectorPerCluster) {
        super(NAME);
        if (IVF_FORMAT_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalStateException("IVF format is not enabled");
        }
        this.vectorPerCluster = vectorPerCluster;
    }

    /** Constructs a format using the given graph construction parameters and scalar quantization. */
    public IVFVectorsFormat() {
        this(DEFAULT_VECTORS_PER_CLUSTER);
        if (IVF_FORMAT_FEATURE_FLAG.isEnabled() == false) {
            throw new IllegalStateException("IVF format is not enabled");
        }
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
        return 1024;
    }

    @Override
    public String toString() {
        return "IVFVectorFormat";
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
