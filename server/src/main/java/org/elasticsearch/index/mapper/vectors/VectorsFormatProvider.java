/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;

import java.util.concurrent.ExecutorService;

/**
 * A service provider interface for obtaining Lucene {@link KnnVectorsFormat} instances.
 * Plugins can implement this interface to provide custom vector formats
 */
public interface VectorsFormatProvider {

    /**
     * Returns a {@link KnnVectorsFormat} instance based on the provided index settings and vector index options.
     * May return {@code null} if the provider does not support the format for the given index settings or vector index options.
     *
     * @param indexSettings The index settings.
     * @param indexOptions The dense vector index options.
     * @param similarity The vector similarity function.
     * @param elementType The type of elements in the vector.
     * @return A KnnVectorsFormat instance.
     */
    KnnVectorsFormat getKnnVectorsFormat(
        IndexSettings indexSettings,
        DenseVectorFieldMapper.DenseVectorIndexOptions indexOptions,
        DenseVectorFieldMapper.VectorSimilarity similarity,
        DenseVectorFieldMapper.ElementType elementType,
        ExecutorService mergingExecutorService,
        int maxMergingWorkers
    );

    /**
     * Returns whether a particular vector index type is allowed (e.g. by licensing) for an index created on the given version.
     * <p>
     * This method is intended for eager decisions such as selecting defaults. Implementations should not throw.
     * Enforced checks must still happen in {@link #getKnnVectorsFormat(IndexSettings, DenseVectorFieldMapper.DenseVectorIndexOptions,
     * DenseVectorFieldMapper.VectorSimilarity, DenseVectorFieldMapper.ElementType, ExecutorService, int)}.
     */
    default boolean isVectorIndexTypeAllowed(IndexVersion indexVersionCreated, DenseVectorFieldMapper.VectorIndexType indexType) {
        return false;
    }
}
