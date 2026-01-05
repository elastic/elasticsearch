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
        DenseVectorFieldMapper.ElementType elementType
    );
}
