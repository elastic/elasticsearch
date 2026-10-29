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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Implemented by field mappers that select a Lucene vector format for the field they write.
 */
public interface KnnVectorsFormatMapper {

    /**
     * Selects the format used to write this mapper's vector value.
     *
     * @param defaultFormat the index's default vector format
     * @param indexSettings settings for the index being written
     * @param threadPool the node thread pool, or {@code null} when unavailable
     */
    KnnVectorsFormat getKnnVectorsFormatForField(
        KnnVectorsFormat defaultFormat,
        IndexSettings indexSettings,
        @Nullable ThreadPool threadPool
    );
}
