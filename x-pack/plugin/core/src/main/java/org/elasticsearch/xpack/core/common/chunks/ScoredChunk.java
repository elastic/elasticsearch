/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.chunks;

/**
 * Represents a chunk with its relevance score.
 *
 * @param docId when this chunk comes from {@link MemoryIndexChunkScorer}, the Lucene document id of the indexed
 *              chunk (aligned with the chunk list order). Otherwise {@link #NO_DOC_ID}.
 */
public record ScoredChunk(String content, float score, int docId) {

    /** Value for {@link #docId} when the chunk is not backed by a Lucene document (e.g. semantic chunking). */
    public static final int NO_DOC_ID = -1;

    public ScoredChunk(String content, float score) {
        this(content, score, NO_DOC_ID);
    }
}
