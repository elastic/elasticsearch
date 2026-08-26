/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.chunks;

/**
 * Represents a chunk with its relevance score and its original position in the input chunk list
 * (0-based). When unknown, {@code originalIndex} is {@code -1}.
 */
public record ScoredChunk(String content, float score, int originalIndex) {
    /** Same as {@link #ScoredChunk(String, float, int)} with {@code originalIndex} {@code -1}. */
    public ScoredChunk(String content, float score) {
        this(content, score, -1);
    }
}
