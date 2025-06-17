/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModel;

import java.util.List;

/**
 * A {@link Chunker} implementation that returns the input unchanged (no chunking is performed).
 *
 * <p><b>WARNING</b>If the input exceeds the maximum token limit, some services (such as {@link OpenAiEmbeddingsModel})
 * may return an error.
 * </p>
 */
public class NoopChunker implements Chunker {
    public static final NoopChunker INSTANCE = new NoopChunker();

    private NoopChunker() {}

    @Override
    public List<ChunkOffset> chunk(String input, ChunkingSettings chunkingSettings) {
        if (chunkingSettings instanceof NoneChunkingSettings) {
            return List.of(new ChunkOffset(0, input.length()));
        } else {
            throw new IllegalArgumentException(
                Strings.format("NoopChunker can't use ChunkingSettings with strategy [%s]", chunkingSettings.getChunkingStrategy())
            );
        }
    }
}
