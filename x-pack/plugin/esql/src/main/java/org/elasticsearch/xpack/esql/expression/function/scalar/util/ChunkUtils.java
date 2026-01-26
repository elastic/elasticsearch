/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.xpack.core.inference.chunking.Chunker;
import org.elasticsearch.xpack.core.inference.chunking.ChunkerBuilder;

import java.util.List;

public class ChunkUtils {
    private ChunkUtils() {}

    public static List<String> chunkText(String content, ChunkingSettings chunkingSettings) {
        Chunker chunker = ChunkerBuilder.fromChunkingStrategy(chunkingSettings.getChunkingStrategy());
        return chunker.chunk(content, chunkingSettings).stream().map(offset -> content.substring(offset.start(), offset.end())).toList();
    }

    public static void emitChunks(BytesRefBlock.Builder builder, List<String> chunks) {
        int size = chunks.size();
        if (size == 0) {
            builder.appendNull();
        } else if (size == 1) {
            builder.appendBytesRef(new BytesRef(chunks.get(0).trim()));
        } else {
            builder.beginPositionEntry();
            for (String chunk : chunks) {
                builder.appendBytesRef(new BytesRef(chunk.trim()));
            }
            builder.endPositionEntry();
        }
    }

}
