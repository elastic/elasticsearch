/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ChunkingStrategy;

import java.util.Map;

public class ChunkingSettingsBuilder {
    public static final SentenceBoundaryChunkingSettings DEFAULT_SETTINGS = new SentenceBoundaryChunkingSettings(250, 1);

    public static ChunkingSettings fromMap(Map<String, Object> settings) {
        if (settings.isEmpty()) {
            return DEFAULT_SETTINGS;
        }
        if (settings.containsKey(ChunkingSettingsOptions.STRATEGY.toString()) == false) {
            throw new IllegalArgumentException("Can't generate Chunker without ChunkingStrategy provided");
        }

        ChunkingStrategy chunkingStrategy = ChunkingStrategy.fromString(
            settings.get(ChunkingSettingsOptions.STRATEGY.toString()).toString()
        );
        return switch (chunkingStrategy) {
            case WORD -> WordBoundaryChunkingSettings.fromMap(settings);
            case SENTENCE -> SentenceBoundaryChunkingSettings.fromMap(settings);
        };
    }
}
