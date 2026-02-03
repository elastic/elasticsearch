/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.chunking;

import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.ChunkingStrategy;

import java.util.HashMap;
import java.util.Map;

public class ChunkingSettingsBuilder {
    public static final SentenceBoundaryChunkingSettings DEFAULT_SETTINGS = new SentenceBoundaryChunkingSettings(250, 1);
    // Old settings used for backward compatibility for endpoints created before 8.16 when default was changed
    public static final WordBoundaryChunkingSettings OLD_DEFAULT_SETTINGS = new WordBoundaryChunkingSettings(250, 100);
    public static final int ELASTIC_RERANKER_TOKEN_LIMIT = 512;
    public static final int ELASTIC_RERANKER_EXTRA_TOKEN_COUNT = 3;
    public static final float WORDS_PER_TOKEN = 0.75f;

    public static ChunkingSettings fromMap(Map<String, Object> settings) {
        return fromMap(settings, true);
    }

    public static ChunkingSettings fromMap(Map<String, Object> settings, boolean returnDefaultValues) {

        if (returnDefaultValues) {
            if (settings == null) {
                return OLD_DEFAULT_SETTINGS;
            }
            if (settings.isEmpty()) {
                return DEFAULT_SETTINGS;
            }
        } else {
            if (settings == null || settings.isEmpty()) {
                return null;
            }
        }

        if (settings.containsKey(ChunkingSettingsOptions.STRATEGY.toString()) == false) {
            throw new IllegalArgumentException("Can't generate Chunker without ChunkingStrategy provided");
        }

        ChunkingStrategy chunkingStrategy = ChunkingStrategy.fromString(
            settings.get(ChunkingSettingsOptions.STRATEGY.toString()).toString()
        );
        return switch (chunkingStrategy) {
            case NONE -> NoneChunkingSettings.INSTANCE;
            case WORD -> WordBoundaryChunkingSettings.fromMap(new HashMap<>(settings));
            case SENTENCE -> SentenceBoundaryChunkingSettings.fromMap(new HashMap<>(settings));
            case RECURSIVE -> RecursiveChunkingSettings.fromMap(new HashMap<>(settings));
        };
    }

    public static ChunkingSettings buildChunkingSettingsForElasticRerank(int queryWordCount) {
        var queryTokenCount = Math.ceil(queryWordCount / WORDS_PER_TOKEN);
        var chunkSizeTokenCountWithFullQuery = (ELASTIC_RERANKER_TOKEN_LIMIT - ELASTIC_RERANKER_EXTRA_TOKEN_COUNT - queryTokenCount);

        var maxChunkSizeTokenCount = Math.floor((float) ELASTIC_RERANKER_TOKEN_LIMIT / 2);
        if (chunkSizeTokenCountWithFullQuery > maxChunkSizeTokenCount) {
            maxChunkSizeTokenCount = chunkSizeTokenCountWithFullQuery;
        }

        var maxChunkSizeWordCount = (int) (maxChunkSizeTokenCount * WORDS_PER_TOKEN);
        return new SentenceBoundaryChunkingSettings(maxChunkSizeWordCount, 1);
    }
}
