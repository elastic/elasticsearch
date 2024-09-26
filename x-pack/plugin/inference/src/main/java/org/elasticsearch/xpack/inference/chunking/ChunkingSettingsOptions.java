/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

public enum ChunkingSettingsOptions {
    STRATEGY("strategy"),
    MAX_CHUNK_SIZE("max_chunk_size"),
    OVERLAP("overlap");

    private final String chunkingSettingsOption;

    ChunkingSettingsOptions(String chunkingSettingsOption) {
        this.chunkingSettingsOption = chunkingSettingsOption;
    }

    @Override
    public String toString() {
        return chunkingSettingsOption;
    }
}
