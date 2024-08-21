/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunking;

import org.elasticsearch.common.Strings;

import java.util.EnumSet;

public enum ChunkingStrategy {
    WORD("word"),
    SENTENCE("sentence");

    private final String chunkingStrategy;

    ChunkingStrategy(String strategy) {
        this.chunkingStrategy = strategy;
    }

    @Override
    public String toString() {
        return chunkingStrategy;
    }

    public static ChunkingStrategy fromString(String strategy) {
        return EnumSet.allOf(ChunkingStrategy.class)
            .stream()
            .filter(cs -> cs.chunkingStrategy.equals(strategy))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(Strings.format("Invalid chunkingStrategy %s", strategy)));
    }
}
