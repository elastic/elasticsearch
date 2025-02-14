/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

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
