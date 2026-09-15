/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

/**
 * What closes a chunk: whichever bound it reaches first. A chunk is what a read of one value decompresses,
 * and the same byte count is a few long values or tens of thousands of short ones, so it is bounded both ways.
 *
 * @param targetBytes bytes a chunk holds before it is closed
 * @param maxValues   values a chunk holds before it is closed. A chunk ends on a value boundary, so it
 *                    closes at the first one at or past this rather than exactly on it.
 */
public record ChunkBounds(int targetBytes, int maxValues) {

    /** Bounds a chunk by bytes alone, for a stream stored without compression, where a value is read where it lies. */
    public static ChunkBounds ofBytes(int targetBytes) {
        return new ChunkBounds(targetBytes, Integer.MAX_VALUE);
    }

    public ChunkBounds {
        if (targetBytes <= 0) {
            throw new IllegalArgumentException("targetBytes must be positive, got " + targetBytes);
        }
        if (maxValues <= 0) {
            throw new IllegalArgumentException("maxValues must be positive, got " + maxValues);
        }
    }
}
