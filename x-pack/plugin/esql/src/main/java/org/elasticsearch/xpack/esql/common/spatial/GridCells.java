/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.common.spatial;

import java.util.Arrays;
import java.util.function.Consumer;

/**
 * Collects the grid cells of one shape into a primitive array, stopping at a limit. Offering a cell past the limit
 * calls {@code onTruncation} once with a message naming the function and marks the collector {@link #full()}, which
 * tilers poll to stop enumerating early. Avoids boxing on the per-document path shared by the evaluators and the
 * fused block loaders.
 */
public final class GridCells {
    private final String functionName;
    private final int maxCells;
    private final Consumer<String> onTruncation;
    private long[] array;
    private int size;
    private boolean full;

    public GridCells(String functionName, int maxCells, Consumer<String> onTruncation) {
        this.functionName = functionName;
        this.maxCells = maxCells;
        this.onTruncation = onTruncation;
        this.array = new long[Math.min(maxCells, 16)];
    }

    public void add(long cell) {
        if (full) {
            return;
        }
        if (size >= maxCells) {
            full = true;
            onTruncation.accept(functionName + " generated more than " + maxCells + " grid cells");
            return;
        }
        if (size == array.length) {
            array = Arrays.copyOf(array, Math.min(size * 2, maxCells));
        }
        array[size++] = cell;
    }

    public boolean full() {
        return full;
    }

    public long[] toArray() {
        return size == array.length ? array : Arrays.copyOf(array, size);
    }
}
