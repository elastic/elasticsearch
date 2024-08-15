/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

public class Util {
    /**
     * Constrains a value between minimum and maximum values
     * (inclusive).
     *
     * @param value the value to constrain
     * @param min   the minimum acceptable value
     * @param max   the maximum acceptable value
     * @return min if value is less than min, max if value is greater
     * than value, otherwise value
     */
    static int boundedBy(int value, int min, int max) {
        assert min < max : min + " vs " + max;
        return Math.min(max, Math.max(min, value));
    }

    static int halfAllocatedProcessors(final int allocatedProcessors) {
        return (allocatedProcessors + 1) / 2;
    }

    static int halfAllocatedProcessorsMaxFive(final int allocatedProcessors) {
        return boundedBy(halfAllocatedProcessors(allocatedProcessors), 1, 5);
    }

    static int halfAllocatedProcessorsMaxTen(final int allocatedProcessors) {
        return boundedBy(halfAllocatedProcessors(allocatedProcessors), 1, 10);
    }

    static int twiceAllocatedProcessors(final int allocatedProcessors) {
        return boundedBy(2 * allocatedProcessors, 2, Integer.MAX_VALUE);
    }

    public static int oneEighthAllocatedProcessors(final int allocatedProcessors) {
        return boundedBy(allocatedProcessors / 8, 1, Integer.MAX_VALUE);
    }

    public static int searchOrGetThreadPoolSize(final int allocatedProcessors) {
        return ((allocatedProcessors * 3) / 2) + 1;
    }

    static int getMaxSnapshotThreadPoolSize(int allocatedProcessors) {
        final ByteSizeValue maxHeapSize = ByteSizeValue.ofBytes(Runtime.getRuntime().maxMemory());
        return getMaxSnapshotThreadPoolSize(allocatedProcessors, maxHeapSize);
    }

    static int getMaxSnapshotThreadPoolSize(int allocatedProcessors, final ByteSizeValue maxHeapSize) {
        // While on larger data nodes, larger snapshot threadpool size improves snapshotting on high latency blob stores,
        // smaller instances can run into OOM issues and need a smaller snapshot threadpool size.
        if (maxHeapSize.compareTo(new ByteSizeValue(750, ByteSizeUnit.MB)) < 0) {
            return halfAllocatedProcessorsMaxFive(allocatedProcessors);
        }
        return 10;
    }
}
