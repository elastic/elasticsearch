/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Groups many small {@link ExternalSplit}s into {@link CoalescedSplit}s to
 * reduce scheduling overhead. Uses greedy bin-packing by size when all splits
 * report a positive {@code estimatedSizeInBytes()}, and falls back to
 * count-based grouping otherwise.
 */
public final class SplitCoalescer {

    public static final long DEFAULT_TARGET_GROUP_SIZE_BYTES = 128 * 1024 * 1024; // 128 MB
    public static final int DEFAULT_TARGET_GROUP_COUNT = 8;
    public static final int COALESCING_THRESHOLD = 32;

    private SplitCoalescer() {}

    public static List<ExternalSplit> coalesce(List<ExternalSplit> splits) {
        return coalesce(splits, DEFAULT_TARGET_GROUP_SIZE_BYTES, DEFAULT_TARGET_GROUP_COUNT);
    }

    public static List<ExternalSplit> coalesce(List<ExternalSplit> splits, long targetGroupSizeBytes, int targetGroupCount) {
        if (splits == null) {
            throw new IllegalArgumentException("splits cannot be null");
        }
        if (splits.size() <= COALESCING_THRESHOLD) {
            return splits;
        }
        if (targetGroupCount <= 0) {
            throw new IllegalArgumentException("targetGroupCount must be positive, got: " + targetGroupCount);
        }
        if (targetGroupSizeBytes <= 0) {
            throw new IllegalArgumentException("targetGroupSizeBytes must be positive, got: " + targetGroupSizeBytes);
        }

        boolean allHaveSize = true;
        for (ExternalSplit split : splits) {
            if (split.estimatedSizeInBytes() < 0) {
                allHaveSize = false;
                break;
            }
        }

        if (allHaveSize) {
            return coalesceBySizeGreedy(splits, targetGroupSizeBytes);
        }
        return coalesceByCount(splits, targetGroupCount);
    }

    private static List<ExternalSplit> coalesceBySizeGreedy(List<ExternalSplit> splits, long targetGroupSizeBytes) {
        List<ExternalSplit> sorted = new ArrayList<>(splits);
        sorted.sort(Comparator.comparingLong(ExternalSplit::estimatedSizeInBytes).reversed());

        List<List<ExternalSplit>> bins = new ArrayList<>();
        List<Long> binSizes = new ArrayList<>();

        for (ExternalSplit split : sorted) {
            long size = split.estimatedSizeInBytes();
            if (size >= targetGroupSizeBytes) {
                bins.add(new ArrayList<>(List.of(split)));
                binSizes.add(size);
                continue;
            }

            int bestBin = -1;
            long bestRemaining = Long.MAX_VALUE;
            for (int i = 0; i < bins.size(); i++) {
                long remaining = targetGroupSizeBytes - binSizes.get(i);
                if (remaining >= size && remaining < bestRemaining) {
                    bestBin = i;
                    bestRemaining = remaining;
                }
            }

            if (bestBin >= 0) {
                bins.get(bestBin).add(split);
                binSizes.set(bestBin, binSizes.get(bestBin) + size);
            } else {
                bins.add(new ArrayList<>(List.of(split)));
                binSizes.add(size);
            }
        }

        return buildResult(bins);
    }

    private static List<ExternalSplit> coalesceByCount(List<ExternalSplit> splits, int targetGroupCount) {
        int groupSize = Math.max(1, (splits.size() + targetGroupCount - 1) / targetGroupCount);
        List<List<ExternalSplit>> bins = new ArrayList<>();

        for (int i = 0; i < splits.size(); i += groupSize) {
            int end = Math.min(i + groupSize, splits.size());
            bins.add(new ArrayList<>(splits.subList(i, end)));
        }

        return buildResult(bins);
    }

    private static List<ExternalSplit> buildResult(List<List<ExternalSplit>> bins) {
        List<ExternalSplit> result = new ArrayList<>(bins.size());
        for (List<ExternalSplit> bin : bins) {
            if (bin.size() == 1) {
                result.add(bin.get(0));
            } else {
                result.add(new CoalescedSplit(bin.get(0).sourceType(), bin));
            }
        }
        return result;
    }
}
