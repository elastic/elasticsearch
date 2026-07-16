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
        return coalesce(splits, targetGroupSizeBytes, targetGroupCount, 1);
    }

    /**
     * Coalesces with the default size and count budgets and a floor of {@code minGroupCount} groups (clamped to
     * the number of input splits), so a scan over many small files keeps at least that many schedulable units.
     */
    public static List<ExternalSplit> coalesce(List<ExternalSplit> splits, int minGroupCount) {
        return coalesce(splits, DEFAULT_TARGET_GROUP_SIZE_BYTES, DEFAULT_TARGET_GROUP_COUNT, minGroupCount);
    }

    /**
     * Coalesces with a floor of {@code minGroupCount} on the number of produced groups, clamped to the number of
     * input splits. A scan over many small files therefore stays spread across at least
     * {@code min(splitCount, minGroupCount)} independently schedulable units, so read concurrency is not collapsed
     * to a single unit when tiny files would otherwise bin-pack into one group. The size budget still raises the
     * group count above the floor when the data needs more bins to stay under {@code targetGroupSizeBytes}; the
     * floor only ever adds groups, never merges past the size budget.
     */
    public static List<ExternalSplit> coalesce(
        List<ExternalSplit> splits,
        long targetGroupSizeBytes,
        int targetGroupCount,
        int minGroupCount
    ) {
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
        if (minGroupCount < 1) {
            throw new IllegalArgumentException("minGroupCount must be positive, got: " + minGroupCount);
        }

        boolean allHaveSize = true;
        for (ExternalSplit split : splits) {
            if (split.estimatedSizeInBytes() < 0) {
                allHaveSize = false;
                break;
            }
        }

        List<List<ExternalSplit>> bins = allHaveSize ? packBySize(splits, targetGroupSizeBytes) : packByCount(splits, targetGroupCount);

        int targetGroups = Math.min(splits.size(), minGroupCount);
        if (bins.size() < targetGroups) {
            bins = floorGroups(splits, targetGroups, targetGroupSizeBytes, allHaveSize);
        }
        return buildResult(bins);
    }

    private static List<List<ExternalSplit>> packBySize(List<ExternalSplit> splits, long targetGroupSizeBytes) {
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

        return bins;
    }

    private static List<List<ExternalSplit>> packByCount(List<ExternalSplit> splits, int targetGroupCount) {
        int groupSize = Math.max(1, (splits.size() + targetGroupCount - 1) / targetGroupCount);
        List<List<ExternalSplit>> bins = new ArrayList<>();

        for (int i = 0; i < splits.size(); i += groupSize) {
            int end = Math.min(i + groupSize, splits.size());
            bins.add(new ArrayList<>(splits.subList(i, end)));
        }

        return bins;
    }

    /**
     * Re-bins the splits into exactly {@code targetGroups} groups when the size/count packing produced fewer,
     * so the number of schedulable units meets the read-parallelism floor. When sizes are known, splits at or
     * above the size budget stay standalone and the rest are spread across the leftover groups by least-loaded
     * assignment (largest first), which yields balanced groups without a straggler; the size budget is preserved
     * because this path only runs when the small splits already fit in fewer than {@code targetGroups} budget-sized
     * bins, so spreading them across strictly more groups keeps every group under the budget. When sizes are
     * unknown, all splits are spread by leaf count.
     */
    private static List<List<ExternalSplit>> floorGroups(
        List<ExternalSplit> splits,
        int targetGroups,
        long targetGroupSizeBytes,
        boolean allHaveSize
    ) {
        List<List<ExternalSplit>> groups = new ArrayList<>(targetGroups);
        List<ExternalSplit> small = new ArrayList<>(splits.size());
        if (allHaveSize) {
            for (ExternalSplit split : splits) {
                if (split.estimatedSizeInBytes() >= targetGroupSizeBytes) {
                    groups.add(new ArrayList<>(List.of(split)));
                } else {
                    small.add(split);
                }
            }
        } else {
            small.addAll(splits);
        }

        int smallGroups = targetGroups - groups.size();
        groups.addAll(spreadLeastLoaded(small, smallGroups, allHaveSize));
        return groups;
    }

    /**
     * Distributes {@code leaves} across exactly {@code groupCount} groups. The largest leaves seed the groups
     * one-per-group, then the rest are placed in the least-loaded group, where load is measured in
     * bytes when {@code bySize} is true and in leaf count otherwise. Ties in load are broken toward the group
     * holding fewer leaves, so leaves that all report the same (or zero) size still spread evenly rather than
     * piling into the first group. Seeding the groups up front guarantees {@code groupCount} non-empty groups
     * even when several leaves report a zero (or unknown) size.
     *
     * <p>Precondition: {@code 1 <= groupCount <= leaves.size()}. Violating this will throw
     * {@link IndexOutOfBoundsException} during the seed phase.
     */
    private static List<List<ExternalSplit>> spreadLeastLoaded(List<ExternalSplit> leaves, int groupCount, boolean bySize) {
        List<ExternalSplit> sorted = new ArrayList<>(leaves);
        if (bySize) {
            sorted.sort(Comparator.comparingLong(ExternalSplit::estimatedSizeInBytes).reversed());
        }

        List<List<ExternalSplit>> groups = new ArrayList<>(groupCount);
        long[] loads = new long[groupCount];
        for (int i = 0; i < groupCount; i++) {
            ExternalSplit leaf = sorted.get(i);
            List<ExternalSplit> group = new ArrayList<>();
            group.add(leaf);
            groups.add(group);
            loads[i] = bySize ? leaf.estimatedSizeInBytes() : 1;
        }

        for (int i = groupCount; i < sorted.size(); i++) {
            ExternalSplit leaf = sorted.get(i);
            int minIdx = 0;
            for (int g = 1; g < groupCount; g++) {
                if (loads[g] < loads[minIdx] || (loads[g] == loads[minIdx] && groups.get(g).size() < groups.get(minIdx).size())) {
                    minIdx = g;
                }
            }
            groups.get(minIdx).add(leaf);
            loads[minIdx] += bySize ? leaf.estimatedSizeInBytes() : 1;
        }
        return groups;
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
