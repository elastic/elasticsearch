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
     * above the size budget stay standalone and the rest are spread across the leftover groups balanced by leaf
     * count with the size budget as a cap, which shares the per-file open cost evenly without a straggler group.
     * When sizes are unknown, all splits are spread by leaf count.
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
        groups.addAll(spreadLeastLoaded(small, smallGroups, allHaveSize, targetGroupSizeBytes));
        return groups;
    }

    /**
     * Distributes {@code leaves} across exactly {@code groupCount} groups. The largest leaves seed the groups
     * one-per-group, then each remaining leaf goes to the group holding the fewest leaves, so groups stay balanced
     * by count and the per-file open cost (which dominates many-small-file scans) is spread evenly rather than
     * concentrated in one group. Byte load only breaks ties between groups with an equal leaf count, and a group
     * that has already reached {@code targetGroupSizeBytes} is skipped while any other group still has room, so a
     * group is never grown past the size budget while a lighter group is available. Seeding the groups up front
     * guarantees {@code groupCount} non-empty groups even when several leaves report a zero (or unknown) size.
     * When {@code bySize} is false the leaves have no known size, so the budget and byte tie-break do not apply and
     * leaves are spread purely by count.
     *
     * <p>Precondition: {@code 1 <= groupCount <= leaves.size()}. Violating this will throw
     * {@link IndexOutOfBoundsException} during the seed phase.
     */
    private static List<List<ExternalSplit>> spreadLeastLoaded(
        List<ExternalSplit> leaves,
        int groupCount,
        boolean bySize,
        long targetGroupSizeBytes
    ) {
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
            int target = 0;
            for (int g = 1; g < groupCount; g++) {
                if (preferred(groups, loads, g, target, bySize, targetGroupSizeBytes)) {
                    target = g;
                }
            }
            groups.get(target).add(leaf);
            loads[target] += bySize ? leaf.estimatedSizeInBytes() : 1;
        }
        return groups;
    }

    /**
     * Returns true when {@code candidate} is a better target for the next leaf than {@code current}. A group still
     * under the size budget is preferred over one that has reached it, so no group grows past the budget while
     * another has room; if every group is at the budget the tie-break below still places the leaf somewhere. Among
     * groups on the same side of the budget the one holding fewer leaves wins (count balance), and byte load only
     * separates groups with an equal leaf count. Byte load and the budget are ignored when {@code bySize} is false
     * because unsized leaves carry no budget to respect.
     */
    private static boolean preferred(
        List<List<ExternalSplit>> groups,
        long[] loads,
        int candidate,
        int current,
        boolean bySize,
        long targetGroupSizeBytes
    ) {
        boolean candidateHasRoom = bySize == false || loads[candidate] < targetGroupSizeBytes;
        boolean currentHasRoom = bySize == false || loads[current] < targetGroupSizeBytes;
        if (candidateHasRoom != currentHasRoom) {
            return candidateHasRoom;
        }
        int candidateCount = groups.get(candidate).size();
        int currentCount = groups.get(current).size();
        if (candidateCount != currentCount) {
            return candidateCount < currentCount;
        }
        return bySize && loads[candidate] < loads[current];
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
