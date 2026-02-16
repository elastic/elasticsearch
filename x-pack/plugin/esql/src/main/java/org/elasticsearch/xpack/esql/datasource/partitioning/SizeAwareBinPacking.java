/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.partitioning;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Size-aware split grouping using first-fit decreasing (FFD) bin-packing.
 *
 * <p><b>Affinity handling:</b>
 * <ul>
 *   <li>{@link NodeAffinity#required() Required} splits are always grouped strictly by node —
 *       never mixed with splits from other nodes.</li>
 *   <li>{@link NodeAffinity#required() Preferred} splits are grouped by node when
 *       {@link DistributionHints#preferDataLocality()} is true (best-effort). When the flag
 *       is false, preference is ignored.</li>
 *   <li>Splits with {@link NodeAffinity#NONE} are grouped purely by size.</li>
 * </ul>
 *
 * <p><b>Bin-packing algorithm:</b>
 * <ol>
 *   <li><b>Trivial cases:</b> empty splits → empty list; fewer splits than
 *       {@code targetPartitions} → one split per group</li>
 *   <li><b>Size check:</b> if no split has {@link DataSourceSplit#estimatedBytes()},
 *       falls back to count-based round-robin</li>
 *   <li><b>FFD bin-packing:</b> sorts splits by size descending, assigns each to the
 *       smallest-total bin that fits, with greedy fallback for oversized splits</li>
 * </ol>
 *
 * @see SplitPartitioner
 * @see NodeAffinity
 */
final class SizeAwareBinPacking {

    private SizeAwareBinPacking() {}

    /**
     * Group splits into balanced bins for partition creation.
     *
     * <p>The generic type parameter preserves the caller's concrete split type in the return value.
     *
     * @param splits The discovered splits to group
     * @param hints Distribution hints from the physical planner
     * @param <S> The concrete split type
     * @return Groups of splits, each group becoming one partition
     */
    static <S extends DataSourceSplit> List<List<S>> groupSplits(List<S> splits, DistributionHints hints) {
        if (splits.isEmpty()) {
            return List.of();
        }

        int targetPartitions = hints.targetPartitions();

        // ---- Step 1: Classify splits by affinity type ----
        //
        // The three affinity types have different grouping rules:
        // Required — data only exists on one node (e.g., local log files). These splits must
        // NEVER be mixed with splits from other nodes, regardless of any flags.
        // Soft — data is faster to read locally but accessible from anywhere (e.g., HDFS
        // block with a local replica). Grouped by node only when preferDataLocality
        // is true; otherwise treated the same as no-affinity.
        // None — data is equally accessible from any node (e.g., S3 objects). Grouped
        // purely by size for balance.
        List<S> requiredAffinity = new ArrayList<>();
        List<S> softAffinity = new ArrayList<>();
        List<S> noAffinity = new ArrayList<>();
        for (S split : splits) {
            NodeAffinity aff = split.nodeAffinity();
            if (aff.hasAffinity() && aff.required()) {
                requiredAffinity.add(split);
            } else if (aff.hasAffinity()) {
                softAffinity.add(split);
            } else {
                noAffinity.add(split);
            }
        }

        // ---- Step 2: Choose grouping strategy based on what affinity types are present ----
        //
        // The strategies are ordered from simplest to most complex. We pick the simplest one
        // that handles the affinity types in the input:
        //
        // No required + no active soft → pure size-based packing (FFD or round-robin)
        // No required + active soft → group by preferred node, then pack within each group
        // Required present → isolate required splits by node first, then pack the rest

        if (requiredAffinity.isEmpty()) {
            if (softAffinity.isEmpty() == false && hints.preferDataLocality()) {
                // Soft affinity with locality enabled: group by preferred node, then bin-pack within
                return groupByAffinityThenPack(splits, targetPartitions, hints);
            }
            // No affinity constraints active — pure size-based grouping.
            // When there are fewer splits than partitions, give each split its own group
            // to maximize parallelism (no point packing 2 splits into 1 bin with 5 bins available).
            if (splits.size() <= targetPartitions) {
                return splits.stream().map(List::of).toList();
            }
            return packSplits(splits, targetPartitions, hints);
        }

        // Required affinity present — this is the most complex path. Required splits get
        // their own node-isolated groups first, then soft/no-affinity splits fill the
        // remaining partition budget.
        return groupWithRequiredAffinity(requiredAffinity, softAffinity, noAffinity, targetPartitions, hints);
    }

    /**
     * Handle the case where some splits have required (hard) node affinity.
     *
     * <p>Required-affinity splits are grouped strictly by node. Soft and no-affinity splits
     * are packed into the remaining partition budget.
     */
    private static <S extends DataSourceSplit> List<List<S>> groupWithRequiredAffinity(
        List<S> requiredSplits,
        List<S> softSplits,
        List<S> noAffinitySplits,
        int targetPartitions,
        DistributionHints hints
    ) {
        // ---- Phase 1: Pack required-affinity splits by node ----
        //
        // Required splits are grouped strictly by node ID — splits from different nodes are
        // NEVER mixed. Within a single node's splits, we still bin-pack by size to allow
        // parallelism (e.g., 10 local log files on node-1 can be split into 3 partitions,
        // all pinned to node-1).
        //
        // Each node gets a proportional share of the partition budget based on how many
        // required splits it has. Example: 6 required splits (4 on node-1, 2 on node-2)
        // with targetPartitions=6 → node-1 gets 4 partitions, node-2 gets 2.
        Map<String, List<S>> requiredByNode = groupByNode(requiredSplits);

        List<List<S>> result = new ArrayList<>();

        int requiredPartitions = 0;
        for (List<S> nodeGroup : requiredByNode.values()) {
            // Proportional budget: (node's splits / total required splits) * targetPartitions
            int nodeTarget = Math.max(1, (int) Math.round((double) nodeGroup.size() / requiredSplits.size() * targetPartitions));
            if (nodeGroup.size() <= nodeTarget) {
                // Few enough splits to fit in one partition — keep them together
                result.add(nodeGroup);
                requiredPartitions++;
            } else {
                // Many splits on this node — bin-pack them into multiple partitions
                List<List<S>> packed = packSplits(nodeGroup, nodeTarget, hints);
                result.addAll(packed);
                requiredPartitions += packed.size();
            }
        }

        // ---- Phase 2: Pack remaining (soft + no-affinity) splits ----
        //
        // Soft and no-affinity splits share the leftover partition budget (targetPartitions
        // minus what the required splits consumed). This prevents required splits from
        // starving the rest of the query of parallelism while still honoring the overall
        // target partition count.
        List<S> remaining = new ArrayList<>(softSplits.size() + noAffinitySplits.size());
        remaining.addAll(softSplits);
        remaining.addAll(noAffinitySplits);

        if (remaining.isEmpty() == false) {
            int remainingTarget = Math.max(1, targetPartitions - requiredPartitions);
            if (hints.preferDataLocality() && softSplits.isEmpty() == false) {
                // Soft affinity with locality enabled — group by preferred node first
                result.addAll(groupByAffinityThenPack(remaining, remainingTarget, hints));
            } else if (remaining.size() <= remainingTarget) {
                // Fewer remaining splits than budget — one split per partition
                remaining.stream().map(List::of).forEach(result::add);
            } else {
                // Pure size-based packing for the remaining splits
                result.addAll(packSplits(remaining, remainingTarget, hints));
            }
        }

        return result;
    }

    /**
     * Group splits by node affinity (best-effort), then bin-pack within each group.
     *
     * <p>Used for soft-affinity splits when {@link DistributionHints#preferDataLocality()} is true.
     */
    private static <S extends DataSourceSplit> List<List<S>> groupByAffinityThenPack(
        List<S> splits,
        int targetPartitions,
        DistributionHints hints
    ) {
        Map<String, List<S>> byNode = groupByNode(splits);

        if (byNode.size() == 1) {
            // All splits prefer the same node (or have no affinity) — grouping by node is
            // trivially satisfied, so just pack by size for the best balance.
            return packSplits(splits, targetPartitions, hints);
        }

        // Multiple preferred nodes — distribute the partition budget proportionally.
        // Example: 6 HDFS blocks (4 prefer node-1, 2 prefer node-2), targetPartitions=3
        // → node-1 gets round(4/6 * 3) = 2 partitions
        // → node-2 gets round(2/6 * 3) = 1 partition
        // Each node's blocks are then bin-packed independently within their budget.
        List<List<S>> result = new ArrayList<>();
        for (List<S> affinityGroup : byNode.values()) {
            int groupTarget = Math.max(1, (int) Math.round((double) affinityGroup.size() / splits.size() * targetPartitions));
            if (affinityGroup.size() <= groupTarget) {
                // Small enough to fit in one partition — keep the affinity group together
                result.add(affinityGroup);
            } else {
                result.addAll(packSplits(affinityGroup, groupTarget, hints));
            }
        }
        return result;
    }

    /**
     * Group splits by their node affinity key. Splits with no affinity are keyed by empty
     * string, so they all land in the same group and are packed together by size.
     *
     * <p>Uses {@link LinkedHashMap} to preserve insertion order — this makes test output
     * deterministic and ensures the first node encountered gets the first partition slots.
     */
    private static <S extends DataSourceSplit> Map<String, List<S>> groupByNode(List<S> splits) {
        Map<String, List<S>> byNode = new LinkedHashMap<>();
        for (S split : splits) {
            NodeAffinity aff = split.nodeAffinity();
            String key = aff.hasAffinity() ? aff.nodeId() : "";
            byNode.computeIfAbsent(key, k -> new ArrayList<>()).add(split);
        }
        return byNode;
    }

    /**
     * Pack splits into bins: size-aware FFD if sizes are available, count-based round-robin otherwise.
     */
    private static <S extends DataSourceSplit> List<List<S>> packSplits(List<S> splits, int targetPartitions, DistributionHints hints) {
        // Decide between size-aware packing and count-based round-robin.
        //
        // If at least one split reports a size, we use FFD bin-packing (size-aware). Splits
        // without sizes are treated as 0 bytes and naturally fill gaps in larger bins.
        //
        // If NO split has size information (e.g., API endpoints, database cursors), we fall
        // back to round-robin by count — the best we can do without size estimates.
        boolean hasSizes = false;
        long totalBytes = 0;
        for (S split : splits) {
            var est = split.estimatedBytes();
            if (est.isPresent()) {
                hasSizes = true;
                totalBytes += est.getAsLong();
            }
        }

        if (hasSizes == false) {
            return roundRobin(splits, targetPartitions);
        }

        return ffdBinPack(splits, targetPartitions, totalBytes, hints);
    }

    /**
     * Count-based round-robin: distributes splits evenly across bins by count.
     *
     * <p>Used when no split has size information. Consecutive splits are grouped into
     * contiguous slices of {@code ceil(splits.size() / partitionCount)}. This produces
     * at most 1 difference in count between the largest and smallest bin.
     *
     * <p>Example: 7 splits across 3 bins → slices of ceil(7/3)=3 → [0..2], [3..5], [6].
     */
    private static <S extends DataSourceSplit> List<List<S>> roundRobin(List<S> splits, int targetPartitions) {
        int partitionCount = Math.min(targetPartitions, splits.size());
        int tasksPerPartition = (splits.size() + partitionCount - 1) / partitionCount; // ceil division

        List<List<S>> result = new ArrayList<>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            int start = i * tasksPerPartition;
            int end = Math.min(start + tasksPerPartition, splits.size());
            if (start < end) {
                result.add(splits.subList(start, end));
            }
        }
        return result;
    }

    /**
     * First-fit decreasing (FFD) bin-packing.
     *
     * <p>FFD is a classic approximation algorithm for the bin-packing problem. It works by:
     * <ol>
     *   <li>Sorting splits by size descending (largest first)</li>
     *   <li>For each split, finding the smallest-total bin where adding the split stays within
     *       the max bin size ("best fit")</li>
     *   <li>If no bin has room (the split is oversized), falling back to the smallest-total bin
     *       anyway ("greedy fallback") — we can't split a split, so we accept the overflow</li>
     * </ol>
     *
     * <p>FFD guarantees bins are within 11/9 * OPT + 6/9 of the optimal number of bins.
     * In practice, it produces well-balanced output for typical file-size distributions.
     *
     * <p>The "best fit" strategy (smallest bin that still has room) is preferred over "first fit"
     * (first bin that has room) because it keeps bins more evenly loaded. This matters when
     * partitions map to cluster nodes — even load means faster overall query completion.
     */
    private static <S extends DataSourceSplit> List<List<S>> ffdBinPack(
        List<S> splits,
        int targetPartitions,
        long totalBytes,
        DistributionHints hints
    ) {
        // Sort by estimated bytes descending — placing the largest splits first gives FFD the
        // most flexibility when filling remaining space with smaller splits.
        List<S> sorted = new ArrayList<>(splits);
        sorted.sort((a, b) -> {
            long aBytes = a.estimatedBytes().orElse(0L);
            long bBytes = b.estimatedBytes().orElse(0L);
            return Long.compare(bBytes, aBytes);
        });

        // Max bin size: use the explicit maxPartitionBytes from hints if set, otherwise
        // compute an ideal target as ceil(totalBytes / targetPartitions). The ceil division
        // ensures we don't systematically create one extra bin for the remainder.
        long maxBinSize = hints.maxPartitionBytes().orElse(totalBytes > 0 ? (totalBytes + targetPartitions - 1) / targetPartitions : 1L);

        // Pre-allocate exactly targetPartitions bins (or fewer if we have fewer splits).
        // Empty bins are filtered out at the end, so over-allocating is harmless.
        int binCount = Math.min(targetPartitions, splits.size());
        List<Bin<S>> bins = new ArrayList<>(binCount);
        for (int i = 0; i < binCount; i++) {
            bins.add(new Bin<>());
        }

        // Assign each split to a bin using best-fit-decreasing
        for (S split : sorted) {
            long splitBytes = split.estimatedBytes().orElse(0L);

            // Scan all bins in one pass to find both:
            // bestFit — the smallest-total bin where the split fits within maxBinSize
            // smallest — the overall smallest-total bin (greedy fallback for oversized splits)
            Bin<S> bestFit = null;
            Bin<S> smallest = bins.get(0);
            for (Bin<S> bin : bins) {
                if (bin.totalBytes < smallest.totalBytes) {
                    smallest = bin;
                }
                if (bin.totalBytes + splitBytes <= maxBinSize) {
                    if (bestFit == null || bin.totalBytes < bestFit.totalBytes) {
                        bestFit = bin;
                    }
                }
            }

            // Best fit when possible; otherwise greedy fallback to the smallest bin.
            // The greedy fallback handles the case where a single split exceeds maxBinSize
            // (e.g., a 500 MB Parquet file with a 300 MB target). We can't split it, so we
            // accept the overflow and ensure it at least lands in the least-loaded bin.
            Bin<S> target = bestFit != null ? bestFit : smallest;
            target.add(split, splitBytes);
        }

        // Collect non-empty bins (bins may be empty if targetPartitions > splits.size())
        List<List<S>> result = new ArrayList<>(binCount);
        for (Bin<S> bin : bins) {
            if (bin.splits.isEmpty() == false) {
                result.add(bin.splits);
            }
        }
        return result;
    }

    /**
     * A bin accumulating splits and their total byte size during FFD bin-packing.
     */
    private static final class Bin<S> {
        final List<S> splits = new ArrayList<>();
        long totalBytes;

        void add(S split, long bytes) {
            splits.add(split);
            totalBytes += bytes;
        }
    }
}
