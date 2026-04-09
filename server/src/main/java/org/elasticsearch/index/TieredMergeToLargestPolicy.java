/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A merge policy that favors merging small segments into large segments.
 * <p>
 * This is optimized for cases where the merge cost model is asymmetric,
 * particularly for IVF/DiskBBQ vector indices where:
 * <ul>
 *   <li>A dominant segment allows reuse of existing K-means centroids (cheap INSERTION strategy)</li>
 *   <li>Balanced merges require full centroid rebuild from scratch (expensive FULL_REBUILD)</li>
 *   <li>Fewer, larger segments reduce search cost (fewer IVF indices to probe)</li>
 * </ul>
 * <p>
 * This also benefits HNSW indices where insert cost is O(log n) per vector (expensive)
 * while copy cost is O(1) per vector (cheap).
 * <p>
 * Key differences from {@link org.apache.lucene.index.TieredMergePolicy}:
 * <ul>
 *   <li>Prefers high-skew merges (small into large) instead of balanced merges</li>
 *   <li>Uses ratio-based write amplification control ({@code maxRatio})</li>
 *   <li>Groups segments by inferred tier based on size, processes from highest tier down</li>
 *   <li>Cross-tier merging: absorbs lower-tier segments into a higher tier's largest segment</li>
 * </ul>
 */
public class TieredMergeToLargestPolicy extends MergePolicy {

    private long maxMergedSegmentBytes = 5 * 1024 * 1024 * 1024L;
    private long floorSegmentBytes = 2 * 1024 * 1024L;
    private double segsPerTier = 10.0;
    private double maxRatio = 5.0;
    private int maxMergeAtOnce = 10;
    private double deletesPctAllowed = 20.0;
    private double forceMergeDeletesPctAllowed = 10.0;

    public TieredMergeToLargestPolicy() {}

    /**
     * Maximum sized segment to produce during normal merging.
     */
    public TieredMergeToLargestPolicy setMaxMergedSegmentMB(double v) {
        if (v < 0.0) {
            throw new IllegalArgumentException("maxMergedSegmentMB must be >= 0 (got " + v + ")");
        }
        v *= 1024 * 1024;
        maxMergedSegmentBytes = v > Long.MAX_VALUE ? Long.MAX_VALUE : (long) v;
        return this;
    }

    public double getMaxMergedSegmentMB() {
        return maxMergedSegmentBytes / 1024.0 / 1024.0;
    }

    /**
     * Segments smaller than this are "rounded up" to this size for tier calculation.
     */
    public TieredMergeToLargestPolicy setFloorSegmentMB(double v) {
        if (v <= 0.0) {
            throw new IllegalArgumentException("floorSegmentMB must be > 0.0 (got " + v + ")");
        }
        v *= 1024 * 1024;
        floorSegmentBytes = v > Long.MAX_VALUE ? Long.MAX_VALUE : (long) v;
        return this;
    }

    public double getFloorSegmentMB() {
        return floorSegmentBytes / 1024.0 / 1024.0;
    }

    /**
     * Sets the allowed number of segments per tier.
     */
    public TieredMergeToLargestPolicy setSegmentsPerTier(double v) {
        if (v < 2.0) {
            throw new IllegalArgumentException("segmentsPerTier must be >= 2.0 (got " + v + ")");
        }
        segsPerTier = v;
        return this;
    }

    public double getSegmentsPerTier() {
        return segsPerTier;
    }

    /**
     * Sets the maximum ratio of the largest segment size to the sum of candidate sizes
     * before the large segment is considered "full" and stops accepting merges.
     * This bounds write amplification: when the largest segment is much bigger than
     * the candidates combined, candidates merge among themselves instead.
     */
    public TieredMergeToLargestPolicy setMaxRatio(double v) {
        if (v < 1.0) {
            throw new IllegalArgumentException("maxRatio must be >= 1.0 (got " + v + ")");
        }
        maxRatio = v;
        return this;
    }

    public double getMaxRatio() {
        return maxRatio;
    }

    /**
     * Maximum number of segments to merge at once.
     */
    public TieredMergeToLargestPolicy setMaxMergeAtOnce(int v) {
        if (v < 2) {
            throw new IllegalArgumentException("maxMergeAtOnce must be >= 2 (got " + v + ")");
        }
        maxMergeAtOnce = v;
        return this;
    }

    public int getMaxMergeAtOnce() {
        return maxMergeAtOnce;
    }

    /**
     * Sets the maximum percentage of deleted documents that is tolerated in the index.
     */
    public TieredMergeToLargestPolicy setDeletesPctAllowed(double v) {
        if (v < 0 || v > 50) {
            throw new IllegalArgumentException("deletesPctAllowed must be between 0 and 50 (got " + v + ")");
        }
        deletesPctAllowed = v;
        return this;
    }

    public double getDeletesPctAllowed() {
        return deletesPctAllowed;
    }

    /**
     * When forceMergeDeletes is called, we only merge away a segment if its delete
     * percentage is over this threshold.
     */
    public TieredMergeToLargestPolicy setForceMergeDeletesPctAllowed(double v) {
        if (v < 0.0 || v > 100.0) {
            throw new IllegalArgumentException("forceMergeDeletesPctAllowed must be between 0.0 and 100.0 (got " + v + ")");
        }
        forceMergeDeletesPctAllowed = v;
        return this;
    }

    public double getForceMergeDeletesPctAllowed() {
        return forceMergeDeletesPctAllowed;
    }

    @Override
    protected long maxFullFlushMergeSize() {
        return floorSegmentBytes;
    }

    /**
     * Segment size and doc count info for merge selection.
     */
    private record SegmentSizeAndDocs(SegmentCommitInfo info, long sizeInBytes, int delCount, int maxDoc, String name) {
        SegmentSizeAndDocs(SegmentCommitInfo info, long sizeInBytes, int delCount) {
            this(info, sizeInBytes, delCount, info.info.maxDoc(), info.info.name);
        }
    }

    /**
     * Infer the tier of a segment based on its size.
     * Tier 0: &lt; floorSegmentBytes * segsPerTier
     * Tier 1: &lt; floorSegmentBytes * segsPerTier^2
     * Tier N: &lt; floorSegmentBytes * segsPerTier^(N+1)
     */
    private int inferTier(long segmentBytes) {
        int tier = 0;
        long boundary = (long) (floorSegmentBytes * segsPerTier);
        while (segmentBytes >= boundary && boundary < maxMergedSegmentBytes) {
            boundary = (long) (boundary * segsPerTier);
            tier++;
        }
        return tier;
    }

    /**
     * Get segments sorted by size (largest first) with size and delete info.
     */
    private List<SegmentSizeAndDocs> getSortedBySegmentSize(SegmentInfos infos, MergeContext mergeContext) throws IOException {
        List<SegmentSizeAndDocs> sortedBySize = new ArrayList<>();
        for (SegmentCommitInfo info : infos) {
            sortedBySize.add(new SegmentSizeAndDocs(info, size(info, mergeContext), mergeContext.numDeletesToMerge(info)));
        }
        sortedBySize.sort((o1, o2) -> {
            int cmp = Long.compare(o2.sizeInBytes, o1.sizeInBytes);
            if (cmp == 0) {
                cmp = o1.name.compareTo(o2.name);
            }
            return cmp;
        });
        return sortedBySize;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();

        // Get segments sorted by size
        List<SegmentSizeAndDocs> sortedInfos = getSortedBySegmentSize(infos, mergeContext);

        // Remove segments that are already merging
        Iterator<SegmentSizeAndDocs> iter = sortedInfos.iterator();
        long totIndexBytes = 0;
        int totalDelDocs = 0;
        int totalMaxDoc = 0;

        while (iter.hasNext()) {
            SegmentSizeAndDocs segSizeDocs = iter.next();
            if (merging.contains(segSizeDocs.info)) {
                iter.remove();
                totalMaxDoc += segSizeDocs.maxDoc - segSizeDocs.delCount;
            } else {
                totIndexBytes += segSizeDocs.sizeInBytes;
                totalDelDocs += segSizeDocs.delCount;
                totalMaxDoc += segSizeDocs.maxDoc;
            }
        }

        if (sortedInfos.isEmpty()) {
            return null;
        }

        // Group segments by inferred tier
        Map<Integer, List<SegmentSizeAndDocs>> tierMap = new HashMap<>();
        int maxTier = 0;
        for (SegmentSizeAndDocs seg : sortedInfos) {
            int tier = inferTier(seg.sizeInBytes);
            maxTier = Math.max(maxTier, tier);
            tierMap.computeIfAbsent(tier, k -> new ArrayList<>()).add(seg);
        }

        // Compute allowed segment count (budget)
        double allowedSegCount = computeAllowedSegmentCount(totIndexBytes, sortedInfos);

        // Check if we need to merge
        int allowedDelCount = (int) (deletesPctAllowed * totalMaxDoc / 100);

        if (sortedInfos.size() <= allowedSegCount && totalDelDocs <= allowedDelCount) {
            return null;
        }

        // Find best merge, processing from highest tier down
        return findBestMerge(tierMap, maxTier, mergeContext, merging);
    }

    private double computeAllowedSegmentCount(long totIndexBytes, List<SegmentSizeAndDocs> segments) {
        long minSegmentBytes = Long.MAX_VALUE;
        for (SegmentSizeAndDocs seg : segments) {
            minSegmentBytes = Math.min(minSegmentBytes, seg.sizeInBytes);
        }

        double allowedSegCount = 0;
        int mergeFactor = (int) segsPerTier;
        long levelSize = Math.max(minSegmentBytes, floorSegmentBytes);
        long bytesLeft = totIndexBytes;

        while (true) {
            double segCountLevel = bytesLeft / (double) levelSize;
            if (segCountLevel < segsPerTier || levelSize == maxMergedSegmentBytes) {
                allowedSegCount += Math.ceil(segCountLevel);
                break;
            }
            allowedSegCount += segsPerTier;
            bytesLeft -= (long) (segsPerTier * levelSize);
            levelSize = Math.min(maxMergedSegmentBytes, levelSize * mergeFactor);
        }

        return Math.max(allowedSegCount, segsPerTier);
    }

    private MergeSpecification findBestMerge(
        Map<Integer, List<SegmentSizeAndDocs>> tierMap,
        int maxTier,
        MergeContext mergeContext,
        Set<SegmentCommitInfo> merging
    ) throws IOException {
        MergeSpecification spec = null;
        Set<SegmentCommitInfo> toBeMerged = new HashSet<>();

        // Process from highest tier down to find merge opportunities.
        // For each tier, find the largest segment (L) and merge smaller segments into it.
        // This creates the skewed merge patterns that benefit IVF/DiskBBQ (INSERTION strategy)
        // and HNSW (cheap copy of large segment + insert small segments).
        for (int tier = maxTier; tier >= 0; tier--) {
            List<SegmentSizeAndDocs> tierSegments = tierMap.get(tier);
            if (tierSegments == null || tierSegments.size() < 2) {
                continue;
            }

            // Find the largest non-merging segment (L) in this tier
            SegmentSizeAndDocs largest = null;
            for (SegmentSizeAndDocs seg : tierSegments) {
                if (toBeMerged.contains(seg.info)) {
                    continue;
                }
                if (largest == null || seg.sizeInBytes > largest.sizeInBytes) {
                    largest = seg;
                }
            }

            if (largest == null) {
                continue;
            }

            // Gather candidates: other segments from this tier + all from lower tiers.
            // Cross-tier merging is key: it absorbs small segments into the dominant segment faster,
            // creating the dominant-segment patterns that trigger DiskBBQ's INSERTION strategy.
            List<SegmentSizeAndDocs> candidates = new ArrayList<>();
            long candidatesSize = 0;

            // Add from current tier (excluding largest)
            for (SegmentSizeAndDocs seg : tierSegments) {
                if (seg != largest && toBeMerged.contains(seg.info) == false && candidates.size() < maxMergeAtOnce - 1) {
                    candidates.add(seg);
                    candidatesSize += seg.sizeInBytes;
                }
            }

            // Add from lower tiers (cross-tier merging)
            for (int lowerTier = tier - 1; lowerTier >= 0 && candidates.size() < maxMergeAtOnce - 1; lowerTier--) {
                List<SegmentSizeAndDocs> lowerTierSegs = tierMap.get(lowerTier);
                if (lowerTierSegs != null) {
                    for (SegmentSizeAndDocs seg : lowerTierSegs) {
                        if (toBeMerged.contains(seg.info) == false && candidates.size() < maxMergeAtOnce - 1) {
                            candidates.add(seg);
                            candidatesSize += seg.sizeInBytes;
                        }
                    }
                }
            }

            if (candidates.isEmpty()) {
                continue;
            }

            // Check ratio: can L accept this merge?
            // If ratio exceeds maxRatio, L is "full" — merge candidates among themselves instead.
            // This bounds write amplification while still favoring skewed merges.
            double ratio = candidatesSize > 0 ? (double) largest.sizeInBytes / candidatesSize : 0;

            List<SegmentCommitInfo> mergeSegments = new ArrayList<>();

            if (ratio <= maxRatio) {
                // Merge candidates into L (L is copied, candidates are inserted)
                mergeSegments.add(largest.info);
                for (SegmentSizeAndDocs seg : candidates) {
                    mergeSegments.add(seg.info);
                }

                if (verbose(mergeContext)) {
                    message(
                        "  TieredMergeToLargest: tier="
                            + tier
                            + " ratio="
                            + String.format(Locale.ROOT, "%.2f", ratio)
                            + " largest="
                            + String.format(Locale.ROOT, "%.2f MB", largest.sizeInBytes / 1024.0 / 1024.0)
                            + " candidates="
                            + candidates.size(),
                        mergeContext
                    );
                }
            } else {
                // L is "full" — ratio exceeded
                // Merge candidates among themselves (without L)
                if (candidates.size() >= 2) {
                    for (SegmentSizeAndDocs seg : candidates) {
                        mergeSegments.add(seg.info);
                    }

                    if (verbose(mergeContext)) {
                        message(
                            "  TieredMergeToLargest (L full): tier="
                                + tier
                                + " ratio="
                                + String.format(Locale.ROOT, "%.2f", ratio)
                                + " > maxRatio="
                                + maxRatio
                                + " merging "
                                + candidates.size()
                                + " candidates without L",
                            mergeContext
                        );
                    }
                }
            }

            if (mergeSegments.size() >= 2) {
                if (spec == null) {
                    spec = new MergeSpecification();
                }
                spec.add(new OneMerge(mergeSegments));
                toBeMerged.addAll(mergeSegments);
            }
        }

        return spec;
    }

    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos infos,
        int maxSegmentCount,
        Map<SegmentCommitInfo, Boolean> segmentsToMerge,
        MergeContext mergeContext
    ) throws IOException {
        // For force merge, favor merging small segments into large ones
        List<SegmentSizeAndDocs> eligible = new ArrayList<>();
        final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();

        for (SegmentCommitInfo info : infos) {
            Boolean isOriginal = segmentsToMerge.get(info);
            if (isOriginal != null && merging.contains(info) == false) {
                eligible.add(new SegmentSizeAndDocs(info, size(info, mergeContext), mergeContext.numDeletesToMerge(info)));
            }
        }

        if (eligible.size() <= maxSegmentCount) {
            return null;
        }

        // Sort by size descending
        eligible.sort((a, b) -> Long.compare(b.sizeInBytes, a.sizeInBytes));

        MergeSpecification spec = new MergeSpecification();

        // Merge from smallest upward, favoring merging small into large
        int idx = eligible.size() - 1;
        while (eligible.size() > maxSegmentCount && idx > 0) {
            List<SegmentCommitInfo> toMerge = new ArrayList<>();
            long mergeSize = 0;

            // Take smallest segments up to maxMergeAtOnce
            while (idx >= 0 && toMerge.size() < maxMergeAtOnce && mergeSize < maxMergedSegmentBytes) {
                SegmentSizeAndDocs seg = eligible.get(idx);
                if (mergeSize + seg.sizeInBytes <= maxMergedSegmentBytes || toMerge.isEmpty()) {
                    toMerge.add(seg.info);
                    mergeSize += seg.sizeInBytes;
                    eligible.remove(idx);
                    idx--;
                } else {
                    break;
                }
            }

            if (toMerge.size() >= 2) {
                spec.add(new OneMerge(toMerge));
            }
        }

        return spec.merges.isEmpty() ? null : spec;
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos infos, MergeContext mergeContext) throws IOException {
        final Set<SegmentCommitInfo> merging = mergeContext.getMergingSegments();
        List<SegmentSizeAndDocs> eligible = new ArrayList<>();

        for (SegmentCommitInfo info : infos) {
            int delCount = mergeContext.numDeletesToMerge(info);
            double pctDeletes = 100.0 * delCount / info.info.maxDoc();
            if (pctDeletes > forceMergeDeletesPctAllowed && merging.contains(info) == false) {
                eligible.add(new SegmentSizeAndDocs(info, size(info, mergeContext), delCount));
            }
        }

        if (eligible.isEmpty()) {
            return null;
        }

        // Sort by size descending
        eligible.sort((a, b) -> Long.compare(b.sizeInBytes, a.sizeInBytes));

        // Group by tier and merge
        Map<Integer, List<SegmentSizeAndDocs>> tierMap = new HashMap<>();
        int maxTier = 0;
        for (SegmentSizeAndDocs seg : eligible) {
            int tier = inferTier(seg.sizeInBytes);
            maxTier = Math.max(maxTier, tier);
            tierMap.computeIfAbsent(tier, k -> new ArrayList<>()).add(seg);
        }

        return findBestMerge(tierMap, maxTier, mergeContext, merging);
    }

    @Override
    public String toString() {
        return "["
            + getClass().getSimpleName()
            + ": maxMergedSegmentMB="
            + getMaxMergedSegmentMB()
            + ", floorSegmentMB="
            + getFloorSegmentMB()
            + ", segsPerTier="
            + segsPerTier
            + ", maxRatio="
            + maxRatio
            + ", maxMergeAtOnce="
            + maxMergeAtOnce
            + ", deletesPctAllowed="
            + deletesPctAllowed
            + "]";
    }
}
