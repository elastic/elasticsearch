/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.HivePartitionDetector;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.PartitionValueMatcher;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Hint-narrowed enumeration of a {@code **} glob over a Hive-partitioned tree: the tree is walked level by level via
 * {@link StorageProvider#listChildren}, and a {@code key=value} folder a partition hint excludes is never listed. On
 * a bare {@code **} glob the textual rewrite has nothing to act on, so this is the only listing-time pruning.
 * Folders are matched by typed value ({@link PartitionValueMatcher}, shared with the read layer), so e.g.
 * {@code month == 6} keeps a zero-padded {@code month=06} and a NULL partition is never pruned.
 *
 * <p><b>Fail-closed.</b> Returning {@code null} means "use the flat listing", always correct since the walk is only
 * an optimisation. The walk declines when the provider cannot enumerate directories (or a directory exceeds
 * {@link #MAX_LISTED_CHILDREN}), when a listing fails mid-walk, and after one probe listing when no level matched a
 * hint (typically a data-column filter), and at the first level no pending hint matches — whether a pending hint
 * is a deeper partition key or a data column is unknowable without listing every level in between, and
 * {@code WHERE <partition> AND <data column>} is the everyday shape, so the walk never descends speculatively.
 * Survivors are finished with one recursive listing each, but only when something was pruned and the survivor
 * count satisfies {@link #MAX_FINISH_SURVIVORS} / {@link #MAX_FINISH_SURVIVOR_FRACTION}; otherwise one flat
 * listing is cheaper.
 *
 * <p><b>Trust boundary.</b> Pruning on {@code year} is sound only if {@code year} really is a partition column, and
 * the walk cannot see inside pruned folders. The caller must therefore verify that every {@link
 * WalkResult#prunedColumns} entry is detected as a partition column of the returned listing, and otherwise discard
 * the walk (see {@code GlobExpander}). A layout broken only inside a pruned subtree stays unverifiable — the same
 * trust the keyed-glob rewrite already places in the layout.
 */
final class PartitionPruningWalk {

    private static final Logger logger = LogManager.getLogger(PartitionPruningWalk.class);

    /**
     * Ceiling on listings in one walk (per-directory and survivor-finishing alike). The walk pays one LIST round
     * trip per directory, which is more expensive per object than the flat listing's one round trip per ~1000
     * objects; this cap bounds the number of requests before the flat fallback, not the total cost.
     * 512 fits realistic Hive trees with a leading-key filter (tens to low hundreds of listings).
     */
    static final int MAX_DIRECTORY_LISTINGS = 512;

    /**
     * Ceiling on one directory's materialized children — {@link StorageProvider#listChildren} buffers a whole
     * directory, unlike the flat listing's lazy iterator. Providers return {@code null} past the limit. 10k bounds
     * the buffer to roughly a megabyte while still covering the widest realistic partition levels (e.g. a date key
     * over ~27 years of days); anything wider is the many-children shape where lazy flat listing is the right tool.
     */
    static final int MAX_LISTED_CHILDREN = 10_000;

    /**
     * Absolute ceiling on surviving directories that {@link #finishSurvivors} will enumerate individually.
     * Overridden by {@link #MAX_FINISH_SURVIVOR_FRACTION} when the level was wide and the kept fraction is small:
     * on a 100-entry level with 5 surviving directories (5 %) individual listings are cheaper than one flat listing
     * of the whole prefix regardless of the absolute count.
     */
    static final int MAX_FINISH_SURVIVORS = 4;

    /**
     * Maximum fraction of a hinted level's children that {@link #finishSurvivors} treats as "few enough to finish
     * individually". When survivors exceed {@link #MAX_FINISH_SURVIVORS} but the kept ratio is at or below this
     * threshold, the pruning was significant enough that individual recursive listings are cheaper than re-listing
     * the whole prefix (which includes the pruned entries). 0.2 (20 %) handles the common {@code IN (handful)}
     * over a wide partition level — e.g. 5 of 100 customer-id folders — without inflating cost on shallow trees
     * where a flat listing is cheaper (e.g. 6 of 10 years, ratio 60 %, would still fall back).
     */
    static final double MAX_FINISH_SURVIVOR_FRACTION = 0.2;

    private PartitionPruningWalk() {}

    /**
     * A walked listing: the matched files (unsorted), the columns folders were pruned on (which the caller must
     * validate against the detected partition columns), the exclusion tally for the user-facing warning, and the
     * type inferred for each partition key from all values seen during the walk (including a one-level retroactive
     * peek into pruned dirs — see {@link #walk}). The caller compares these types against the types detected in the
     * walked file set to catch cases where a pruned subtree was the sole source of a type-widening folder value.
     */
    record WalkResult(
        List<StorageEntry> matched,
        Set<String> prunedColumns,
        int excludedCount,
        String excludedExample,
        String excludedExampleEntry,
        Map<String, DataType> columnFullTypes
    ) {}

    /**
     * Walks the tree under {@code prefix}, or returns {@code null} when the caller should fall back to the flat
     * listing — including on a mid-walk listing failure, which the flat listing will surface on its own if genuine.
     */
    @Nullable
    static WalkResult tryWalk(
        StorageProvider provider,
        StoragePath prefix,
        GlobMatcher matcher,
        ExclusionConfig.NameFilter nameFilter,
        List<PartitionFilterHint> hints,
        int maxDiscoveredFiles
    ) {
        try {
            return walk(provider, prefix, matcher, nameFilter, hints, maxDiscoveredFiles);
        } catch (IOException | ExternalUnavailableException e) {
            logger.debug(() -> "Partition-pruning walk of [" + prefix + "] failed; falling back to a flat listing", e);
            return null;
        }
    }

    @Nullable
    private static WalkResult walk(
        StorageProvider provider,
        StoragePath prefix,
        GlobMatcher matcher,
        ExclusionConfig.NameFilter nameFilter,
        List<PartitionFilterHint> hints,
        int maxDiscoveredFiles
    ) throws IOException {
        Collector collector = new Collector(prefix.toString(), matcher, nameFilter, maxDiscoveredFiles);
        Set<String> pending = new HashSet<>();
        for (PartitionFilterHint hint : hints) {
            pending.add(hint.columnName());
        }
        // A key prunes only at its outermost occurrence: HivePartitionDetector binds the FIRST key=value segment of
        // a path. The prefix's own segments count — under data/year=2024/ every file's year IS 2024, so a deeper
        // year=X folder is just a name and pruning it against a year hint would drop matching rows.
        Set<String> seenKeys = new HashSet<>();
        for (String segment : prefix.path().split("/")) {
            String prefixKey = PartitionValueMatcher.folderKey(segment);
            if (prefixKey != null) {
                seenKeys.add(prefixKey);
                pending.remove(prefixKey);
            }
        }
        Set<String> prunedColumns = new HashSet<>();
        boolean anyLevelHinted = false;
        List<StoragePath> dirs = List.of(prefix);
        int listings = 0;
        // Total shaped dirs at the last hinted level (before pruning). Tracked so finishSurvivors can
        // decide whether the survived fraction is small enough to justify individual recursive listings
        // even when the absolute count exceeds MAX_FINISH_SURVIVORS.
        int lastHintedLevelPeerCount = 0;
        // All raw values seen for each partition key, used to infer the "full" column type. Populated from normal
        // walk listings AND from retroactive peeks into pruned dirs (see below).
        Map<String, List<String>> seenValues = new LinkedHashMap<>();
        // Pruned dirs from the previous level, candidates for a one-level retroactive peek. A peek fires only when
        // the current level introduces a new partition key (confirming a multi-level layout): peeking into a pruned
        // dir whose direct children are files (single-level layout) would pollute the test's enumeration tracking
        // without adding any partition-key information.
        List<StoragePath> pendingPeeks = List.of();

        while (dirs.isEmpty() == false) {
            if (pending.isEmpty() || listings + dirs.size() > MAX_DIRECTORY_LISTINGS) {
                // No hint can narrow a deeper level (or the budget is spent): finish each surviving subtree with
                // one recursive listing, unless one flat listing of the whole prefix is cheaper.
                return finishSurvivors(
                    collector,
                    provider,
                    dirs,
                    prunedColumns,
                    inferColumnTypes(seenValues),
                    lastHintedLevelPeerCount
                );
            }

            List<StoragePath> shapedDirs = new ArrayList<>();
            List<String> shapedKeys = new ArrayList<>();
            List<String> shapedValues = new ArrayList<>();
            List<StoragePath> next = new ArrayList<>();
            // Direct files found in dirs at this level. Deferred until we know which path we take: if this
            // level is unhinted and finishSurvivors lists each parent dir recursively, addRecursively would
            // re-enumerate the same files, causing a double-count. We commit them only on the normal path.
            List<StorageEntry> levelFiles = new ArrayList<>();
            for (StoragePath dir : dirs) {
                listings++;
                StorageChildren children = provider.listChildren(dir, MAX_LISTED_CHILDREN);
                if (children == null) {
                    return null; // the provider cannot enumerate directories, or this one is too wide to buffer
                }
                for (StorageEntry file : children.files()) {
                    // A glob-matching file at a folder level breaks Hive detection; the caller's validation then
                    // rejects the walk.
                    levelFiles.add(file);
                }
                for (StoragePath sub : children.directories()) {
                    String key = PartitionValueMatcher.folderKey(sub.objectName());
                    if (key == null) {
                        // Not partition-shaped (a junk dir, a nested root): descend it untouched; its files either
                        // fall to the exclusion rules or break detection and void the walk.
                        next.add(sub);
                    } else {
                        shapedDirs.add(sub);
                        shapedKeys.add(key);
                        shapedValues.add(PartitionValueMatcher.folderValue(sub.objectName()));
                    }
                }
            }

            boolean[] keep = new boolean[shapedDirs.size()];
            for (int i = 0; i < keep.length; i++) {
                keep[i] = true;
            }
            boolean hintedLevel = false;
            boolean newKeyLevel = false;
            Map<String, List<Integer>> byKey = new LinkedHashMap<>();
            for (int i = 0; i < shapedKeys.size(); i++) {
                byKey.computeIfAbsent(shapedKeys.get(i), k -> new ArrayList<>()).add(i);
            }
            for (Map.Entry<String, List<Integer>> group : byKey.entrySet()) {
                String key = group.getKey();
                List<String> values = new ArrayList<>(group.getValue().size());
                for (int i : group.getValue()) {
                    values.add(shapedValues.get(i));
                }
                // Track ALL values for this key across all current dirs (before the seenKeys guard).
                seenValues.computeIfAbsent(key, k -> new ArrayList<>()).addAll(values);
                if (seenKeys.add(key) == false) {
                    continue;
                }
                newKeyLevel = true;
                pending.remove(key);
                List<PartitionFilterHint> keyHints = PartitionValueMatcher.hintsFor(key, hints);
                if (keyHints.isEmpty()) {
                    continue;
                }
                hintedLevel = true;
                boolean[] matches = PartitionValueMatcher.matchesFolders(values, keyHints);
                for (int j = 0; j < matches.length; j++) {
                    if (matches[j] == false) {
                        keep[group.getValue().get(j)] = false;
                        prunedColumns.add(key);
                    }
                }
            }

            // Retroactive peek: a new partition key at this level confirms a multi-level layout, so it is now
            // safe to list the direct children of dirs pruned at the previous level. Any key=value dirs found
            // contribute shadow values to seenValues, letting the caller detect whether a pruned subtree was the
            // sole source of a type-widening value for a non-pruned column.
            if (newKeyLevel && pendingPeeks.isEmpty() == false) {
                int updated = processPendingPeeks(provider, pendingPeeks, seenValues, listings);
                if (updated < 0) {
                    return null; // budget or buffer limit hit during peek
                }
                listings = updated;
                pendingPeeks = List.of();
            }

            // Partition surviving dirs and collect pruned dirs as candidates for the next level's retroactive peek.
            List<StoragePath> newPeeks = null;
            for (int i = 0; i < keep.length; i++) {
                if (keep[i]) {
                    next.add(shapedDirs.get(i));
                } else {
                    if (newPeeks == null) {
                        newPeeks = new ArrayList<>();
                    }
                    newPeeks.add(shapedDirs.get(i));
                }
            }
            pendingPeeks = newPeeks != null ? newPeeks : List.of();

            if (hintedLevel == false) {
                if (anyLevelHinted == false) {
                    // No level has matched a hint yet — typically a data-column filter, where walking on would
                    // spend a LIST per folder for nothing. Withdrawing after one probe also forfeits pruning for
                    // partition folders nested below a non-partition root, which is what they got before the walk.
                    return null;
                }
                // Some level already pruned, but this one matches no pending hint. Whether a pending hint is a
                // deeper partition key or a data column is unknowable without listing every level in between — a
                // LIST per directory — and `WHERE <partition> AND <data column>` is the everyday shape. Keep the
                // pruning already done and finish by recursively listing each surviving PARENT dir (dirs), not each
                // child (next): listing the parent once enumerates the same files with one round trip instead of N.
                return finishSurvivors(
                    collector,
                    provider,
                    dirs,
                    prunedColumns,
                    inferColumnTypes(seenValues),
                    lastHintedLevelPeerCount
                );
            }
            // Commit direct files now that we know finishSurvivors won't re-enumerate them.
            for (StorageEntry file : levelFiles) {
                collector.add(file);
            }
            anyLevelHinted = true;
            lastHintedLevelPeerCount = shapedDirs.size();
            dirs = next;
        }
        return collector.result(prunedColumns, inferColumnTypes(seenValues));
    }

    /**
     * Lists the direct children of each pruned dir in {@code peeks}, adding any {@code key=value} directory
     * entries to {@code seenValues} as shadow values for type-divergence detection. Returns the updated listings
     * count, or {@code -1} when a peek could not complete (budget exhausted or directory too wide to buffer) —
     * the caller should return {@code null} (fall back to flat listing) in that case.
     */
    private static int processPendingPeeks(
        StorageProvider provider,
        List<StoragePath> peeks,
        Map<String, List<String>> seenValues,
        int listings
    ) throws IOException {
        for (StoragePath pruned : peeks) {
            if (listings >= MAX_DIRECTORY_LISTINGS) {
                return -1;
            }
            listings++;
            StorageChildren peekChildren = provider.listChildren(pruned, MAX_LISTED_CHILDREN);
            if (peekChildren == null) {
                return -1;
            }
            for (StoragePath sub : peekChildren.directories()) {
                String key = PartitionValueMatcher.folderKey(sub.objectName());
                if (key != null) {
                    seenValues.computeIfAbsent(key, k -> new ArrayList<>()).add(PartitionValueMatcher.folderValue(sub.objectName()));
                }
            }
        }
        return listings;
    }

    /** Infers the {@link DataType} for each partition key from all values collected during the walk. */
    private static Map<String, DataType> inferColumnTypes(Map<String, List<String>> seenValues) {
        if (seenValues.isEmpty()) {
            return Map.of();
        }
        Map<String, DataType> types = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : seenValues.entrySet()) {
            types.put(e.getKey(), HivePartitionDetector.inferType(e.getValue()));
        }
        return types;
    }

    /**
     * Ends a walk whose remaining subtrees no hint can narrow: one recursive listing per survivor, but only when
     * something was pruned and individual listings are cheaper than one flat listing of the whole prefix —
     * otherwise {@code null} to signal a flat fallback.
     *
     * <p>Individual listings are cheaper when either: (a) the absolute survivor count is small
     * ({@link #MAX_FINISH_SURVIVORS}), or (b) the kept fraction of the last hinted level is small enough
     * ({@link #MAX_FINISH_SURVIVOR_FRACTION}) that the pruned entries would inflate the flat listing significantly.
     * The fraction guard handles wide levels: {@code IN (5 of 100)} customer-id folders costs 5 individual listings
     * vs a flat listing covering all 100, while {@code year >= 2020} keeping 6 of 10 years falls back to flat
     * because both the absolute count and the fraction are above their respective thresholds.
     */
    @Nullable
    private static WalkResult finishSurvivors(
        Collector collector,
        StorageProvider provider,
        List<StoragePath> dirs,
        Set<String> prunedColumns,
        Map<String, DataType> columnFullTypes,
        int lastHintedLevelPeerCount
    ) throws IOException {
        if (prunedColumns.isEmpty()) {
            return null;
        }
        boolean tooManyAbsolute = dirs.size() > MAX_FINISH_SURVIVORS;
        boolean tooLargeFraction = lastHintedLevelPeerCount > 0
            && (double) dirs.size() / lastHintedLevelPeerCount > MAX_FINISH_SURVIVOR_FRACTION;
        if (tooManyAbsolute && tooLargeFraction) {
            return null;
        }
        for (StoragePath dir : dirs) {
            collector.addRecursively(provider, dir);
        }
        return collector.result(prunedColumns, columnFullTypes);
    }

    /**
     * Accumulates the files the walk keeps, applying exactly the flat listing's per-object rules: relative path,
     * placeholder skipping, glob matcher, exclusion filter (with the warning's count-and-example tally), and the
     * discovery cap.
     */
    private static final class Collector {
        private final String prefixStr;
        private final GlobMatcher matcher;
        private final ExclusionConfig.NameFilter nameFilter;
        private final int maxDiscoveredFiles;
        private final List<StorageEntry> matched = new ArrayList<>();
        private int excludedCount = 0;
        private String excludedExample = null;
        private String excludedExampleEntry = null;

        Collector(String prefixStr, GlobMatcher matcher, ExclusionConfig.NameFilter nameFilter, int maxDiscoveredFiles) {
            this.prefixStr = prefixStr;
            this.matcher = matcher;
            this.nameFilter = nameFilter;
            this.maxDiscoveredFiles = maxDiscoveredFiles;
        }

        void add(StorageEntry entry) {
            String entryPath = entry.path().toString();
            String relativePath = entryPath.startsWith(prefixStr) ? entryPath.substring(prefixStr.length()) : entry.path().objectName();
            if (relativePath.isEmpty() || relativePath.endsWith("/")) {
                return; // directory placeholder key, skipped as in the flat listing
            }
            if (matcher.matches(relativePath) == false) {
                return;
            }
            String excludedBy = nameFilter.excludedBy(relativePath);
            if (excludedBy != null) {
                excludedCount++;
                if (excludedExample == null) {
                    excludedExample = relativePath;
                    excludedExampleEntry = excludedBy;
                }
                return;
            }
            matched.add(entry);
            GlobExpander.checkDiscoveredFilesLimit(matched.size(), maxDiscoveredFiles);
        }

        void addRecursively(StorageProvider provider, StoragePath dir) throws IOException {
            try (StorageIterator iterator = provider.listObjects(dir, true)) {
                while (iterator.hasNext()) {
                    add(iterator.next());
                }
            }
        }

        WalkResult result(Set<String> prunedColumns, Map<String, DataType> columnFullTypes) {
            return new WalkResult(matched, prunedColumns, excludedCount, excludedExample, excludedExampleEntry, columnFullTypes);
        }
    }
}
