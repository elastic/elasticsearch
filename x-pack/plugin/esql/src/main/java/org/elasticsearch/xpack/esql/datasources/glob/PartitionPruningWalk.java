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
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.PartitionValueMatcher;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
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
 * hint (typically a data-column filter). Once a hint has matched it may cross unhinted levels — {@code year} and
 * {@code hour} hints prune both ends of a {@code year=/month=/day=/hour=} tree — bounded by
 * {@link #MAX_DIRECTORY_LISTINGS}. Survivors are finished with one recursive listing each, but only when something
 * was pruned and they fit the remaining budget; otherwise one flat listing is cheaper.
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
     * Ceiling on listings in one walk (per-directory and survivor-finishing alike), so a wide or deep tree cannot
     * turn the walk into more LIST requests than the flat listing it replaces. Sized against that comparison: a
     * flat listing pages ~1000 keys per request (~100 requests for 100k objects), and walking a realistic Hive tree
     * with a leading-key filter takes tens to low hundreds of listings — so 512 fits the legitimate deep-tree case
     * while capping a walk that speculates wrongly at the same order of cost as the flat listing it falls back to.
     */
    static final int MAX_DIRECTORY_LISTINGS = 512;

    /**
     * Ceiling on one directory's materialized children — {@link StorageProvider#listChildren} buffers a whole
     * directory, unlike the flat listing's lazy iterator. Providers return {@code null} past the limit. 10k bounds
     * the buffer to roughly a megabyte while still covering the widest realistic partition levels (e.g. a date key
     * over ~27 years of days); anything wider is the many-children shape where lazy flat listing is the right tool.
     */
    static final int MAX_LISTED_CHILDREN = 10_000;

    private PartitionPruningWalk() {}

    /**
     * A walked listing: the matched files (unsorted), the columns folders were pruned on (which the caller must
     * validate against the detected partition columns), and the exclusion tally for the user-facing warning.
     */
    record WalkResult(
        List<StorageEntry> matched,
        Set<String> prunedColumns,
        int excludedCount,
        String excludedExample,
        String excludedExampleEntry
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
        } catch (IOException e) {
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

        while (dirs.isEmpty() == false) {
            if (pending.isEmpty() || listings + dirs.size() > MAX_DIRECTORY_LISTINGS) {
                // No hint can narrow a deeper level (or the budget is spent): finish each surviving subtree with
                // one recursive listing, unless one flat listing of the whole prefix is cheaper.
                return finishSurvivors(collector, provider, dirs, prunedColumns, listings);
            }

            List<StoragePath> shapedDirs = new ArrayList<>();
            List<String> shapedKeys = new ArrayList<>();
            List<String> shapedValues = new ArrayList<>();
            List<StoragePath> next = new ArrayList<>();
            for (StoragePath dir : dirs) {
                listings++;
                StorageChildren children = provider.listChildren(dir, MAX_LISTED_CHILDREN);
                if (children == null) {
                    return null; // the provider cannot enumerate directories, or this one is too wide to buffer
                }
                for (StorageEntry file : children.files()) {
                    // A glob-matching file at a folder level breaks Hive detection; the caller's validation then
                    // rejects the walk.
                    collector.add(file);
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
            Map<String, List<Integer>> byKey = new LinkedHashMap<>();
            for (int i = 0; i < shapedKeys.size(); i++) {
                byKey.computeIfAbsent(shapedKeys.get(i), k -> new ArrayList<>()).add(i);
            }
            for (Map.Entry<String, List<Integer>> group : byKey.entrySet()) {
                String key = group.getKey();
                if (seenKeys.add(key) == false) {
                    continue;
                }
                pending.remove(key);
                List<PartitionFilterHint> keyHints = PartitionValueMatcher.hintsFor(key, hints);
                if (keyHints.isEmpty()) {
                    continue;
                }
                hintedLevel = true;
                List<String> values = new ArrayList<>(group.getValue().size());
                for (int i : group.getValue()) {
                    values.add(shapedValues.get(i));
                }
                boolean[] matches = PartitionValueMatcher.matchesFolders(values, keyHints);
                for (int j = 0; j < matches.length; j++) {
                    if (matches[j] == false) {
                        keep[group.getValue().get(j)] = false;
                        prunedColumns.add(key);
                    }
                }
            }

            if (hintedLevel == false && anyLevelHinted == false) {
                // No level has matched a hint yet — typically a data-column filter, where walking on would spend a
                // LIST per folder for nothing. Withdrawing after one probe also forfeits pruning for partition
                // folders nested below a non-partition root, which is what they got before the walk existed.
                return null;
            }
            anyLevelHinted |= hintedLevel;

            for (int i = 0; i < keep.length; i++) {
                if (keep[i]) {
                    next.add(shapedDirs.get(i));
                }
            }
            dirs = next;
        }
        return collector.result(prunedColumns);
    }

    /**
     * Ends a walk whose remaining subtrees no hint can narrow: one recursive listing per survivor, but only when
     * something was pruned and the survivors fit the remaining budget — otherwise {@code null}, since one flat
     * listing enumerates the same files.
     */
    @Nullable
    private static WalkResult finishSurvivors(
        Collector collector,
        StorageProvider provider,
        List<StoragePath> dirs,
        Set<String> prunedColumns,
        int listings
    ) throws IOException {
        if (prunedColumns.isEmpty() || dirs.size() > MAX_DIRECTORY_LISTINGS - listings) {
            return null;
        }
        for (StoragePath dir : dirs) {
            collector.addRecursively(provider, dir);
        }
        return collector.result(prunedColumns);
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

        WalkResult result(Set<String> prunedColumns) {
            return new WalkResult(matched, prunedColumns, excludedCount, excludedExample, excludedExampleEntry);
        }
    }
}
