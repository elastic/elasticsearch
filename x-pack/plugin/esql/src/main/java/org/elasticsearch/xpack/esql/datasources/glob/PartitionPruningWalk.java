/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.CheckedSupplier;
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
     * {@code WHERE <partition> AND <data column>} is the everyday shape, so the walk never descends speculatively
     * on a {@code **} glob. A keyed glob ({@code key=*} / {@code key=literal} segments) may descend an
     * unhinted leading identity key when a hinted key of that glob is still pending.
 * Survivors are finished with one recursive listing each, but only when something was pruned and
 * {@link #cheaperToFinish} says the remaining listings cost no more LIST pages than one flat listing of the
 * prefix (the first survivor is already listed); otherwise the walk returns {@code null} and the caller
 * flat-lists.
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
     * Ceiling on pruned directories retained as probe sources for a one-file inference anchor. Outermost pruned
     * folders first; the walk records them as it goes and does not list them again.
     */
    static final int MAX_PRUNED_DIRS_FOR_ANCHOR = 16;

    /**
     * Ceiling on one directory's materialized children — {@link StorageProvider#listChildren} buffers a whole
     * directory, unlike the flat listing's lazy iterator. Providers return {@code null} past the limit. 10k bounds
     * the buffer to roughly a megabyte while still covering the widest realistic partition levels (e.g. a date key
     * over ~27 years of days); anything wider is the many-children shape where lazy flat listing is the right tool.
     */
    static final int MAX_LISTED_CHILDREN = 10_000;

    /**
     * LIST page size used by {@link #cheaperToFinish}. S3 ListObjectsV2's default; {@link StorageProvider} does
     * not expose a page size.
     */
    static final int LIST_PAGE_SIZE = 1000;

    /**
     * Test-only override of {@link #finishSurvivors}' cost check. Production always uses {@link FinishMode#RULE}.
     */
    enum FinishMode {
        RULE,
        ALWAYS_FINISH,
        ALWAYS_FLAT
    }

    private static final ThreadLocal<FinishMode> FINISH_MODE = ThreadLocal.withInitial(() -> FinishMode.RULE);

    private PartitionPruningWalk() {}

    /**
     * Runs {@code body} with a test-only {@link #finishSurvivors} policy. Restores the previous mode even if
     * {@code body} throws.
     */
    static <T> T withFinishMode(FinishMode mode, CheckedSupplier<T, IOException> body) throws IOException {
        FinishMode previous = FINISH_MODE.get();
        FINISH_MODE.set(mode);
        try {
            return body.get();
        } finally {
            if (previous == FinishMode.RULE) {
                FINISH_MODE.remove();
            } else {
                FINISH_MODE.set(previous);
            }
        }
    }

    /** Test-only. Clears a leaked {@link FinishMode} override on the current thread. */
    static void clearFinishMode() {
        FINISH_MODE.remove();
    }

    /**
     * True when finishing the remaining {@code survivors - 1} directories costs no more LIST pages than
     * flattening {@code estimatedPrefixWidth} directories of the same size, given the first survivor is
     * already listed: {@code (S - 1) * ceil(k / P) <= ceil(L * k / P)}. Flatten still pays the prefix
     * listing on top of that probe; finish reuses it. {@code estimatedPrefixWidth} is the product, over
     * walked levels, of average partition-shaped children per listed directory.
     */
    static boolean cheaperToFinish(int survivors, long objectsUnderFirst, double estimatedPrefixWidth) {
        return cheaperToFinish(survivors, objectsUnderFirst, estimatedPrefixWidth, LIST_PAGE_SIZE);
    }

    static boolean cheaperToFinish(int survivors, long objectsUnderFirst, double estimatedPrefixWidth, int pageSize) {
        if (survivors <= 0) {
            return true;
        }
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be positive: " + pageSize);
        }
        // An empty LIST still costs one page. Billing k=0 as 1 keeps an empty first survivor from
        // making every remaining listing look free.
        long k = Math.max(1L, objectsUnderFirst);
        long pagesPerDir = ceilDiv(k, pageSize);
        double remainingFinishPages = (double) (survivors - 1) * pagesPerDir;
        double flatPages = Math.ceil(estimatedPrefixWidth * k / pageSize);
        return remainingFinishPages <= flatPages;
    }

    /** Ceiling of {@code n / d} for non-negative {@code n}; 0 when {@code n} is 0. */
    static long ceilDiv(long n, int d) {
        if (n <= 0) {
            return 0;
        }
        return (n + d - 1L) / d;
    }

    /**
     * A walked listing: the matched files (unsorted), the columns folders were pruned on (which the caller must
     * validate against the detected partition columns), the exclusion tally for the user-facing warning, and the
     * type inferred for each partition key from all values seen during the walk (including a one-level retroactive
     * peek into pruned dirs — see {@link #walk}). The caller compares these types against the types detected in the
     * walked file set to catch cases where a pruned subtree was the sole source of a type-widening folder value.
     * {@link #prunedDirs} is the outermost-first probe list (capped at {@link #MAX_PRUNED_DIRS_FOR_ANCHOR}) used
     * when the walk matched nothing: an empty glob under surviving dirs still needs a directory that holds a file.
     */
    record WalkResult(
        List<StorageEntry> matched,
        Set<String> prunedColumns,
        int excludedCount,
        String excludedExample,
        String excludedExampleEntry,
        Map<String, DataType> columnFullTypes,
        List<StoragePath> prunedDirs
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
        int maxDiscoveredFiles,
        String glob
    ) {
        return tryWalk(provider, prefix, matcher, nameFilter, hints, maxDiscoveredFiles, glob, null);
    }

    static WalkResult tryWalk(
        StorageProvider provider,
        StoragePath prefix,
        GlobMatcher matcher,
        ExclusionConfig.NameFilter nameFilter,
        List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        String glob,
        @Nullable FolderRangePredicate folderRange
    ) {
        try {
            return walk(provider, prefix, matcher, nameFilter, hints, maxDiscoveredFiles, keyedGlobKeys(glob), folderRange);
        } catch (IOException | ExternalUnavailableException e) {
            logger.debug(() -> "Partition-pruning walk of [" + prefix + "] failed; falling back to a flat listing", e);
            return null;
        }
    }

    /**
     * Hive {@code key=} segments of {@code globPart} (star or pinned value). A {@code **} glob has none, so
     * the walk still withdraws at an unhinted first level.
     */
    static Set<String> keyedGlobKeys(String glob) {
        if (glob == null || glob.isEmpty()) {
            return Set.of();
        }
        Set<String> keys = new HashSet<>();
        for (String segment : glob.split("/")) {
            String key = PartitionValueMatcher.folderKey(segment);
            if (key != null) {
                keys.add(key);
            }
        }
        return keys;
    }

    @Nullable
    private static WalkResult walk(
        StorageProvider provider,
        StoragePath prefix,
        GlobMatcher matcher,
        ExclusionConfig.NameFilter nameFilter,
        List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        Set<String> keyedGlobKeys,
        @Nullable FolderRangePredicate folderRange
    ) throws IOException {
        Collector collector = new Collector(prefix.toString(), matcher, nameFilter, maxDiscoveredFiles);
        Set<String> pending = new HashSet<>();
        for (PartitionFilterHint hint : hints) {
            pending.add(hint.columnName());
        }
        if (folderRange != null) {
            pending.addAll(folderRange.bindKeys());
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
        List<StoragePath> prunedDirs = new ArrayList<>();
        boolean anyLevelHinted = false;
        List<StoragePath> dirs = List.of(prefix);
        int listings = 0;
        // Product over walked levels of (shaped children / listed directories). Estimates how many dirs
        // of the current grain sit under the prefix, so finishSurvivors can compare remaining finish pages
        // (S-1)*ceil(k/P) against a flat listing of L * k objects.
        double estimatedPrefixWidth = 1.0;
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
                    estimatedPrefixWidth,
                    prunedDirs
                );
            }

            int listedDirs = dirs.size();
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
            if (folderRange != null && shapedDirs.isEmpty() == false) {
                // The predicate ran at this level even if nothing dropped: year=2026 kept by [2026-10-01, ∞)
                // must still count as hinted so the walk descends to month/day.
                hintedLevel = true;
                for (int i = 0; i < keep.length; i++) {
                    if (keep[i] == false) {
                        continue;
                    }
                    Map<String, String> bindings = PartitionValueMatcher.pathBindings(shapedDirs.get(i).path());
                    if (folderRange.mayContain(bindings) == false) {
                        keep[i] = false;
                        prunedColumns.add(shapedKeys.get(i));
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
                    if (prunedDirs.size() < MAX_PRUNED_DIRS_FOR_ANCHOR) {
                        prunedDirs.add(shapedDirs.get(i));
                    }
                }
            }
            pendingPeeks = newPeeks != null ? newPeeks : List.of();

            if (hintedLevel == false) {
                if (descendUnhintedKeyedLevel(byKey.keySet(), pending, keyedGlobKeys)) {
                    // Leading identity keys of a keyed glob (aws-account-id=*, aws-region=*) with year/month/day
                    // still pending: keep every child and walk on. Do not prune this level. A ** glob has no
                    // keyed segments and never takes this path.
                    for (StorageEntry file : levelFiles) {
                        collector.add(file);
                    }
                    estimatedPrefixWidth = bumpPrefixWidth(estimatedPrefixWidth, listedDirs, shapedDirs.size());
                    dirs = next;
                    continue;
                }
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
                // Do not bump L: survivors are the parents, whose width is already in estimatedPrefixWidth.
                return finishSurvivors(
                    collector,
                    provider,
                    dirs,
                    prunedColumns,
                    inferColumnTypes(seenValues),
                    estimatedPrefixWidth,
                    prunedDirs
                );
            }
            // Commit direct files now that we know finishSurvivors won't re-enumerate them.
            for (StorageEntry file : levelFiles) {
                collector.add(file);
            }
            anyLevelHinted = true;
            estimatedPrefixWidth = bumpPrefixWidth(estimatedPrefixWidth, listedDirs, shapedDirs.size());
            dirs = next;
        }
        return collector.result(prunedColumns, inferColumnTypes(seenValues), prunedDirs);
    }

    /**
     * A keyed glob may walk through an unhinted hive level (keep every child) when a hinted key of that
     * glob is still pending. Never prune the unhinted level. Empty keyed set ({@code **}) stays fail-closed.
     */
    private static boolean descendUnhintedKeyedLevel(Set<String> levelKeys, Set<String> pending, Set<String> keyedGlobKeys) {
        if (keyedGlobKeys.isEmpty() || levelKeys.isEmpty()) {
            return false;
        }
        boolean pendingKeyedHint = false;
        for (String column : pending) {
            if (keyedGlobKeys.contains(column)) {
                pendingKeyedHint = true;
                break;
            }
        }
        if (pendingKeyedHint == false) {
            return false;
        }
        for (String key : levelKeys) {
            if (keyedGlobKeys.contains(key)) {
                return true;
            }
        }
        return false;
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

    /** Product update: skip empty levels so a file-only listing does not zero the prefix-width estimate. */
    private static double bumpPrefixWidth(double estimatedPrefixWidth, int listedDirs, int shapedCount) {
        if (listedDirs <= 0 || shapedCount <= 0) {
            return estimatedPrefixWidth;
        }
        return estimatedPrefixWidth * ((double) shapedCount / listedDirs);
    }

    /**
     * Ends a walk whose remaining subtrees no hint can narrow: one recursive listing per survivor, but only when
     * something was pruned and {@link #cheaperToFinish} says the remaining listings cost no more LIST pages than
     * one flat listing of the prefix — otherwise {@code null} to signal a flat fallback.
     *
     * <p>The first partition-shaped survivor is listed before the decision so {@code k} is measured rather than
     * guessed. Junk dirs ({@code _temporary}) sort first in {@code dirs} and would understate {@code k}. If the
     * rule then flattens, that one LIST is wasted; the collector is discarded with the {@code null} return.
     */
    @Nullable
    private static WalkResult finishSurvivors(
        Collector collector,
        StorageProvider provider,
        List<StoragePath> dirs,
        Set<String> prunedColumns,
        Map<String, DataType> columnFullTypes,
        double estimatedPrefixWidth,
        List<StoragePath> prunedDirs
    ) throws IOException {
        if (prunedColumns.isEmpty()) {
            return null;
        }
        FinishMode mode = FINISH_MODE.get();
        if (mode == FinishMode.ALWAYS_FLAT) {
            return null;
        }
        if (dirs.isEmpty()) {
            return collector.result(prunedColumns, columnFullTypes, prunedDirs);
        }
        int measure = indexOfFirstShapedDir(dirs);
        List<StorageEntry> firstBatch = listDir(provider, dirs.get(measure));
        if (mode == FinishMode.RULE && cheaperToFinish(dirs.size(), firstBatch.size(), estimatedPrefixWidth) == false) {
            return null;
        }
        for (StorageEntry entry : firstBatch) {
            collector.add(entry);
        }
        for (int i = 0; i < dirs.size(); i++) {
            if (i != measure) {
                collector.addRecursively(provider, dirs.get(i));
            }
        }
        return collector.result(prunedColumns, columnFullTypes, prunedDirs);
    }

    /**
     * Index of the first {@code key=value} folder in {@code dirs}, else {@code 0}. Junk dirs are prepended
     * during the walk, so {@code dirs.get(0)} is often {@code _temporary} and must not set {@code k}.
     */
    private static int indexOfFirstShapedDir(List<StoragePath> dirs) {
        for (int i = 0; i < dirs.size(); i++) {
            if (PartitionValueMatcher.folderKey(dirs.get(i).objectName()) != null) {
                return i;
            }
        }
        return 0;
    }

    private static List<StorageEntry> listDir(StorageProvider provider, StoragePath dir) throws IOException {
        List<StorageEntry> entries = new ArrayList<>();
        try (StorageIterator iterator = provider.listObjects(dir, true)) {
            while (iterator.hasNext()) {
                entries.add(iterator.next());
            }
        }
        return entries;
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

        WalkResult result(Set<String> prunedColumns, Map<String, DataType> columnFullTypes, List<StoragePath> prunedDirs) {
            return new WalkResult(
                matched,
                prunedColumns,
                excludedCount,
                excludedExample,
                excludedExampleEntry,
                columnFullTypes,
                List.copyOf(prunedDirs)
            );
        }
    }
}
