/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.HivePartitionDetector;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.PartitionValueMatcher;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.TemplatePartitionDetector;
import org.elasticsearch.xpack.esql.datasources.TemplatePartitionDetector.WalkLayout;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Hint-narrowed enumeration of a partition tree: the tree is walked level by level via
 * {@link StorageProvider#listChildren}, and a folder a partition hint excludes is never listed. Hive layouts
 * match {@code key=value} names; TEMPLATE layouts with a fixed-depth glob match by position in
 * {@code partition_path}. On a bare {@code **} glob the textual rewrite has nothing to act on, so this is
 * the only listing-time pruning for Hive. TEMPLATE never walks {@code **}. Folders are matched by typed
 * value ({@link PartitionValueMatcher}, shared with the read layer), so e.g. {@code month == 6} keeps a
 * zero-padded {@code 06} (or {@code month=06}) and a NULL partition is never pruned.
 *
 * <p><b>Fail-closed.</b> Returning {@code null} means "use the flat listing", a superset, used when the walk cannot
 * prove a narrowing: the provider cannot enumerate directories (or a directory exceeds {@link #MAX_LISTED_CHILDREN}),
 * after one probe listing when no level matched a hint (typically a data-column filter), and at the first level no
 * pending hint matches — whether a pending hint is a deeper partition key or a data column is unknowable without
 * listing every level in between, and {@code WHERE <partition> AND <data column>} is the everyday shape, so the walk
 * never descends speculatively on a {@code **} glob. A keyed glob ({@code key=*} / {@code key=literal} segments) may
 * descend an unhinted leading identity key when a hinted key of that glob is still pending. A TEMPLATE layout has
 * known depth, so leftover pending after the last glob-directory level finishes survivors instead of listing past
 * the layout and withdrawing. A mid-walk store {@link IOException} is not a withdraw: it fails the listing rather
 * than flattening the prefix. Transient retries sit on the storage provider; this class does not add a second retry.
 * Limit errors ({@link DiscoveredFilesLimitException}) likewise propagate. Survivors are finished with one recursive
 * listing each, but only when something was pruned and {@link #cheaperToFinish} says the remaining listings cost no
 * more LIST pages than one flat listing of the prefix (the first survivor is already listed); otherwise the walk
 * returns {@code null} and the caller flat-lists.
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
     * Default ceiling on issued {@code listChildren} calls in one walk. Finish {@code listObjects} pages
     * are not counted. Production reads {@code esql.external.max_directory_listings}; this is that
     * setting's default and the value tests use when they do not pass a cap. 3000 is observed 2111
     * {@code listChildren} for a 7-day hour-grain walk on the 154-prefix tree, plus headroom for a
     * wider account/region fan-out. Peeks are already in that 2111.
     */
    static final int DEFAULT_MAX_DIRECTORY_LISTINGS = 3_000;

    private static final BooleanSupplier NEVER_CANCELLED = () -> false;

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

    /** Occupies a completed peek slot after folder values are consumed so the payload can be dropped. */
    private static final StorageChildren SLOT_DONE = new StorageChildren(List.of(), List.of());

    /**
     * LIST page size used by {@link #cheaperToFinish}. S3 ListObjectsV2's default; {@link StorageProvider} does
     * not expose a page size.
     */
    static final int LIST_PAGE_SIZE = 1000;

    /**
     * Test-only override of {@link #finishSurvivors}' cost check. Production always uses {@link FinishMode#RULE}.
     * Captured on the {@link WalkSession} at construction so {@link #tryWalkAsync} honors {@link #withFinishMode}
     * from the calling thread.
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
        List<StoragePath> prunedDirs,
        @Nullable StorageEntry fileKeepStash,
        int globKeptCount
    ) {}

    /**
     * Walks the tree under {@code prefix}, or returns {@code null} when the caller should fall back to the
     * flat listing (unprovable narrowing). A store {@link IOException} is thrown rather than converted to a
     * flat listing of the prefix.
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
    ) throws IOException {
        return tryWalk(provider, prefix, matcher, nameFilter, hints, maxDiscoveredFiles, glob, null, null);
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
    ) throws IOException {
        return tryWalk(provider, prefix, matcher, nameFilter, hints, maxDiscoveredFiles, glob, folderRange, null, Integer.MAX_VALUE);
    }

    static WalkResult tryWalk(
        StorageProvider provider,
        StoragePath prefix,
        GlobMatcher matcher,
        ExclusionConfig.NameFilter nameFilter,
        List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        String glob,
        @Nullable FolderRangePredicate folderRange,
        @Nullable Predicate<StorageEntry> fileKeep
    ) throws IOException {
        return tryWalk(provider, prefix, matcher, nameFilter, hints, maxDiscoveredFiles, glob, folderRange, fileKeep, Integer.MAX_VALUE);
    }

    static WalkResult tryWalk(
        StorageProvider provider,
        StoragePath prefix,
        GlobMatcher matcher,
        ExclusionConfig.NameFilter nameFilter,
        List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        String glob,
        @Nullable FolderRangePredicate folderRange,
        @Nullable Predicate<StorageEntry> fileKeep,
        int maxListedObjects
    ) throws IOException {
        return tryWalk(
            provider,
            prefix,
            matcher,
            nameFilter,
            hints,
            maxDiscoveredFiles,
            glob,
            folderRange,
            fileKeep,
            maxListedObjects,
            DEFAULT_MAX_DIRECTORY_LISTINGS,
            NEVER_CANCELLED
        );
    }

    static WalkResult tryWalk(
        StorageProvider provider,
        StoragePath prefix,
        GlobMatcher matcher,
        ExclusionConfig.NameFilter nameFilter,
        List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        String glob,
        @Nullable FolderRangePredicate folderRange,
        @Nullable Predicate<StorageEntry> fileKeep,
        int maxListedObjects,
        int maxDirectoryListings,
        BooleanSupplier cancelled
    ) throws IOException {
        return tryWalk(
            provider,
            prefix,
            matcher,
            nameFilter,
            hints,
            maxDiscoveredFiles,
            glob,
            folderRange,
            fileKeep,
            maxListedObjects,
            maxDirectoryListings,
            cancelled,
            null
        );
    }

    static WalkResult tryWalk(
        StorageProvider provider,
        StoragePath prefix,
        GlobMatcher matcher,
        ExclusionConfig.NameFilter nameFilter,
        List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        String glob,
        @Nullable FolderRangePredicate folderRange,
        @Nullable Predicate<StorageEntry> fileKeep,
        int maxListedObjects,
        int maxDirectoryListings,
        BooleanSupplier cancelled,
        @Nullable String pathTemplate
    ) throws IOException {
        return walk(
            provider,
            prefix,
            matcher,
            nameFilter,
            hints,
            maxDiscoveredFiles,
            glob,
            folderRange,
            fileKeep,
            maxListedObjects,
            maxDirectoryListings,
            cancelled,
            pathTemplate
        );
    }

    /**
     * Async counterpart of {@link #tryWalk}: each directory level and each retroactive peek batch is
     * listed concurrently on {@code executor} via {@link ThrottledIterator}, results merged in
     * directory-slot order. The calling thread is not blocked; {@code listener} fires on an executor
     * thread. A cancelled child fails with {@link TaskCancelledException}. A store {@link IOException}
     * fails the listener rather than completing with {@code null}. {@code null} remains a walk withdraw
     * (unsupported {@code listChildren}, ceiling, stray). Output and LIST count match the sequential
     * walk: peeks consume the remaining listing budget greedily, then fail closed if any peek was skipped.
     */
    static void tryWalkAsync(
        StorageProvider provider,
        StoragePath prefix,
        GlobMatcher matcher,
        ExclusionConfig.NameFilter nameFilter,
        List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        String glob,
        @Nullable FolderRangePredicate folderRange,
        @Nullable Predicate<StorageEntry> fileKeep,
        int maxListedObjects,
        int maxDirectoryListings,
        BooleanSupplier cancelled,
        int concurrency,
        Executor executor,
        ActionListener<WalkResult> listener
    ) {
        tryWalkAsync(
            provider,
            prefix,
            matcher,
            nameFilter,
            hints,
            maxDiscoveredFiles,
            glob,
            folderRange,
            fileKeep,
            maxListedObjects,
            maxDirectoryListings,
            cancelled,
            concurrency,
            executor,
            listener,
            null
        );
    }

    static void tryWalkAsync(
        StorageProvider provider,
        StoragePath prefix,
        GlobMatcher matcher,
        ExclusionConfig.NameFilter nameFilter,
        List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        String glob,
        @Nullable FolderRangePredicate folderRange,
        @Nullable Predicate<StorageEntry> fileKeep,
        int maxListedObjects,
        int maxDirectoryListings,
        BooleanSupplier cancelled,
        int concurrency,
        Executor executor,
        ActionListener<WalkResult> listener,
        @Nullable String pathTemplate
    ) {
        WalkSession session;
        try {
            session = new WalkSession(
                provider,
                prefix,
                matcher,
                nameFilter,
                hints,
                maxDiscoveredFiles,
                glob,
                folderRange,
                fileKeep,
                maxListedObjects,
                maxDirectoryListings,
                cancelled,
                pathTemplate
            );
        } catch (RuntimeException e) {
            listener.onFailure(e);
            return;
        }
        session.runAsync(Math.max(1, concurrency), executor, listener);
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
        String glob,
        @Nullable FolderRangePredicate folderRange,
        @Nullable Predicate<StorageEntry> fileKeep,
        int maxListedObjects,
        int maxDirectoryListings,
        BooleanSupplier cancelled,
        @Nullable String pathTemplate
    ) throws IOException {
        return new WalkSession(
            provider,
            prefix,
            matcher,
            nameFilter,
            hints,
            maxDiscoveredFiles,
            glob,
            folderRange,
            fileKeep,
            maxListedObjects,
            maxDirectoryListings,
            cancelled,
            pathTemplate
        ).runSequential();
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
     * entries to {@code seenValues} as shadow values for type-divergence detection. Each LIST is consumed as
     * it returns so the walk retains folder values, not the full {@link StorageChildren} payload. Returns
     * {@code false} when a peek could not complete (budget exhausted or directory too wide to buffer) — the
     * caller should return {@code null} (fall back to flat listing) in that case. Issues at most the remaining
     * listing budget, matching the sequential walk's greedy ceiling.
     */
    private static boolean processPendingPeeks(
        StorageProvider provider,
        List<StoragePath> peeks,
        Map<String, List<String>> seenValues,
        AtomicInteger listings,
        int maxDirectoryListings,
        BooleanSupplier cancelled,
        @Nullable WalkLayout templateLayout,
        int walkLevel
    ) throws IOException {
        if (cancelled.getAsBoolean()) {
            throw new TaskCancelledException("listing cancelled");
        }
        List<StoragePath> toPeek = peeksToIssue(peeks, listings.get(), maxDirectoryListings);
        if (toPeek == null) {
            return false;
        }
        for (StoragePath dir : toPeek) {
            if (cancelled.getAsBoolean()) {
                throw new TaskCancelledException("listing cancelled");
            }
            listings.incrementAndGet();
            StorageChildren children = provider.listChildren(dir, MAX_LISTED_CHILDREN);
            if (cancelled.getAsBoolean()) {
                throw new TaskCancelledException("listing cancelled");
            }
            if (children == null) {
                return false;
            }
            applyPeekChildren(children, seenValues, templateLayout, walkLevel);
        }
        return toPeek.size() == peeks.size();
    }

    /**
     * First {@code min(peeks, remaining budget)} dirs, or {@code null} when none fit. A truncated
     * slice must still be listed then fail closed so issued counts match the sequential greedy ceiling.
     */
    @Nullable
    private static List<StoragePath> peeksToIssue(List<StoragePath> peeks, int issued, int maxDirectoryListings) {
        int remaining = maxDirectoryListings - issued;
        if (remaining <= 0) {
            return null;
        }
        if (remaining < peeks.size()) {
            return peeks.subList(0, remaining);
        }
        return peeks;
    }

    private static void applyPeekChildren(
        StorageChildren peekChildren,
        Map<String, List<String>> seenValues,
        @Nullable WalkLayout templateLayout,
        int walkLevel
    ) {
        if (templateLayout != null) {
            WalkLayout.Level role = templateLayout.level(walkLevel);
            if (role.isPlaceholder() == false) {
                return;
            }
            String key = role.name();
            for (StoragePath sub : peekChildren.directories()) {
                seenValues.computeIfAbsent(key, k -> new ArrayList<>()).add(role.decodedFolder(sub.objectName()));
            }
            return;
        }
        for (StoragePath sub : peekChildren.directories()) {
            String key = PartitionValueMatcher.folderKey(sub.objectName());
            if (key != null) {
                seenValues.computeIfAbsent(key, k -> new ArrayList<>()).add(PartitionValueMatcher.folderValue(sub.objectName()));
            }
        }
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
        List<StoragePath> prunedDirs,
        FinishMode mode
    ) throws IOException {
        return finishSurvivors(collector, provider, dirs, prunedColumns, columnFullTypes, estimatedPrefixWidth, prunedDirs, mode, false);
    }

    @Nullable
    private static WalkResult finishSurvivors(
        Collector collector,
        StorageProvider provider,
        List<StoragePath> dirs,
        Set<String> prunedColumns,
        Map<String, DataType> columnFullTypes,
        double estimatedPrefixWidth,
        List<StoragePath> prunedDirs,
        FinishMode mode,
        boolean templateFrontier
    ) throws IOException {
        if (prunedColumns.isEmpty()) {
            return null;
        }
        if (mode == FinishMode.ALWAYS_FLAT) {
            return null;
        }
        if (dirs.isEmpty()) {
            return collector.result(prunedColumns, columnFullTypes, prunedDirs);
        }
        int measure = templateFrontier ? 0 : indexOfFirstShapedDir(dirs);
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
     * Lists {@code dirs} in order on the calling thread. Increments {@code issued} once per started call.
     * {@code null} means a child listing was unsupported or too wide — the walk withdraws.
     */
    private static StorageChildren[] listChildrenSequential(
        StorageProvider provider,
        List<StoragePath> dirs,
        AtomicInteger issued,
        BooleanSupplier cancelled
    ) throws IOException {
        return listChildrenSequential(provider, dirs, issued, cancelled, null);
    }

    private static StorageChildren[] listChildrenSequential(
        StorageProvider provider,
        List<StoragePath> dirs,
        AtomicInteger issued,
        BooleanSupplier cancelled,
        @Nullable Consumer<StorageChildren> consumeListed
    ) throws IOException {
        StorageChildren[] listed = new StorageChildren[dirs.size()];
        for (int i = 0; i < dirs.size(); i++) {
            if (cancelled.getAsBoolean()) {
                throw new TaskCancelledException("listing cancelled");
            }
            issued.incrementAndGet();
            StorageChildren children = provider.listChildren(dirs.get(i), MAX_LISTED_CHILDREN);
            if (cancelled.getAsBoolean()) {
                throw new TaskCancelledException("listing cancelled");
            }
            if (children == null) {
                return null;
            }
            if (consumeListed != null) {
                consumeListed.accept(children);
                listed[i] = SLOT_DONE;
            } else {
                listed[i] = children;
            }
        }
        return listed;
    }

    /**
     * Level-parallel {@code listChildren}: {@link ThrottledIterator} on {@code executor}, results merged in
     * directory-slot order. A failed, cancelled, or {@code null} child fails the level (same as sequential:
     * {@code null} withdraws the walk); unstarted slots are skipped and do not increment {@code issued}.
     * A store failure replaces a sibling withdraw so a retryable 503 is not flattened away. Slot payloads
     * are detached once on cancel or fail, not rescanned per skipped index. The listener fires on an executor
     * thread. Does not block the caller.
     */
    static void listChildrenParallel(
        StorageProvider provider,
        List<StoragePath> dirs,
        int concurrency,
        Executor executor,
        AtomicInteger issued,
        BooleanSupplier cancelled,
        ActionListener<StorageChildren[]> listener
    ) {
        listChildrenParallel(provider, dirs, concurrency, executor, issued, cancelled, null, listener);
    }

    /**
     * @param consumeListed if non-null, each successful listing is consumed immediately and the slot keeps
     *                      only {@link #SLOT_DONE} so peek folder values need not retain full children until
     *                      the batch merges
     */
    private static void listChildrenParallel(
        StorageProvider provider,
        List<StoragePath> dirs,
        int concurrency,
        Executor executor,
        AtomicInteger issued,
        BooleanSupplier cancelled,
        @Nullable Consumer<StorageChildren> consumeListed,
        ActionListener<StorageChildren[]> listener
    ) {
        int size = dirs.size();
        if (size == 0) {
            listener.onResponse(new StorageChildren[0]);
            return;
        }
        if (concurrency <= 1 || executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
            StorageChildren[] listed;
            try {
                listed = listChildrenSequential(provider, dirs, issued, cancelled, consumeListed);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(listed);
            return;
        }
        AtomicReference<AtomicReferenceArray<StorageChildren>> liveSlots = new AtomicReference<>(new AtomicReferenceArray<>(size));
        AtomicReference<Exception> failure = new AtomicReference<>();
        ThrottledIterator.run(indexIterator(size), (Releasable releasable, Integer i) -> {
            boolean submitted = false;
            try {
                Exception cancelledNow = cancellationFailure(cancelled);
                if (failure.get() != null || cancelledNow != null) {
                    if (cancelledNow != null) {
                        recordBatchFailure(failure, cancelledNow);
                    }
                    discardSlotResults(liveSlots);
                    return;
                }
                Runnable list = () -> {
                    try {
                        Exception cancelledInSlot = cancellationFailure(cancelled);
                        if (failure.get() == null && cancelledInSlot == null) {
                            issued.incrementAndGet();
                            StorageChildren children = provider.listChildren(dirs.get(i), MAX_LISTED_CHILDREN);
                            Exception cancelledAfter = cancellationFailure(cancelled);
                            if (cancelledAfter != null || failure.get() != null) {
                                // Sync listChildren cannot abort mid-call. Drop this payload and any
                                // already-ordered sibling slots; the batch will not merge them.
                                if (cancelledAfter != null) {
                                    recordBatchFailure(failure, cancelledAfter);
                                }
                                discardSlotResults(liveSlots);
                            } else if (children == null) {
                                // Same as sequential: a null child withdraws the walk. Signal so
                                // unstarted siblings skip; completion maps this to onResponse(null)
                                // unless a sibling store failure wins.
                                recordBatchFailure(failure, new ListChildrenUnsupported());
                                discardSlotResults(liveSlots);
                            } else {
                                if (consumeListed != null) {
                                    consumeListed.accept(children);
                                    children = SLOT_DONE;
                                }
                                storeSlot(liveSlots, i, children);
                                cancelledAfter = cancellationFailure(cancelled);
                                if (cancelledAfter != null || failure.get() != null) {
                                    if (cancelledAfter != null) {
                                        recordBatchFailure(failure, cancelledAfter);
                                    }
                                    discardSlotResults(liveSlots);
                                }
                            }
                        } else {
                            if (cancelledInSlot != null) {
                                recordBatchFailure(failure, cancelledInSlot);
                            }
                            discardSlotResults(liveSlots);
                        }
                    } catch (Exception e) {
                        recordBatchFailure(failure, e);
                        discardSlotResults(liveSlots);
                    } finally {
                        releasable.close();
                    }
                };
                executor.execute(list);
                submitted = true;
            } catch (Exception submitEx) {
                recordBatchFailure(failure, submitEx);
                discardSlotResults(liveSlots);
            } finally {
                if (submitted == false) {
                    releasable.close();
                }
            }
        }, Math.max(1, concurrency), () -> {
            Exception cancelledNow = cancellationFailure(cancelled);
            if (cancelledNow != null) {
                discardSlotResults(liveSlots);
                listener.onFailure(cancelledNow);
                return;
            }
            Exception e = failure.get();
            if (e instanceof ListChildrenUnsupported) {
                discardSlotResults(liveSlots);
                listener.onResponse(null);
                return;
            }
            if (e != null) {
                discardSlotResults(liveSlots);
                listener.onFailure(e);
                return;
            }
            AtomicReferenceArray<StorageChildren> results = liveSlots.get();
            if (results == null) {
                cancelledNow = cancellationFailure(cancelled);
                listener.onFailure(
                    cancelledNow != null ? cancelledNow : new IllegalStateException("walk listing ended without slot results")
                );
                return;
            }
            StorageChildren[] listed = new StorageChildren[size];
            for (int i = 0; i < size; i++) {
                StorageChildren slot = results.get(i);
                if (slot == null) {
                    discardSlotResults(liveSlots);
                    cancelledNow = cancellationFailure(cancelled);
                    listener.onFailure(
                        cancelledNow != null
                            ? cancelledNow
                            : new IllegalStateException("walk listing ended without a result for directory slot [" + i + "]")
                    );
                    return;
                }
                listed[i] = slot;
            }
            discardSlotResults(liveSlots);
            cancelledNow = cancellationFailure(cancelled);
            if (cancelledNow != null) {
                listener.onFailure(cancelledNow);
                return;
            }
            listener.onResponse(listed);
        }, executor, e -> {
            if (recordBatchFailure(failure, e) == false) {
                logger.debug("Additional failure during partition-walk listChildren (suppressed by first failure)", e);
            }
            discardSlotResults(liveSlots);
        });
    }

    /**
     * Evaluates {@code cancelled} without throwing. {@code null} means continue; otherwise the walk must stop
     * and complete the listener with this exception (cancelled, or the check itself failed).
     */
    @Nullable
    private static Exception cancellationFailure(BooleanSupplier cancelled) {
        try {
            return cancelled.getAsBoolean() ? new TaskCancelledException("listing cancelled") : null;
        } catch (Exception e) {
            return e;
        }
    }

    /**
     * First-wins, except a store failure or cancellation replaces a withdraw
     * ({@link ListChildrenUnsupported}) so a retryable 503 is not flattened away.
     */
    private static boolean recordBatchFailure(AtomicReference<Exception> failure, Exception e) {
        while (true) {
            Exception current = failure.get();
            if (current == null) {
                if (failure.compareAndSet(null, e)) {
                    return true;
                }
                continue;
            }
            if (current instanceof ListChildrenUnsupported && (e instanceof ListChildrenUnsupported) == false) {
                if (failure.compareAndSet(current, e)) {
                    return true;
                }
                continue;
            }
            return false;
        }
    }

    private static void storeSlot(AtomicReference<AtomicReferenceArray<StorageChildren>> liveSlots, int i, StorageChildren children) {
        AtomicReferenceArray<StorageChildren> results = liveSlots.get();
        if (results != null) {
            results.set(i, children);
        }
    }

    /** Detach and null the slot array once. Later stores see a null live buffer and drop the payload. */
    private static void discardSlotResults(AtomicReference<AtomicReferenceArray<StorageChildren>> liveSlots) {
        AtomicReferenceArray<StorageChildren> results = liveSlots.getAndSet(null);
        if (results == null) {
            return;
        }
        for (int i = 0; i < results.length(); i++) {
            results.set(i, null);
        }
    }

    private static Iterator<Integer> indexIterator(int count) {
        return new Iterator<>() {
            private int next = 0;

            @Override
            public boolean hasNext() {
                return next < count;
            }

            @Override
            public Integer next() {
                if (next >= count) {
                    throw new NoSuchElementException();
                }
                return next++;
            }
        };
    }

    /**
     * Mutable walk state shared by the sequential loop and the level-parallel async continuation.
     * One session is one walk; {@link #runAsync} must not re-enter a session that is already running.
     */
    private static final class WalkSession {
        private final StorageProvider provider;
        private final List<PartitionFilterHint> hints;
        private final Set<String> keyedGlobKeys;
        private final FolderRangePredicate folderRange;
        private final int maxDirectoryListings;
        private final BooleanSupplier cancelled;
        private final FinishMode finishMode;
        private final Collector collector;
        private final @Nullable WalkLayout templateLayout;
        private final boolean declineWalk;
        private final Set<String> pending = new HashSet<>();
        private final Set<String> seenKeys = new HashSet<>();
        private final Set<String> prunedColumns = new HashSet<>();
        private final List<StoragePath> prunedDirs = new ArrayList<>();
        private final Map<String, List<String>> seenValues = new LinkedHashMap<>();
        private boolean anyLevelHinted;
        private int walkLevel;
        private List<StoragePath> dirs;
        /** Issued {@code listChildren} from the walk AND from retroactive peeks into pruned dirs. */
        private final AtomicInteger listings = new AtomicInteger();
        /**
         * Product over walked levels of (shaped children / listed directories). Estimates how many dirs
         * of the current grain sit under the prefix, so finishSurvivors can compare remaining finish pages
         * against a flat listing of the prefix.
         */
        private double estimatedPrefixWidth = 1.0;
        /**
         * Pruned dirs from the last placeholder, candidates for a one-level retroactive peek. A peek fires
         * only when the current level introduces a new partition key (confirming a multi-level layout):
         * peeking into a pruned dir whose direct children are files (single-level layout) would pollute
         * enumeration tracking without adding any partition-key information. TEMPLATE identity levels
         * between placeholders advance these dirs one folder deeper so {@code {year}/data/{month}} peeks
         * month folders under a pruned year, not {@code data} itself as a month value.
         */
        private List<StoragePath> pendingPeeks = List.of();

        WalkSession(
            StorageProvider provider,
            StoragePath prefix,
            GlobMatcher matcher,
            ExclusionConfig.NameFilter nameFilter,
            List<PartitionFilterHint> hints,
            int maxDiscoveredFiles,
            String glob,
            @Nullable FolderRangePredicate folderRange,
            @Nullable Predicate<StorageEntry> fileKeep,
            int maxListedObjects,
            int maxDirectoryListings,
            BooleanSupplier cancelled,
            @Nullable String pathTemplate
        ) {
            this.provider = provider;
            this.hints = hints;
            this.folderRange = folderRange;
            this.maxDirectoryListings = maxDirectoryListings;
            this.cancelled = cancelled;
            this.finishMode = FINISH_MODE.get();
            this.collector = new Collector(prefix.toString(), matcher, nameFilter, maxDiscoveredFiles, fileKeep, maxListedObjects);
            WalkLayout layout = pathTemplate == null ? null : TemplatePartitionDetector.walkLayout(prefix, glob, pathTemplate);
            this.declineWalk = pathTemplate != null && layout == null;
            this.templateLayout = layout;
            this.keyedGlobKeys = layout != null ? layout.placeholderKeys() : keyedGlobKeys(glob);
            for (PartitionFilterHint hint : hints) {
                pending.add(hint.columnName());
            }
            if (folderRange != null) {
                pending.addAll(folderRange.bindKeys());
            }
            if (layout != null) {
                for (Map.Entry<String, String> binding : layout.prefixBindings().entrySet()) {
                    seenKeys.add(binding.getKey());
                    pending.remove(binding.getKey());
                }
            } else {
                // A key prunes only at its outermost occurrence: HivePartitionDetector binds the FIRST key=value
                // segment of a path. The prefix's own segments count — under data/year=2024/ every file's year IS
                // 2024, so a deeper year=X folder is just a name and pruning it against a year hint would drop
                // matching rows.
                for (String segment : prefix.path().split("/")) {
                    String prefixKey = PartitionValueMatcher.folderKey(segment);
                    if (prefixKey != null) {
                        seenKeys.add(prefixKey);
                        pending.remove(prefixKey);
                    }
                }
            }
            this.dirs = List.of(prefix);
        }

        @Nullable
        WalkResult runSequential() throws IOException {
            if (declineWalk) {
                return null;
            }
            while (dirs.isEmpty() == false) {
                if (cancelled.getAsBoolean()) {
                    throw new TaskCancelledException("listing cancelled");
                }
                if (shouldList() == false) {
                    return afterListingsStop();
                }
                StorageChildren[] listed = listChildrenSequential(provider, dirs, listings, cancelled);
                if (listed == null) {
                    return null;
                }
                ClassifiedLevel level = classifyListed(listed);
                if (level == null) {
                    return null;
                }
                if (applyPeeksIfNeeded(level) == false) {
                    return null;
                }
                WalkResult done = applyClassified(level);
                if (done != null || level.continueWalk == false) {
                    return done;
                }
            }
            return collector.result(prunedColumns, inferColumnTypes(seenValues), prunedDirs);
        }

        void runAsync(int concurrency, Executor executor, ActionListener<WalkResult> listener) {
            if (declineWalk) {
                listener.onResponse(null);
                return;
            }
            advanceAsync(concurrency, executor, listener);
        }

        private void advanceAsync(int concurrency, Executor executor, ActionListener<WalkResult> listener) {
            Exception cancelledNow = cancellationFailure(cancelled);
            if (cancelledNow != null) {
                listener.onFailure(cancelledNow);
                return;
            }
            if (shouldList()) {
                listChildrenParallel(provider, dirs, concurrency, executor, listings, cancelled, new ActionListener<>() {
                    @Override
                    public void onResponse(StorageChildren[] listed) {
                        if (listed == null) {
                            listener.onResponse(null);
                            return;
                        }
                        ClassifiedLevel level;
                        try {
                            level = classifyListed(listed);
                        } catch (Exception e) {
                            listener.onFailure(e);
                            return;
                        }
                        if (level == null) {
                            listener.onResponse(null);
                            return;
                        }
                        applyPeeksIfNeededAsync(level, concurrency, executor, new ActionListener<>() {
                            @Override
                            public void onResponse(Boolean peekOk) {
                                Exception cancelledNow = cancellationFailure(cancelled);
                                if (cancelledNow != null) {
                                    listener.onFailure(cancelledNow);
                                    return;
                                }
                                if (peekOk == false) {
                                    listener.onResponse(null);
                                    return;
                                }
                                WalkResult done;
                                boolean continueWalk;
                                try {
                                    done = applyClassified(level);
                                    continueWalk = done == null && level.continueWalk;
                                } catch (Exception e) {
                                    listener.onFailure(e);
                                    return;
                                }
                                if (continueWalk) {
                                    advanceAsync(concurrency, executor, listener);
                                    return;
                                }
                                listener.onResponse(done);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                listener.onFailure(e);
                            }
                        });
                    }

                    @Override
                    public void onFailure(Exception e) {
                        listener.onFailure(e);
                    }
                });
                return;
            }
            ActionListener.completeWith(listener, this::afterListingsStop);
        }

        /**
         * Retroactive peek: a new partition key at this level confirms a multi-level layout, so it is now
         * safe to list the direct children of dirs pruned at the previous level. Any {@code key=value} dirs
         * found contribute shadow values to {@code seenValues}, letting the caller detect whether a pruned
         * subtree was the sole source of a type-widening value for a non-pruned column. Sequential walk
         * lists peeks in order; {@link #applyPeeksIfNeededAsync} uses the same budget slice on
         * {@link #listChildrenParallel} so issued counts still match when the ceiling cuts a peek batch.
         */
        private boolean applyPeeksIfNeeded(ClassifiedLevel level) throws IOException {
            if (level.newKeyLevel && pendingPeeks.isEmpty() == false) {
                if (processPendingPeeks(
                    provider,
                    pendingPeeks,
                    seenValues,
                    listings,
                    maxDirectoryListings,
                    cancelled,
                    templateLayout,
                    walkLevel
                ) == false) {
                    return false;
                }
                pendingPeeks = List.of();
            }
            return true;
        }

        /**
         * Async counterpart of {@link #applyPeeksIfNeeded}. Completes {@code listener} on every path:
         * {@code true} when peeks finished or were not needed, {@code false} to fail the walk closed.
         * Does not block; peek LIST calls go through {@link #listChildrenParallel}.
         */
        private void applyPeeksIfNeededAsync(ClassifiedLevel level, int concurrency, Executor executor, ActionListener<Boolean> listener) {
            if (level.newKeyLevel == false || pendingPeeks.isEmpty()) {
                listener.onResponse(true);
                return;
            }
            Exception cancelledNow = cancellationFailure(cancelled);
            if (cancelledNow != null) {
                listener.onFailure(cancelledNow);
                return;
            }
            List<StoragePath> toPeek = peeksToIssue(pendingPeeks, listings.get(), maxDirectoryListings);
            if (toPeek == null) {
                listener.onResponse(false);
                return;
            }
            boolean truncated = toPeek.size() < pendingPeeks.size();
            listChildrenParallel(provider, toPeek, concurrency, executor, listings, cancelled, children -> {
                synchronized (seenValues) {
                    applyPeekChildren(children, seenValues, templateLayout, walkLevel);
                }
            }, new ActionListener<>() {
                @Override
                public void onResponse(StorageChildren[] listed) {
                    Exception cancelledNow = cancellationFailure(cancelled);
                    if (cancelledNow != null) {
                        listener.onFailure(cancelledNow);
                        return;
                    }
                    if (listed == null) {
                        listener.onResponse(false);
                        return;
                    }
                    boolean ok = truncated == false;
                    if (ok) {
                        pendingPeeks = List.of();
                    }
                    listener.onResponse(ok);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        /** True when a later glob-directory slot is a TEMPLATE placeholder that still needs a peek. */
        private boolean laterPlaceholder() {
            for (int i = walkLevel + 1; i < templateLayout.depth(); i++) {
                if (templateLayout.level(i).isPlaceholder()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Walks each pending peek one identity folder deeper. Without this, a later placeholder would
         * list the identity folder itself as if it were a partition value. Budget miss or a directory
         * too wide to buffer fails closed (same as {@link #applyPeeksIfNeeded}). Stray siblings inside
         * a pruned subtree are skipped — that layout is already untrusted for walking. Callers skip this
         * when {@link #laterPlaceholder()} is false so a trailing literal does not list unused peeks.
         */
        private boolean advancePendingPeeks(WalkLayout.Level role) throws IOException {
            if (pendingPeeks.isEmpty()) {
                return true;
            }
            List<StoragePath> next = new ArrayList<>(pendingPeeks.size());
            for (StoragePath pruned : pendingPeeks) {
                if (cancelled.getAsBoolean()) {
                    throw new TaskCancelledException("listing cancelled");
                }
                int issued = listings.get();
                if (issued >= maxDirectoryListings) {
                    return false;
                }
                listings.set(issued + 1);
                StorageChildren children = provider.listChildren(pruned, MAX_LISTED_CHILDREN);
                if (children == null) {
                    return false;
                }
                for (StoragePath sub : children.directories()) {
                    if (role.isLiteral() == false || role.literal().equals(sub.objectName())) {
                        next.add(sub);
                    }
                }
            }
            pendingPeeks = next;
            return true;
        }

        private WalkResult finishSurvivors() throws IOException {
            return PartitionPruningWalk.finishSurvivors(
                collector,
                provider,
                dirs,
                prunedColumns,
                inferColumnTypes(seenValues),
                estimatedPrefixWidth,
                prunedDirs,
                finishMode,
                templateLayout != null
            );
        }

        /**
         * Remaining pending can be a data-column hint ({@code WHERE partition AND srcaddr=…}). Hive
         * keeps listing because a deeper {@code key=value} might still appear; a TEMPLATE layout has
         * no further glob-directory slots, so another list would be past {@link WalkLayout#depth()}.
         * Stop before that list.
         */
        private boolean shouldList() {
            if (dirs.isEmpty() || pending.isEmpty() || listings.get() + dirs.size() > maxDirectoryListings) {
                return false;
            }
            return templateLayout == null || walkLevel < templateLayout.depth();
        }

        @Nullable
        private WalkResult afterListingsStop() throws IOException {
            if (dirs.isEmpty()) {
                return collector.result(prunedColumns, inferColumnTypes(seenValues), prunedDirs);
            }
            if (pending.isEmpty() || listings.get() + dirs.size() > maxDirectoryListings) {
                return finishSurvivors();
            }
            return anyLevelHinted ? finishSurvivors() : null;
        }

        /**
         * Interprets already-listed children of {@link #dirs}. No further I/O. Direct files at this
         * folder level are deferred until {@link #applyClassified} so a finish-survivors recursive
         * listing of the same parent does not double-count them. A glob-matching file at a folder
         * level breaks Hive detection; the caller's validation then rejects the walk. Unshaped dirs
         * (junk, nested root) are descended untouched. An empty directory name (S3 {@code foo//})
         * returns {@code null} so the walk withdraws rather than binding the next folder.
         */
        @Nullable
        private ClassifiedLevel classifyListed(StorageChildren[] listed) {
            int listedDirs = dirs.size();
            List<StoragePath> shapedDirs = new ArrayList<>();
            List<String> shapedKeys = new ArrayList<>();
            List<String> shapedValues = new ArrayList<>();
            List<StoragePath> next = new ArrayList<>();
            List<StorageEntry> levelFiles = new ArrayList<>();
            boolean withdraw = false;
            boolean identityLevel = false;
            if (templateLayout != null) {
                WalkLayout.Level role = templateLayout.level(walkLevel);
                identityLevel = role.isPlaceholder() == false;
                for (StorageChildren children : listed) {
                    for (StorageEntry file : children.files()) {
                        levelFiles.add(file);
                    }
                    for (StoragePath sub : children.directories()) {
                        String name = sub.objectName();
                        if (name == null || name.isEmpty()) {
                            // Empty path segment (S3 common prefix foo//). Binding the next folder
                            // would prune a glob match. Withdraw.
                            return ClassifiedLevel.withdrawn(listedDirs, levelFiles);
                        }
                        if (role.isPlaceholder()) {
                            shapedDirs.add(sub);
                            shapedKeys.add(role.name());
                            shapedValues.add(role.decodedFolder(name));
                        } else if (role.isLiteral()) {
                            if (role.literal().equals(name)) {
                                next.add(sub);
                            } else if (role.globSpelled() == false) {
                                withdraw = true;
                            }
                        } else {
                            next.add(sub);
                        }
                    }
                }
                if (withdraw) {
                    return ClassifiedLevel.withdrawn(listedDirs, levelFiles);
                }
            } else {
                for (StorageChildren children : listed) {
                    for (StorageEntry file : children.files()) {
                        levelFiles.add(file);
                    }
                    for (StoragePath sub : children.directories()) {
                        String name = sub.objectName();
                        if (name == null || name.isEmpty()) {
                            // Empty path segment (S3 common prefix foo//). Template alignment skips it;
                            // binding the next folder would prune a glob match. Withdraw.
                            return ClassifiedLevel.withdrawn(listedDirs, levelFiles);
                        }
                        String key = PartitionValueMatcher.folderKey(name);
                        if (key == null) {
                            next.add(sub);
                        } else {
                            shapedDirs.add(sub);
                            shapedKeys.add(key);
                            shapedValues.add(PartitionValueMatcher.folderValue(name));
                        }
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
                hintedLevel = true;
                for (int i = 0; i < keep.length; i++) {
                    if (keep[i] == false) {
                        continue;
                    }
                    Map<String, String> bindings = templateLayout != null
                        ? templateLayout.bindings(shapedDirs.get(i).path())
                        : PartitionValueMatcher.pathBindings(shapedDirs.get(i).path());
                    if (folderRange.mayContain(bindings) == false) {
                        keep[i] = false;
                        prunedColumns.add(shapedKeys.get(i));
                    }
                }
            }
            return new ClassifiedLevel(
                listedDirs,
                shapedDirs,
                keep,
                next,
                levelFiles,
                byKey.keySet(),
                hintedLevel,
                newKeyLevel,
                false,
                identityLevel
            );
        }

        /**
         * Applies a classified level: peeks must already have been consumed. Surviving dirs become the
         * next walk frontier; pruned dirs are candidates for the next level's retroactive peek (and
         * the outermost-first inference-anchor probe list). Non-null is a finished walk; {@code null}
         * with {@code level.continueWalk} means keep walking; {@code null} without it withdraws.
         */
        @Nullable
        private WalkResult applyClassified(ClassifiedLevel level) throws IOException {
            if (level.withdraw) {
                dirs = List.of();
                return null;
            }
            List<StoragePath> newPeeks = null;
            for (int i = 0; i < level.keep.length; i++) {
                if (level.keep[i]) {
                    level.next.add(level.shapedDirs.get(i));
                } else {
                    if (newPeeks == null) {
                        newPeeks = new ArrayList<>();
                    }
                    newPeeks.add(level.shapedDirs.get(i));
                    if (prunedDirs.size() < MAX_PRUNED_DIRS_FOR_ANCHOR) {
                        prunedDirs.add(level.shapedDirs.get(i));
                    }
                }
            }
            if (level.identityLevel) {
                if (laterPlaceholder() && advancePendingPeeks(templateLayout.level(walkLevel)) == false) {
                    level.continueWalk = false;
                    return null;
                }
                for (StorageEntry file : level.levelFiles) {
                    collector.add(file);
                }
                estimatedPrefixWidth = bumpPrefixWidth(estimatedPrefixWidth, level.listedDirs, Math.max(level.next.size(), 1));
                dirs = level.next;
                walkLevel++;
                level.continueWalk = true;
                return null;
            }
            pendingPeeks = newPeeks != null ? newPeeks : List.of();
            if (level.hintedLevel == false) {
                if (descendUnhintedKeyedLevel(level.levelKeys, pending, keyedGlobKeys)) {
                    for (StorageEntry file : level.levelFiles) {
                        collector.add(file);
                    }
                    estimatedPrefixWidth = bumpPrefixWidth(estimatedPrefixWidth, level.listedDirs, level.shapedDirs.size());
                    dirs = level.next;
                    walkLevel++;
                    level.continueWalk = true;
                    return null;
                }
                if (anyLevelHinted == false) {
                    dirs = List.of();
                    return null;
                }
                return finishSurvivors();
            }
            for (StorageEntry file : level.levelFiles) {
                collector.add(file);
            }
            anyLevelHinted = true;
            estimatedPrefixWidth = bumpPrefixWidth(estimatedPrefixWidth, level.listedDirs, level.shapedDirs.size());
            dirs = level.next;
            walkLevel++;
            level.continueWalk = true;
            return null;
        }
    }

    private static final class ClassifiedLevel {
        final int listedDirs;
        final List<StoragePath> shapedDirs;
        final boolean[] keep;
        final List<StoragePath> next;
        final List<StorageEntry> levelFiles;
        final Set<String> levelKeys;
        final boolean hintedLevel;
        final boolean newKeyLevel;
        final boolean withdraw;
        final boolean identityLevel;
        boolean continueWalk;

        static ClassifiedLevel withdrawn(int listedDirs, List<StorageEntry> levelFiles) {
            return new ClassifiedLevel(
                listedDirs,
                List.of(),
                new boolean[0],
                new ArrayList<>(),
                levelFiles,
                Set.of(),
                false,
                false,
                true,
                false
            );
        }

        ClassifiedLevel(
            int listedDirs,
            List<StoragePath> shapedDirs,
            boolean[] keep,
            List<StoragePath> next,
            List<StorageEntry> levelFiles,
            Set<String> levelKeys,
            boolean hintedLevel,
            boolean newKeyLevel,
            boolean withdraw,
            boolean identityLevel
        ) {
            this.listedDirs = listedDirs;
            this.shapedDirs = shapedDirs;
            this.keep = keep;
            this.next = next;
            this.levelFiles = levelFiles;
            this.levelKeys = levelKeys;
            this.hintedLevel = hintedLevel;
            this.newKeyLevel = newKeyLevel;
            this.withdraw = withdraw;
            this.identityLevel = identityLevel;
        }
    }

    /**
     * Accumulates the files the walk keeps, applying exactly the flat listing's per-object rules: relative path,
     * placeholder skipping, glob matcher, exclusion filter (with the warning's count-and-example tally),
     * {@code _file.*} keep (including {@code LIKE}), then the discovery cap. Visited objects count toward
     * {@code max_listed_objects} before a LIKE miss, matching the flat listing.
     */
    private static final class Collector {
        private final String prefixStr;
        private final GlobMatcher matcher;
        private final ExclusionConfig.NameFilter nameFilter;
        private final int maxDiscoveredFiles;
        @Nullable
        private final Predicate<StorageEntry> fileKeep;
        private final int maxListedObjects;
        private final List<StorageEntry> matched = new ArrayList<>();
        @Nullable
        private StorageEntry fileKeepStash;
        private int excludedCount = 0;
        private int globKeptCount = 0;
        private int listed = 0;
        private String excludedExample = null;
        private String excludedExampleEntry = null;

        Collector(
            String prefixStr,
            GlobMatcher matcher,
            ExclusionConfig.NameFilter nameFilter,
            int maxDiscoveredFiles,
            @Nullable Predicate<StorageEntry> fileKeep,
            int maxListedObjects
        ) {
            this.prefixStr = prefixStr;
            this.matcher = matcher;
            this.nameFilter = nameFilter;
            this.maxDiscoveredFiles = maxDiscoveredFiles;
            this.fileKeep = fileKeep;
            this.maxListedObjects = maxListedObjects;
        }

        void add(StorageEntry entry) {
            String entryPath = entry.path().toString();
            String relativePath = entryPath.startsWith(prefixStr) ? entryPath.substring(prefixStr.length()) : entry.path().objectName();
            if (relativePath.isEmpty() || relativePath.endsWith("/")) {
                return; // directory placeholder key, skipped as in the flat listing
            }
            listed++;
            GlobExpander.checkListedObjectsLimit(listed, maxListedObjects);
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
            globKeptCount++;
            if (fileKeep != null && fileKeep.test(entry) == false) {
                if (fileKeepStash == null) {
                    fileKeepStash = entry;
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
                List.copyOf(prunedDirs),
                fileKeepStash,
                globKeptCount
            );
        }
    }

    /**
     * {@link StorageProvider#listChildren} returned {@code null}: the walk cannot continue and the caller
     * falls back to a flat listing. Not a store failure.
     */
    private static final class ListChildrenUnsupported extends IOException {
        ListChildrenUnsupported() {
            super("listChildren unsupported or too wide");
        }
    }
}
