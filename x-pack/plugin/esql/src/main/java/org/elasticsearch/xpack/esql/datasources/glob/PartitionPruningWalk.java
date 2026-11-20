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
import org.elasticsearch.xpack.esql.datasources.spi.ExternalUnavailableException;
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
import java.util.function.Predicate;

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
    ) {
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
    ) {
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
    ) {
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
    ) {
        try {
            return walk(
                provider,
                prefix,
                matcher,
                nameFilter,
                hints,
                maxDiscoveredFiles,
                keyedGlobKeys(glob),
                folderRange,
                fileKeep,
                maxListedObjects,
                maxDirectoryListings,
                cancelled
            );
        } catch (TaskCancelledException | ExternalUnavailableException e) {
            throw e;
        } catch (IOException e) {
            logger.debug("Partition-pruning walk of [{}] failed; falling back to a flat listing", prefix, e);
            return null;
        }
    }

    /**
     * Async counterpart of {@link #tryWalk}: each directory level is listed concurrently on
     * {@code executor} via {@link ThrottledIterator}, results merged in directory order. The calling
     * thread is not blocked; {@code listener} fires on an executor thread. A failed or cancelled
     * child fails the walk closed ({@code null} for {@link IOException}, {@link TaskCancelledException}
     * for cancel, other failures on {@code listener.onFailure}). Output and LIST count match the
     * sequential walk.
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
        WalkSession session;
        try {
            session = new WalkSession(
                provider,
                prefix,
                matcher,
                nameFilter,
                hints,
                maxDiscoveredFiles,
                keyedGlobKeys(glob),
                folderRange,
                fileKeep,
                maxListedObjects,
                maxDirectoryListings,
                cancelled
            );
        } catch (RuntimeException e) {
            listener.onFailure(e);
            return;
        }
        session.runAsync(Math.max(1, concurrency), executor, new ActionListener<>() {
            @Override
            public void onResponse(WalkResult walk) {
                listener.onResponse(walk);
            }

            @Override
            public void onFailure(Exception e) {
                if (e instanceof IOException) {
                    logger.debug("Partition-pruning walk of [{}] failed; falling back to a flat listing", prefix, e);
                    listener.onResponse(null);
                    return;
                }
                listener.onFailure(e);
            }
        });
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
        @Nullable FolderRangePredicate folderRange,
        @Nullable Predicate<StorageEntry> fileKeep,
        int maxListedObjects,
        int maxDirectoryListings,
        BooleanSupplier cancelled
    ) throws IOException {
        return new WalkSession(
            provider,
            prefix,
            matcher,
            nameFilter,
            hints,
            maxDiscoveredFiles,
            keyedGlobKeys,
            folderRange,
            fileKeep,
            maxListedObjects,
            maxDirectoryListings,
            cancelled
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
     * entries to {@code seenValues} as shadow values for type-divergence detection. Returns the updated listings
     * count, or {@code -1} when a peek could not complete (budget exhausted or directory too wide to buffer) —
     * the caller should return {@code null} (fall back to flat listing) in that case.
     */
    private static int processPendingPeeks(
        StorageProvider provider,
        List<StoragePath> peeks,
        Map<String, List<String>> seenValues,
        int listings,
        int maxDirectoryListings,
        BooleanSupplier cancelled
    ) throws IOException {
        for (StoragePath pruned : peeks) {
            if (cancelled.getAsBoolean()) {
                throw new TaskCancelledException("listing cancelled");
            }
            if (listings >= maxDirectoryListings) {
                return -1;
            }
            listings++;
            StorageChildren peekChildren = provider.listChildren(pruned, MAX_LISTED_CHILDREN);
            if (peekChildren == null) {
                return -1;
            }
            applyPeekChildren(peekChildren, seenValues);
        }
        return listings;
    }

    private static void applyPeekChildren(StorageChildren peekChildren, Map<String, List<String>> seenValues) {
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
        if (prunedColumns.isEmpty()) {
            return null;
        }
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
     * Lists {@code dirs} in order on the calling thread. Increments {@code issued} once per started call.
     * {@code null} means a child listing was unsupported or too wide — the walk withdraws.
     */
    private static StorageChildren[] listChildrenSequential(
        StorageProvider provider,
        List<StoragePath> dirs,
        AtomicInteger issued,
        BooleanSupplier cancelled
    ) throws IOException {
        StorageChildren[] listed = new StorageChildren[dirs.size()];
        for (int i = 0; i < dirs.size(); i++) {
            if (cancelled.getAsBoolean()) {
                throw new TaskCancelledException("listing cancelled");
            }
            issued.incrementAndGet();
            listed[i] = provider.listChildren(dirs.get(i), MAX_LISTED_CHILDREN);
            if (listed[i] == null) {
                return null;
            }
        }
        return listed;
    }

    /**
     * Level-parallel {@code listChildren}: {@link ThrottledIterator} on {@code executor}, results merged in
     * directory-slot order. A failed, cancelled, or {@code null} child fails the level (same as sequential:
     * {@code null} withdraws the walk); unstarted slots are skipped and do not increment {@code issued}.
     * The listener fires on an executor thread. Does not block the caller.
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
        int size = dirs.size();
        if (size == 0) {
            listener.onResponse(new StorageChildren[0]);
            return;
        }
        if (concurrency <= 1 || executor == EsExecutors.DIRECT_EXECUTOR_SERVICE) {
            StorageChildren[] listed;
            try {
                listed = listChildrenSequential(provider, dirs, issued, cancelled);
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            listener.onResponse(listed);
            return;
        }
        AtomicReferenceArray<StorageChildren> results = new AtomicReferenceArray<>(size);
        AtomicReference<Exception> failure = new AtomicReference<>();
        ThrottledIterator.run(indexIterator(size), (Releasable releasable, Integer i) -> {
            if (failure.get() != null || cancelled.getAsBoolean()) {
                failure.compareAndSet(null, new TaskCancelledException("listing cancelled"));
                releasable.close();
                return;
            }
            Runnable list = () -> {
                try {
                    if (failure.get() == null && cancelled.getAsBoolean() == false) {
                        issued.incrementAndGet();
                        StorageChildren children = provider.listChildren(dirs.get(i), MAX_LISTED_CHILDREN);
                        if (children == null) {
                            // Same as sequential: a null child withdraws the walk. Treat as failure so
                            // unstarted siblings skip and LIST count stays close to sequential.
                            failure.compareAndSet(null, new IOException("listChildren unsupported or too wide"));
                        } else {
                            results.set(i, children);
                        }
                    } else {
                        failure.compareAndSet(null, new TaskCancelledException("listing cancelled"));
                    }
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                } finally {
                    releasable.close();
                }
            };
            try {
                executor.execute(list);
            } catch (Exception submitEx) {
                try {
                    failure.compareAndSet(null, submitEx);
                } finally {
                    releasable.close();
                }
            }
        }, Math.max(1, concurrency), () -> {
            Exception e = failure.get();
            if (e != null) {
                listener.onFailure(e);
                return;
            }
            StorageChildren[] listed = new StorageChildren[size];
            for (int i = 0; i < size; i++) {
                StorageChildren slot = results.get(i);
                if (slot == null) {
                    listener.onFailure(
                        cancelled.getAsBoolean()
                            ? new TaskCancelledException("listing cancelled")
                            : new IllegalStateException("walk listing ended without a result for directory slot [" + i + "]")
                    );
                    return;
                }
                listed[i] = slot;
            }
            listener.onResponse(listed);
        }, executor, e -> {
            if (failure.compareAndSet(null, e) == false) {
                logger.debug("Additional failure during partition-walk listChildren (suppressed by first failure)", e);
            }
        });
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
        private final Set<String> pending = new HashSet<>();
        private final Set<String> seenKeys = new HashSet<>();
        private final Set<String> prunedColumns = new HashSet<>();
        private final List<StoragePath> prunedDirs = new ArrayList<>();
        private final Map<String, List<String>> seenValues = new LinkedHashMap<>();
        private boolean anyLevelHinted;
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
         * Pruned dirs from the previous level, candidates for a one-level retroactive peek. A peek fires
         * only when the current level introduces a new partition key (confirming a multi-level layout):
         * peeking into a pruned dir whose direct children are files (single-level layout) would pollute
         * enumeration tracking without adding any partition-key information.
         */
        private List<StoragePath> pendingPeeks = List.of();

        WalkSession(
            StorageProvider provider,
            StoragePath prefix,
            GlobMatcher matcher,
            ExclusionConfig.NameFilter nameFilter,
            List<PartitionFilterHint> hints,
            int maxDiscoveredFiles,
            Set<String> keyedGlobKeys,
            @Nullable FolderRangePredicate folderRange,
            @Nullable Predicate<StorageEntry> fileKeep,
            int maxListedObjects,
            int maxDirectoryListings,
            BooleanSupplier cancelled
        ) {
            this.provider = provider;
            this.hints = hints;
            this.keyedGlobKeys = keyedGlobKeys;
            this.folderRange = folderRange;
            this.maxDirectoryListings = maxDirectoryListings;
            this.cancelled = cancelled;
            this.finishMode = FINISH_MODE.get();
            this.collector = new Collector(prefix.toString(), matcher, nameFilter, maxDiscoveredFiles, fileKeep, maxListedObjects);
            for (PartitionFilterHint hint : hints) {
                pending.add(hint.columnName());
            }
            if (folderRange != null) {
                pending.addAll(folderRange.bindKeys());
            }
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
            this.dirs = List.of(prefix);
        }

        @Nullable
        WalkResult runSequential() throws IOException {
            while (dirs.isEmpty() == false) {
                if (cancelled.getAsBoolean()) {
                    throw new TaskCancelledException("listing cancelled");
                }
                if (pending.isEmpty() || listings.get() + dirs.size() > maxDirectoryListings) {
                    return finishSurvivors();
                }
                StorageChildren[] listed = listChildrenSequential(provider, dirs, listings, cancelled);
                if (listed == null) {
                    return null;
                }
                ClassifiedLevel level = classifyListed(listed);
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
            advanceAsync(concurrency, executor, listener);
        }

        private void advanceAsync(int concurrency, Executor executor, ActionListener<WalkResult> listener) {
            final WalkResult early;
            final boolean listLevel;
            final boolean cancelledNow;
            try {
                if (cancelled.getAsBoolean()) {
                    cancelledNow = true;
                    early = null;
                    listLevel = false;
                } else if (dirs.isEmpty()) {
                    cancelledNow = false;
                    early = collector.result(prunedColumns, inferColumnTypes(seenValues), prunedDirs);
                    listLevel = false;
                } else if (pending.isEmpty() || listings.get() + dirs.size() > maxDirectoryListings) {
                    cancelledNow = false;
                    early = finishSurvivors();
                    listLevel = false;
                } else {
                    cancelledNow = false;
                    early = null;
                    listLevel = true;
                }
            } catch (Exception e) {
                listener.onFailure(e);
                return;
            }
            if (cancelledNow) {
                listener.onFailure(new TaskCancelledException("listing cancelled"));
                return;
            }
            if (listLevel == false) {
                listener.onResponse(early);
                return;
            }
            listChildrenParallel(provider, dirs, concurrency, executor, listings, cancelled, new ActionListener<>() {
                @Override
                public void onResponse(StorageChildren[] listed) {
                    WalkResult done;
                    boolean continueWalk;
                    try {
                        if (listed == null) {
                            done = null;
                            continueWalk = false;
                        } else {
                            ClassifiedLevel level = classifyListed(listed);
                            if (applyPeeksIfNeeded(level) == false) {
                                done = null;
                                continueWalk = false;
                            } else {
                                done = applyClassified(level);
                                continueWalk = done == null && level.continueWalk;
                            }
                        }
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

        /**
         * Retroactive peek: a new partition key at this level confirms a multi-level layout, so it is now
         * safe to list the direct children of dirs pruned at the previous level. Any {@code key=value} dirs
         * found contribute shadow values to {@code seenValues}, letting the caller detect whether a pruned
         * subtree was the sole source of a type-widening value for a non-pruned column. Sequential so issued
         * counts match the sequential walk when the ceiling is nearby; parallel peeks are a follow-up.
         */
        private boolean applyPeeksIfNeeded(ClassifiedLevel level) throws IOException {
            if (level.newKeyLevel && pendingPeeks.isEmpty() == false) {
                int updated = processPendingPeeks(provider, pendingPeeks, seenValues, listings.get(), maxDirectoryListings, cancelled);
                if (updated < 0) {
                    return false;
                }
                listings.set(updated);
                pendingPeeks = List.of();
            }
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
                finishMode
            );
        }

        /**
         * Interprets already-listed children of {@link #dirs}. No further I/O. Direct files at this
         * folder level are deferred until {@link #applyClassified} so a finish-survivors recursive
         * listing of the same parent does not double-count them. A glob-matching file at a folder
         * level breaks Hive detection; the caller's validation then rejects the walk. Unshaped dirs
         * (junk, nested root) are descended untouched.
         */
        private ClassifiedLevel classifyListed(StorageChildren[] listed) {
            int listedDirs = dirs.size();
            List<StoragePath> shapedDirs = new ArrayList<>();
            List<String> shapedKeys = new ArrayList<>();
            List<String> shapedValues = new ArrayList<>();
            List<StoragePath> next = new ArrayList<>();
            List<StorageEntry> levelFiles = new ArrayList<>();
            for (StorageChildren children : listed) {
                for (StorageEntry file : children.files()) {
                    levelFiles.add(file);
                }
                for (StoragePath sub : children.directories()) {
                    String key = PartitionValueMatcher.folderKey(sub.objectName());
                    if (key == null) {
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
                    Map<String, String> bindings = PartitionValueMatcher.pathBindings(shapedDirs.get(i).path());
                    if (folderRange.mayContain(bindings) == false) {
                        keep[i] = false;
                        prunedColumns.add(shapedKeys.get(i));
                    }
                }
            }
            return new ClassifiedLevel(listedDirs, shapedDirs, keep, next, levelFiles, byKey.keySet(), hintedLevel, newKeyLevel);
        }

        /**
         * Applies a classified level: peeks must already have been consumed. Surviving dirs become the
         * next walk frontier; pruned dirs are candidates for the next level's retroactive peek (and
         * the outermost-first inference-anchor probe list). Non-null is a finished walk; {@code null}
         * with {@code level.continueWalk} means keep walking; {@code null} without it withdraws.
         */
        @Nullable
        private WalkResult applyClassified(ClassifiedLevel level) throws IOException {
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
            pendingPeeks = newPeeks != null ? newPeeks : List.of();

            if (level.hintedLevel == false) {
                if (descendUnhintedKeyedLevel(level.levelKeys, pending, keyedGlobKeys)) {
                    for (StorageEntry file : level.levelFiles) {
                        collector.add(file);
                    }
                    estimatedPrefixWidth = bumpPrefixWidth(estimatedPrefixWidth, level.listedDirs, level.shapedDirs.size());
                    dirs = level.next;
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
        boolean continueWalk;

        ClassifiedLevel(
            int listedDirs,
            List<StoragePath> shapedDirs,
            boolean[] keep,
            List<StoragePath> next,
            List<StorageEntry> levelFiles,
            Set<String> levelKeys,
            boolean hintedLevel,
            boolean newKeyLevel
        ) {
            this.listedDirs = listedDirs;
            this.shapedDirs = shapedDirs;
            this.keep = keep;
            this.next = next;
            this.levelFiles = levelFiles;
            this.levelKeys = levelKeys;
            this.hintedLevel = hintedLevel;
            this.newKeyLevel = newKeyLevel;
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
}
