/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.datasources.PartitionConfig;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.PartitionSpec;
import org.elasticsearch.xpack.esql.datasources.PartitionValueMatcher;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counting in-memory store for listing-walk tests. {@link #listObjects} pages at {@link #LIST_PAGE_SIZE}
 * (one request per page). {@link #listChildren} is one request. Set {@link #childrenUnsupported} to force
 * the flat {@code listObjects} path used as the reference.
 *
 * <p>The reference is a flat prefix listing plus {@link PartitionValueMatcher} and
 * {@link PartitionSpec#overlaps}, then {@link FileOrderConfig#apply}. Listing must return the same files
 * (and partition columns, excluded count, notices, inference-anchor flag) as that filter; a strategy
 * change may only change cost.
 */
public final class ListingWalkHarness implements StorageProvider {

    /** S3 ListObjectsV2 default page. Each page is one LIST request. */
    public static final int LIST_PAGE_SIZE = 1000;

    public static final int FILES_PER_DAY = 8;

    public static final String LOGS_PREFIX = "s3://bucket/logs";
    /** Globstar so grain INs cannot splice {@code year=2026} and skip the walk. */
    public static final String LOGS_GLOB = "s3://bucket/logs/" + "**/*.parquet";
    public static final String PADDED_PREFIX = "s3://bucket/padded";
    public static final String PADDED_GLOB = "s3://bucket/padded/year=*/*/*/*.parquet";
    public static final String MIXED_PREFIX = "s3://bucket/mixed";
    /** Keyed {@code month=*} so {@code month == 6} splices and misses padded siblings. */
    public static final String MIXED_GLOB = "s3://bucket/mixed/year=*/month=*/*.parquet";

    /** Fixed calendar the named trees cover: September and October 2026. */
    public static final int TREE_YEAR = 2026;
    public static final LocalDate TREE_START = LocalDate.of(TREE_YEAR, 9, 1);
    public static final LocalDate TREE_END = LocalDate.of(TREE_YEAR, 10, 31);
    /** Exclusive end of the "now" used by window tests: midnight 15 Oct 2026 UTC. */
    public static final Instant NOW = Instant.parse("2026-10-15T00:00:00Z");

    /** Empty settings: AUTO detection, which still runs Hive on {@code key=value} paths. */
    public static final Map<String, Object> HIVE_ON = Map.of();
    public static final PartitionSpec DAY_SPEC = PartitionSpec.parse("year(@timestamp), month(@timestamp), day(@timestamp)");
    public static final Set<String> HIVE_KEYS = Set.of("year", "month", "day");

    private final List<StorageEntry> allEntries;
    final List<String> listedPrefixes = Collections.synchronizedList(new ArrayList<>());
    final List<String> childListedPrefixes = Collections.synchronizedList(new ArrayList<>());
    final List<String> enumeratedFiles = Collections.synchronizedList(new ArrayList<>());
    final Map<String, Integer> keysPulled = Collections.synchronizedMap(new LinkedHashMap<>());
    final AtomicInteger listObjectPages = new AtomicInteger();
    final AtomicInteger listChildrenRequests = new AtomicInteger();
    boolean childrenUnsupported;

    public ListingWalkHarness(List<StorageEntry> allEntries) {
        List<StorageEntry> sorted = new ArrayList<>(allEntries);
        sorted.sort(Comparator.comparing(e -> e.path().toString()));
        this.allEntries = sorted;
    }

    public int requestCount() {
        return listObjectPages.get() + listChildrenRequests.get();
    }

    /** Sep–Oct 2026, unpadded {@code year}/{@code month}/{@code day}, {@link #FILES_PER_DAY} files per day. */
    public static List<StorageEntry> fieldShapedTree() {
        return calendarTree(LOGS_PREFIX);
    }

    /** Zero-padded {@code month=06}, {@code day=03} under 2026, plus a sibling month so equality is not the whole tree. */
    public static List<StorageEntry> paddedTree() {
        List<StorageEntry> entries = new ArrayList<>();
        addDay(entries, PADDED_PREFIX, TREE_YEAR, "06", "03");
        addDay(entries, PADDED_PREFIX, TREE_YEAR, "07", "01");
        return entries;
    }

    /** {@code month=6}, {@code month=06}, {@code month=006} — three spellings of integer 6. {@code 007} is 7. */
    public static List<StorageEntry> mixedTree() {
        List<StorageEntry> entries = new ArrayList<>();
        addMixedMonth(entries, "6");
        addMixedMonth(entries, "06");
        addMixedMonth(entries, "006");
        return entries;
    }

    private static void addMixedMonth(List<StorageEntry> entries, String month) {
        for (int n = 0; n < FILES_PER_DAY; n++) {
            entries.add(entry(MIXED_PREFIX + "/year=" + TREE_YEAR + "/month=" + month + "/f-" + n + ".parquet"));
        }
    }

    private static List<StorageEntry> calendarTree(String prefix) {
        List<StorageEntry> entries = new ArrayList<>();
        for (LocalDate day = TREE_START; day.isAfter(TREE_END) == false; day = day.plusDays(1)) {
            addDay(entries, prefix, day.getYear(), Integer.toString(day.getMonthValue()), Integer.toString(day.getDayOfMonth()));
        }
        return entries;
    }

    private static void addDay(List<StorageEntry> entries, String prefix, int year, String month, String day) {
        for (int n = 0; n < FILES_PER_DAY; n++) {
            entries.add(entry(prefix + "/year=" + year + "/month=" + month + "/day=" + day + "/f-" + n + ".parquet"));
        }
    }

    public static StorageEntry entry(String path) {
        return new StorageEntry(StoragePath.of(path), 100, Instant.EPOCH);
    }

    /**
     * Flat prefix listing, then {@link PartitionValueMatcher} and {@link PartitionSpec#overlaps}, then
     * {@link FileOrderConfig#apply}. Path bindings use the same first-wins fill as
     * {@link org.elasticsearch.xpack.esql.datasources.HivePartitionDetector}
     * ({@link PartitionValueMatcher#folderKey} / {@link PartitionValueMatcher#folderValue}).
     */
    public static Reference reference(
        List<StorageEntry> tree,
        String glob,
        @Nullable List<PartitionFilterHint> listingHints,
        @Nullable PartitionSpec spec,
        @Nullable List<PartitionFilterHint> temporalHints,
        @Nullable Map<String, Object> config
    ) {
        StoragePath storagePath = StoragePath.of(glob);
        StoragePath prefix = storagePath.patternPrefix();
        GlobMatcher matcher = new GlobMatcher(storagePath.globPart());
        ExclusionConfig.NameFilter nameFilter = ExclusionConfig.fromConfig(config).compile();
        FileOrderConfig fileOrder = FileOrderConfig.forListing(config);
        PartitionSpec overlapSpec = spec == null ? PartitionSpec.EMPTY : spec;
        List<PartitionFilterHint> matcherHints = listingHints == null ? List.of() : listingHints;
        List<PartitionFilterHint> overlapHints = temporalHints == null ? List.of() : temporalHints;

        String prefixStr = prefix.toString();
        String p = prefixStr.endsWith("/") ? prefixStr : prefixStr + "/";
        List<StorageEntry> inKeyOrder = new ArrayList<>(tree);
        inKeyOrder.sort(Comparator.comparing(e -> e.path().toString()));
        List<StorageEntry> kept = new ArrayList<>();
        int excludedByNameFilter = 0;
        int excludedByPartition = 0;
        int excludedByMatcher = 0;
        List<String> notices = new ArrayList<>();
        for (StorageEntry entry : inKeyOrder) {
            String path = entry.path().toString();
            if (path.startsWith(p) == false) {
                continue;
            }
            String relative = path.substring(p.length());
            if (relative.isEmpty() || relative.endsWith("/") || matcher.matches(relative) == false) {
                continue;
            }
            String excludedBy = nameFilter.excludedBy(relative);
            if (excludedBy != null) {
                excludedByNameFilter++;
                continue;
            }
            Map<String, String> bindings = pathBindings(entry.path());
            boolean matcherDrop = keepsIsolated(bindings, matcherHints) == false;
            boolean overlapDrop = overlapSpec.overlaps(asObjects(bindings), overlapHints) == false;
            if (matcherDrop || overlapDrop) {
                excludedByPartition++;
                if (matcherDrop) {
                    excludedByMatcher++;
                }
                continue;
            }
            kept.add(entry);
        }
        fileOrder.apply(kept);
        // Production promotes an inference anchor from the value filter, not from an overlaps-only empty keep.
        boolean inferenceAnchor = kept.isEmpty() && excludedByMatcher > 0;
        PartitionMetadata metadata = GlobExpander.detectPartitions(kept, PartitionConfig.fromConfig(config), notices::add);
        Set<String> columns = metadata == null ? Set.of() : metadata.partitionColumns().keySet();
        return new Reference(pathsOf(kept), columns, excludedByNameFilter, excludedByPartition, notices, inferenceAnchor);
    }

    /** Expand {@code glob} under AUTO/Hive detection. */
    public static FileList expand(
        ListingWalkHarness provider,
        String glob,
        @Nullable List<PartitionFilterHint> hints,
        int maxDiscoveredFiles
    ) throws IOException {
        return expand(provider, glob, hints, maxDiscoveredFiles, HIVE_ON);
    }

    public static FileList expand(
        ListingWalkHarness provider,
        String glob,
        @Nullable List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        @Nullable Map<String, Object> config
    ) throws IOException {
        return expand(provider, glob, hints, maxDiscoveredFiles, config, null);
    }

    public static FileList expand(
        ListingWalkHarness provider,
        String glob,
        @Nullable List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        @Nullable FolderRangePredicate folderRange
    ) throws IOException {
        return expand(provider, glob, hints, maxDiscoveredFiles, HIVE_ON, folderRange);
    }

    public static FileList expand(
        ListingWalkHarness provider,
        String glob,
        @Nullable List<PartitionFilterHint> hints,
        int maxDiscoveredFiles,
        @Nullable Map<String, Object> config,
        @Nullable FolderRangePredicate folderRange
    ) throws IOException {
        return GlobExpander.expand(glob, provider, hints, config, maxDiscoveredFiles, Integer.MAX_VALUE, folderRange);
    }

    /** TEMPORAL extract as a folder-range predicate. Null when the spec is empty or has no usable bound. */
    public static FolderRangePredicate folderRange(List<Expression> filters, PartitionSpec spec) {
        if (spec == null || spec.isEmpty()) {
            return null;
        }
        return spec.folderRangePredicate(temporalHints(filters, spec));
    }

    /**
     * Named-tree listing hints: hive keys {@code year}/{@code month}/{@code day} plus spec-projected grain INs.
     * {@code @timestamp} is dropped. Does not keep {@code _file.*} or identity-remapped keys;
     * {@code FileSplitProvider.listingHintsForQuery} does.
     */
    public static List<PartitionFilterHint> listingHints(List<Expression> filters, PartitionSpec spec) {
        List<PartitionFilterHint> hints = PartitionFilterHintExtractor.fromConjuncts(filters, Set.of(), HIVE_KEYS);
        if (spec == null || spec.isEmpty()) {
            return hints;
        }
        List<PartitionFilterHint> merged = new ArrayList<>(hints);
        merged.addAll(
            PartitionFilterHintExtractor.fromConjuncts(filters, Set.of(), spec.boundColumns(), PartitionFilterHintExtractor.TEMPORAL)
        );
        List<PartitionFilterHint> kept = new ArrayList<>();
        for (PartitionFilterHint hint : spec.projectListingHints(merged)) {
            if (HIVE_KEYS.contains(hint.columnName())) {
                kept.add(hint);
            }
        }
        return kept;
    }

    /** TEMPORAL extract used by {@link PartitionSpec#overlaps}; includes {@code @timestamp}. */
    public static List<PartitionFilterHint> temporalHints(List<Expression> filters, PartitionSpec spec) {
        if (spec == null || spec.isEmpty()) {
            return List.of();
        }
        return PartitionFilterHintExtractor.fromConjuncts(filters, Set.of(), spec.boundColumns(), PartitionFilterHintExtractor.TEMPORAL);
    }

    /** Paths in listing order. */
    public static List<String> pathsOf(FileList list) {
        List<String> paths = new ArrayList<>(list.fileCount());
        for (int i = 0; i < list.fileCount(); i++) {
            paths.add(list.path(i).toString());
        }
        return paths;
    }

    static List<String> pathsOf(List<StorageEntry> entries) {
        List<String> paths = new ArrayList<>(entries.size());
        for (StorageEntry entry : entries) {
            paths.add(entry.path().toString());
        }
        return paths;
    }

    /**
     * First-wins {@code key=value} bindings of directory segments. Same skip-if-already-bound fill as
     * {@code HivePartitionDetector.fillPartitions}; keys use the walk grammar ({@link PartitionValueMatcher#folderKey}),
     * including reserved-name surfacing.
     */
    public static Map<String, String> pathBindings(StoragePath storagePath) {
        return PartitionValueMatcher.pathBindings(storagePath.path());
    }

    private static boolean keepsIsolated(Map<String, String> bindings, List<PartitionFilterHint> hints) {
        if (hints.isEmpty()) {
            return true;
        }
        for (Map.Entry<String, String> binding : bindings.entrySet()) {
            List<PartitionFilterHint> forKey = PartitionValueMatcher.hintsFor(binding.getKey(), hints);
            if (forKey.isEmpty() == false && PartitionValueMatcher.keepsIsolated(binding.getValue(), forKey) == false) {
                return false;
            }
        }
        return true;
    }

    private static Map<String, Object> asObjects(Map<String, String> bindings) {
        return new HashMap<>(bindings);
    }

    private static String withTrailingSlash(String prefix) {
        return prefix.endsWith("/") ? prefix : prefix + "/";
    }

    @Override
    public boolean listsInKeyOrder() {
        return true;
    }

    /** Prefix listing is always recursive, like S3; {@code recursive} is ignored. */
    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
        String p = withTrailingSlash(prefix.toString());
        listedPrefixes.add(p);
        List<StorageEntry> under = new ArrayList<>();
        for (StorageEntry e : allEntries) {
            if (e.path().toString().startsWith(p)) {
                under.add(e);
            }
        }
        return new StorageIterator() {
            int cursor;
            int fetched;
            boolean started;

            @Override
            public boolean hasNext() {
                if (started == false) {
                    listObjectPages.incrementAndGet();
                    fetched = Math.min(under.size(), LIST_PAGE_SIZE);
                    started = true;
                }
                if (cursor < fetched) {
                    return true;
                }
                if (cursor >= under.size()) {
                    return false;
                }
                listObjectPages.incrementAndGet();
                fetched = Math.min(under.size(), cursor + LIST_PAGE_SIZE);
                return cursor < fetched;
            }

            @Override
            public StorageEntry next() {
                if (hasNext() == false) {
                    throw new NoSuchElementException();
                }
                StorageEntry e = under.get(cursor++);
                enumeratedFiles.add(e.path().toString());
                keysPulled.merge(p, 1, Integer::sum);
                return e;
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public StorageChildren listChildren(StoragePath prefix, int limit) {
        if (childrenUnsupported) {
            return null;
        }
        String p = withTrailingSlash(prefix.toString());
        childListedPrefixes.add(p);
        listChildrenRequests.incrementAndGet();
        List<StorageEntry> files = new ArrayList<>();
        Set<String> dirs = new LinkedHashSet<>();
        for (StorageEntry e : allEntries) {
            String path = e.path().toString();
            if (path.startsWith(p) == false) {
                continue;
            }
            String rest = path.substring(p.length());
            int slash = rest.indexOf('/');
            if (slash < 0) {
                files.add(e);
                enumeratedFiles.add(path);
            } else {
                dirs.add(p + rest.substring(0, slash));
            }
            if (files.size() + dirs.size() > limit) {
                return null;
            }
        }
        List<StoragePath> dirPaths = new ArrayList<>(dirs.size());
        for (String dir : dirs) {
            dirPaths.add(StoragePath.of(dir));
        }
        return new StorageChildren(files, dirPaths);
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        return new StubStorageObject(path, 0, exists(path));
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return new StubStorageObject(path, length, true);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        return new StubStorageObject(path, length, true);
    }

    @Override
    public boolean exists(StoragePath path) {
        String want = path.toString();
        for (StorageEntry e : allEntries) {
            if (e.path().toString().equals(want)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("s3");
    }

    @Override
    public void close() {}

    /** Files the reference keeps, plus the metadata expand must match. */
    public record Reference(
        List<String> paths,
        Set<String> partitionColumns,
        int excludedByNameFilter,
        int excludedByPartition,
        List<String> notices,
        boolean inferenceAnchor
    ) {}

    private static final class StubStorageObject extends AbstractTestStorageObject {
        private final StoragePath path;
        private final long length;
        private final boolean exists;

        StubStorageObject(StoragePath path, long length, boolean exists) {
            this.path = path;
            this.length = length;
            this.exists = exists;
        }

        @Override
        public InputStream newStream() {
            return InputStream.nullInputStream();
        }

        @Override
        public InputStream newStream(long position, long length) {
            return InputStream.nullInputStream();
        }

        @Override
        public long length() {
            return this.length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return exists;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }
}
