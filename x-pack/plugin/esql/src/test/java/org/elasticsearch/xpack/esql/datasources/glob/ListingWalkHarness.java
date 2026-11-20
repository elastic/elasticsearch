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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * Counting in-memory store for listing-walk tests. {@link #listObjects} pages at {@link #LIST_PAGE_SIZE}
 * (one request per page). {@link #listChildren} is one request. Set {@link #childrenUnsupported} to force
 * the flat {@code listObjects} path used as the reference.
 *
 * <p>Replay a sorted key dump with {@link #loadKeyDump} / {@link #loadKeyDumpResource}, or synthesize the
 * 154-prefix AWS VPC Hive tree with {@link #wideHiveTree}. Optional {@link #setListChildrenDelayNanos} /
 * {@link #setListObjectPageDelayNanos} inject per-call latency for parallel-walk ordering tests.
 *
 * <p>The reference is a flat prefix listing plus {@link PartitionValueMatcher},
 * {@link PartitionSpec#overlaps}, {@link GlobExpander#applyFileMetadataFilters}, then
 * {@link FileOrderConfig#apply}. Listing must return the same files (and partition columns, excluded
 * count, notices, inference-anchor flag) as that filter; a strategy change may only change cost.
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

    /**
     * AWS VPC Hive layout at 154 account+region prefixes: 131 accounts, 20 region names (sparse, not the
     * 131×20 cartesian product). Enough leaves to count LIST pages, not tens of millions of keys.
     */
    public static final int WIDE_HIVE_ACCOUNTS = 131;
    public static final int WIDE_HIVE_PREFIXES = 154;
    public static final int WIDE_HIVE_REGION_NAMES = 20;
    public static final int WIDE_HIVE_FILES_PER_LEAF = 2;
    public static final int WIDE_HIVE_HOURS = 2;
    public static final String WIDE_HIVE_PREFIX = "s3://bucket/AWSLogs";
    public static final String WIDE_HIVE_GLOB = WIDE_HIVE_PREFIX
        + "/aws-account-id=*/aws-service=vpcflowlogs/aws-region=*/year=*/month=*/day=*/hour=*/*.parquet";
    /** Matching day of the synthetic tree: 1 June 2026 UTC. */
    public static final Instant WIDE_HIVE_DAY_START = Instant.parse("2026-06-01T00:00:00Z");
    public static final Instant WIDE_HIVE_DAY_END = Instant.parse("2026-06-02T00:00:00Z");
    /**
     * Kept files for a time-only 1 June filter: {@code prefixes × hours × filesPerLeaf}. Sibling 2 June and
     * 1 July folders exist so the walk has something to prune.
     */
    public static final int WIDE_HIVE_ONE_DAY_FILES = WIDE_HIVE_PREFIXES * WIDE_HIVE_HOURS * WIDE_HIVE_FILES_PER_LEAF;
    /**
     * {@code listChildren} (including peeks) to reach matching day directories on {@link #wideHiveTree()}
     * with a 1-day filter: root + accounts + services + regions + years + matching months + July peeks.
     * Gates the walk directory-listing ceiling default. Finish {@code listObjects} are
     * {@link #WIDE_HIVE_ONE_DAY_FINISH_PAGES} extra and are not counted by {@code MAX_DIRECTORY_LISTINGS}.
     * On this sparse tree RULE still flattens after a complete walk ({@code k=4}); a dense production
     * prefix makes finish cheaper.
     */
    public static final int WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES = 1 + 2 * WIDE_HIVE_ACCOUNTS + 4 * WIDE_HIVE_PREFIXES;
    /** One recursive {@code listObjects} per matching day directory if finish wins. */
    public static final int WIDE_HIVE_ONE_DAY_FINISH_PAGES = WIDE_HIVE_PREFIXES;
    public static final int WIDE_HIVE_ONE_DAY_COMPLETE_WALK = WIDE_HIVE_ONE_DAY_WALK_DIRECTORIES + WIDE_HIVE_ONE_DAY_FINISH_PAGES;

    private static final String[] WIDE_HIVE_REGIONS = {
        "us-east-1",
        "us-east-2",
        "us-west-1",
        "us-west-2",
        "eu-west-1",
        "eu-west-2",
        "eu-central-1",
        "eu-north-1",
        "ap-southeast-1",
        "ap-southeast-2",
        "ap-northeast-1",
        "ap-northeast-2",
        "ap-south-1",
        "sa-east-1",
        "ca-central-1",
        "af-south-1",
        "me-south-1",
        "eu-west-3",
        "ap-east-1",
        "il-central-1" };

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
    private volatile LongSupplier listChildrenDelayNanos = () -> 0L;
    private volatile LongSupplier listObjectPageDelayNanos = () -> 0L;

    public ListingWalkHarness(List<StorageEntry> allEntries) {
        List<StorageEntry> sorted = new ArrayList<>(allEntries);
        sorted.sort(Comparator.comparing(e -> e.path().toString()));
        this.allEntries = List.copyOf(sorted);
    }

    public int requestCount() {
        return listObjectPages.get() + listChildrenRequests.get();
    }

    public int listObjectPages() {
        return listObjectPages.get();
    }

    public int listChildrenRequests() {
        return listChildrenRequests.get();
    }

    /** Snapshot of recursive {@code listObjects} prefixes, in call order. */
    public List<String> listedPrefixes() {
        synchronized (listedPrefixes) {
            return List.copyOf(listedPrefixes);
        }
    }

    /** Snapshot of {@code listChildren} prefixes, in call order. */
    public List<String> childListedPrefixes() {
        synchronized (childListedPrefixes) {
            return List.copyOf(childListedPrefixes);
        }
    }

    public List<StorageEntry> entries() {
        return allEntries;
    }

    public void setChildrenUnsupported(boolean childrenUnsupported) {
        this.childrenUnsupported = childrenUnsupported;
    }

    /**
     * Called once per {@link #listChildren} before the directory is enumerated. Return {@code 0} for no
     * pause. Parallel-walk ordering tests supply a per-call latency; the default is a no-op.
     */
    public void setListChildrenDelayNanos(LongSupplier delayNanos) {
        this.listChildrenDelayNanos = delayNanos == null ? () -> 0L : delayNanos;
    }

    /**
     * Called once per {@link #listObjects} page before keys of that page are yielded. Return {@code 0}
     * for no pause.
     */
    public void setListObjectPageDelayNanos(LongSupplier delayNanos) {
        this.listObjectPageDelayNanos = delayNanos == null ? () -> 0L : delayNanos;
    }

    /** LIST pages a recursive {@code listObjects} of {@code objects} keys costs. An empty listing is still one page. */
    public static int objectPages(int objects) {
        if (objects <= 0) {
            return 1;
        }
        return Math.ceilDiv(objects, LIST_PAGE_SIZE);
    }

    /** Sep–Oct 2026, unpadded {@code year}/{@code month}/{@code day}, {@link #FILES_PER_DAY} files per day. */
    public static List<StorageEntry> fieldShapedTree() {
        return calendarTree(LOGS_PREFIX, FILES_PER_DAY);
    }

    /** Same calendar as {@link #fieldShapedTree()} with {@code filesPerDay} objects under each day folder. */
    public static List<StorageEntry> fieldShapedTree(int filesPerDay) {
        return calendarTree(LOGS_PREFIX, filesPerDay);
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

    /**
     * 154-prefix Hive VPC tree: padded {@code month}/{@code day}/{@code hour}, 1 June (match), 2 June and 1 July
     * (prune siblings), {@link #WIDE_HIVE_HOURS} hours × {@link #WIDE_HIVE_FILES_PER_LEAF} files per leaf.
     */
    public static List<StorageEntry> wideHiveTree() {
        assert WIDE_HIVE_REGIONS.length == WIDE_HIVE_REGION_NAMES : WIDE_HIVE_REGIONS.length;
        int extraDualRegionAccounts = WIDE_HIVE_PREFIXES - WIDE_HIVE_ACCOUNTS;
        List<StorageEntry> entries = new ArrayList<>();
        int prefixes = 0;
        for (int a = 1; a <= WIDE_HIVE_ACCOUNTS; a++) {
            String account = String.format(Locale.ROOT, "%012d", a);
            String firstRegion = WIDE_HIVE_REGIONS[(a - 1) % WIDE_HIVE_REGIONS.length];
            addWideHivePrefix(entries, account, firstRegion);
            prefixes++;
            if (a <= extraDualRegionAccounts) {
                String secondRegion = WIDE_HIVE_REGIONS[a % WIDE_HIVE_REGIONS.length];
                addWideHivePrefix(entries, account, secondRegion);
                prefixes++;
            }
        }
        assert prefixes == WIDE_HIVE_PREFIXES : prefixes;
        return entries;
    }

    private static void addWideHivePrefix(List<StorageEntry> entries, String account, String region) {
        String prefix = WIDE_HIVE_PREFIX + "/aws-account-id=" + account + "/aws-service=vpcflowlogs/aws-region=" + region;
        addWideHiveDay(entries, prefix, 2026, 6, 1);
        addWideHiveDay(entries, prefix, 2026, 6, 2);
        addWideHiveDay(entries, prefix, 2026, 7, 1);
    }

    private static void addWideHiveDay(List<StorageEntry> entries, String prefix, int year, int month, int day) {
        for (int hour = 0; hour < WIDE_HIVE_HOURS; hour++) {
            for (int n = 0; n < WIDE_HIVE_FILES_PER_LEAF; n++) {
                entries.add(
                    entry(
                        prefix
                            + "/year="
                            + year
                            + "/month="
                            + pad2(month)
                            + "/day="
                            + pad2(day)
                            + "/hour="
                            + pad2(hour)
                            + "/f-"
                            + n
                            + ".parquet"
                    )
                );
            }
        }
    }

    private static String pad2(int n) {
        return String.format(Locale.ROOT, "%02d", n);
    }

    /**
     * Load a key dump: one object URI per line. Blank lines and {@code #} comments are skipped. Optional
     * tab-separated {@code path}, {@code length}, {@code lastModified} (ISO-8601 or epoch millis). First
     * occurrence of a path wins. The harness sorts on construct.
     */
    public static List<StorageEntry> loadKeyDump(InputStream in) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return loadKeyDump(reader);
        }
    }

    /** Load a classpath resource next to this class, for example {@code key-dump-sample.keys}. */
    public static List<StorageEntry> loadKeyDumpResource(String name) throws IOException {
        InputStream in = ListingWalkHarness.class.getResourceAsStream(name);
        if (in == null) {
            throw new IllegalArgumentException("missing key dump resource [" + name + "]");
        }
        try (in) {
            return loadKeyDump(in);
        }
    }

    public static List<StorageEntry> loadKeyDump(BufferedReader reader) throws IOException {
        Map<String, StorageEntry> unique = new LinkedHashMap<>();
        int lineNo = 0;
        String line;
        while ((line = reader.readLine()) != null) {
            lineNo++;
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.charAt(0) == '#') {
                continue;
            }
            try {
                StorageEntry parsed = parseDumpLine(trimmed);
                unique.putIfAbsent(parsed.path().toString(), parsed);
            } catch (RuntimeException e) {
                throw new IllegalArgumentException("key dump line " + lineNo + ": " + e.getMessage(), e);
            }
        }
        return new ArrayList<>(unique.values());
    }

    private static StorageEntry parseDumpLine(String line) {
        String path = line;
        long length = 100L;
        Instant lastModified = Instant.EPOCH;
        int tab = line.indexOf('\t');
        if (tab >= 0) {
            path = line.substring(0, tab);
            String rest = line.substring(tab + 1);
            int tab2 = rest.indexOf('\t');
            String lengthPart = tab2 < 0 ? rest : rest.substring(0, tab2);
            if (lengthPart.isBlank() == false) {
                length = Long.parseLong(lengthPart.trim());
            }
            if (tab2 >= 0) {
                String timePart = rest.substring(tab2 + 1);
                int extra = timePart.indexOf('\t');
                if (extra >= 0) {
                    timePart = timePart.substring(0, extra);
                }
                timePart = timePart.trim();
                if (timePart.isEmpty() == false) {
                    lastModified = parseDumpTime(timePart);
                }
            }
        }
        path = path.trim();
        if (path.isEmpty()) {
            throw new IllegalArgumentException("empty path");
        }
        return new StorageEntry(StoragePath.of(path), length, lastModified);
    }

    private static Instant parseDumpTime(String timePart) {
        if (isEpochMillisToken(timePart)) {
            return Instant.ofEpochMilli(Long.parseLong(timePart));
        }
        return Instant.parse(timePart);
    }

    /** Signed integer, including negative pre-1970 millis. ISO-8601 instants contain {@code T} or extra dashes. */
    private static boolean isEpochMillisToken(String timePart) {
        if (timePart.isEmpty()) {
            return false;
        }
        int i = 0;
        char first = timePart.charAt(0);
        if (first == '-' || first == '+') {
            if (timePart.length() == 1) {
                return false;
            }
            i = 1;
        }
        for (; i < timePart.length(); i++) {
            char c = timePart.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
        }
        return true;
    }

    private static List<StorageEntry> calendarTree(String prefix, int filesPerDay) {
        List<StorageEntry> entries = new ArrayList<>();
        for (LocalDate day = TREE_START; day.isAfter(TREE_END) == false; day = day.plusDays(1)) {
            addDay(
                entries,
                prefix,
                day.getYear(),
                Integer.toString(day.getMonthValue()),
                Integer.toString(day.getDayOfMonth()),
                filesPerDay
            );
        }
        return entries;
    }

    private static void addDay(List<StorageEntry> entries, String prefix, int year, String month, String day) {
        addDay(entries, prefix, year, month, day, FILES_PER_DAY);
    }

    private static void addDay(List<StorageEntry> entries, String prefix, int year, String month, String day, int filesPerDay) {
        for (int n = 0; n < filesPerDay; n++) {
            entries.add(entry(prefix + "/year=" + year + "/month=" + month + "/day=" + day + "/f-" + n + ".parquet"));
        }
    }

    public static StorageEntry entry(String path) {
        return new StorageEntry(StoragePath.of(path), 100, Instant.EPOCH);
    }

    /**
     * Flat prefix listing, then {@link PartitionValueMatcher} and {@link PartitionSpec#overlaps}, then
     * {@link GlobExpander#applyFileMetadataFilters}, then {@link FileOrderConfig#apply}. Path bindings use
     * the same first-wins fill as {@link org.elasticsearch.xpack.esql.datasources.HivePartitionDetector}
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
        List<StorageEntry> afterPartition = kept;
        kept = new ArrayList<>(GlobExpander.applyFileMetadataFilters(afterPartition, matcherHints));
        int excludedByFileHint = afterPartition.size() - kept.size();
        // Partition-value all-miss is an inference anchor. An all-filtered _file.* hint stashes the first
        // listing-order reject as a schema file and does not set the anchor flag.
        boolean inferenceAnchor = afterPartition.isEmpty() && excludedByPartition > 0;
        if (kept.isEmpty() && excludedByFileHint > 0) {
            kept.add(afterPartition.get(0));
        }
        fileOrder.apply(kept);
        PartitionMetadata metadata = GlobExpander.detectPartitions(kept, PartitionConfig.fromConfig(config), notices::add);
        Set<String> columns = metadata == null ? Set.of() : metadata.partitionColumns().keySet();
        return new Reference(
            pathsOf(kept),
            columns,
            excludedByNameFilter,
            excludedByPartition,
            excludedByFileHint,
            notices,
            inferenceAnchor
        );
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

    private static void delay(LongSupplier nanos) {
        long n = nanos.getAsLong();
        if (n <= 0L) {
            return;
        }
        long deadline = System.nanoTime() + n;
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0L) {
                return;
            }
            LockSupport.parkNanos(remaining);
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                return;
            }
        }
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
                    delay(listObjectPageDelayNanos);
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
                delay(listObjectPageDelayNanos);
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
        delay(listChildrenDelayNanos);
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
        int excludedByFileHint,
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
