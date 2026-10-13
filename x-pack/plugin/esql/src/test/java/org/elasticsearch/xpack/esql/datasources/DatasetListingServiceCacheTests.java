/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.ExternalSourceCacheService;
import org.elasticsearch.xpack.esql.datasources.glob.ListingExtents;
import org.elasticsearch.xpack.esql.datasources.glob.PlanningMemory;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;

/**
 * What the two rails are allowed to hand each other through the shared listing cache.
 *
 * <p>The cache holds one entry per set of listing inputs, and the rails reach it differently: a query that wants
 * the whole pattern takes an entry without comparing anything, while a bounded one — the schema rail — checks that
 * the entry answers its own bound. That asymmetry is only safe while both rails would list the same files, and for
 * one shape they would not. A bounded drain applies a partition-value filter itself; an unbounded one first tries
 * the partition walk, and where the walk withdraws it re-lists with that filter suppressed, which is the unfiltered
 * superset. The walk types a partition column from the folder names it enumerated, the drain from the files it
 * matched, so a {@code key=value} folder holding no matching file splits the two.
 *
 * <p>The fixture is that split: {@code year=2025} holds a {@code _SUCCESS}, not a {@code .csv}, so it widens the
 * column to text for the walk while the drain never sees it. The walk withdraws, the unbounded listing is both
 * {@code .csv} files, and a bounded query's own listing is the one its filter selects.
 */
public class DatasetListingServiceCacheTests extends ESTestCase {

    private static final String GLOB = "s3://bucket/logs/year=*/month=*/*.csv";
    private static final String OLD = "s3://bucket/logs/year=2023/month=01/old.csv";
    private static final String KEPT = "s3://bucket/logs/year=2024/month=01/a.csv";

    /** The spelling-miss case: a leading {@code *} segment, so neither the original nor the spliced glob walks. */
    private static final String SPELLING_GLOB = "s3://bucket/logs/*/year=*/month=*/*.csv";
    private static final String SPELLING_DROPPED = "s3://bucket/logs/x/year=2023/month=6/o.csv";
    private static final String SPELLING_ALT = "s3://bucket/logs/x/year=2024/month=06/s.csv";
    private static final String SPELLING_KEPT = "s3://bucket/logs/x/year=2024/month=6/k.csv";

    /** The glob of the retry case: a second segment the splice leaves as {@code *}, so the narrowed pass cannot walk. */
    private static final String RETRY_GLOB = "s3://bucket/logs/year=*/*/*.csv";
    private static final String RETRY_OUT_OF_RANGE = "s3://bucket/logs/year=2024/month=01/a.csv";
    private static final String RETRY_OTHER_YEAR = "s3://bucket/logs/year=2023/month=06/o.csv";
    private static final String RETRY_IN_RANGE = "s3://bucket/logs/year=2024/month=06/k.csv";

    private static Settings cacheEnabled() {
        return Settings.builder()
            .put("esql.external.cache.size", "10mb")
            .put("esql.external.cache.enabled", true)
            .put("esql.external.cache.listing.ttl", "30s")
            .build();
    }

    private static TreeProvider fixture() {
        return new TreeProvider(
            List.of(
                new StorageEntry(StoragePath.of(OLD), 10, Instant.EPOCH),
                new StorageEntry(StoragePath.of(KEPT), 10, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://bucket/logs/year=2025/month=abc/_SUCCESS"), 0, Instant.EPOCH)
            )
        );
    }

    /** The hints that make the walk run and then withdraw; {@code status} is data, and keeps it descending. */
    private static List<PartitionFilterHintExtractor.PartitionFilterHint> hints() {
        return List.of(
            new PartitionFilterHintExtractor.PartitionFilterHint("year", PartitionFilterHintExtractor.Operator.IN, List.of(2024, 2025)),
            new PartitionFilterHintExtractor.PartitionFilterHint("status", PartitionFilterHintExtractor.Operator.EQUALS, List.of("ok"))
        );
    }

    private static List<String> paths(FileList list) {
        List<String> out = new ArrayList<>(list.fileCount());
        for (int i = 0; i < list.fileCount(); i++) {
            out.add(list.path(i).toString());
        }
        out.sort(Comparator.naturalOrder());
        return out;
    }

    private FileList bounded(DatasetListingService service, StorageProvider provider) throws Exception {
        return service.boundedListing(
            GLOB,
            StoragePath.of(GLOB),
            provider,
            "",
            "",
            hints(),
            Map.of(),
            new ListingExtents(1000),
            PlanningMemory.NONE,
            () -> false
        );
    }

    private FileList unbounded(DatasetListingService service, StorageProvider provider) throws Exception {
        return service.cachedListing(GLOB, StoragePath.of(GLOB), provider, "", "", hints(), Map.of(), PlanningMemory.NONE, () -> false);
    }

    /**
     * A bounded listing must not be published where the unbounded rail would have walked — the case that failed.
     *
     * <p>Both rails address one key, so without the refusal the bounded query's narrower listing becomes the entry
     * every later query reads, and a query that needs the whole pattern is handed a file set that is not its own.
     * Rows would not change: the {@code WHERE} stays in the plan. The schema would — under {@code first_file_wins}
     * it comes from the first path, so it would be read from a different file depending on which query ran first.
     */
    public void testABoundedListingIsNotPublishedWhereTheUnboundedRailWouldWalk() throws Exception {
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(cacheEnabled())) {
            DatasetListingService service = new DatasetListingService(Settings.EMPTY, cache, null, null, null);
            TreeProvider provider = fixture();

            FileList boundedFirst = bounded(service, provider);
            assertEquals("the bounded drain applies the filter itself", List.of(KEPT), paths(boundedFirst));

            long listsAfterBounded = provider.listObjectsCalls.get();
            FileList whole = unbounded(service, provider);

            assertEquals("a query that wants the whole pattern gets its own file set", List.of(OLD, KEPT), paths(whole));
            assertTrue(
                "which means it listed for itself rather than reading the bounded entry",
                provider.listObjectsCalls.get() > listsAfterBounded
            );
        }
    }

    /** And the bounded listing must not overwrite an entry the unbounded rail already published. */
    public void testABoundedListingDoesNotOverwriteTheWholePattern() throws Exception {
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(cacheEnabled())) {
            DatasetListingService service = new DatasetListingService(Settings.EMPTY, cache, null, null, null);
            TreeProvider provider = fixture();

            assertEquals(List.of(OLD, KEPT), paths(unbounded(service, provider)));
            assertEquals(List.of(KEPT), paths(bounded(service, provider)));

            long listsBefore = provider.listObjectsCalls.get();
            assertEquals(
                "the same query, run twice, must not change its answer because a bounded one ran between",
                List.of(OLD, KEPT),
                paths(unbounded(service, provider))
            );
            assertEquals("and it is still served from the cache", listsBefore, provider.listObjectsCalls.get());
        }
    }

    /**
     * The retry on the original pattern must not be published either — the second route to the same superset.
     *
     * <p>Here the splice leaves a glob the walk cannot use, so no prediction about the unbounded rail refuses this
     * listing. The divergence is in what this rail itself produced: the narrowed pass spends its whole bound on
     * {@code month=01}, a glob match the closed range then drops, so it returns empty-and-truncated and is retried
     * unbounded on the <em>original</em> pattern. That pattern does walk, the walk withdraws on {@code year=abc},
     * and the re-list suppresses the year filter — so the bounded rail ends up holding the wider set, and the
     * unbounded rail, whose own narrowed pass finds the in-range file and never retries, holds the narrower one.
     *
     * <p>The bound is 1 so the fixture stays readable; at the default of 1000 the same thing needs 1000 dropped
     * matches listed ahead of the first surviving one.
     */
    public void testTheRetryOnTheOriginalPatternIsNotPublished() throws Exception {
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(cacheEnabled())) {
            DatasetListingService service = new DatasetListingService(Settings.EMPTY, cache, null, null, null);
            TreeProvider provider = retryFixture();

            FileList boundedFirst = service.boundedListing(
                RETRY_GLOB,
                StoragePath.of(RETRY_GLOB),
                provider,
                "",
                "",
                retryHints(),
                Map.of(),
                new ListingExtents(1),
                PlanningMemory.NONE,
                () -> false
            );
            assertEquals(
                "the retry re-listed the original pattern with the year filter suppressed",
                List.of(RETRY_OTHER_YEAR, RETRY_IN_RANGE),
                paths(boundedFirst)
            );

            long listsAfterBounded = provider.listObjectsCalls.get();
            FileList whole = service.cachedListing(
                RETRY_GLOB,
                StoragePath.of(RETRY_GLOB),
                provider,
                "",
                "",
                retryHints(),
                Map.of(),
                PlanningMemory.NONE,
                () -> false
            );

            assertEquals("a query that wants the whole pattern gets its own file set", List.of(RETRY_IN_RANGE), paths(whole));
            assertTrue(
                "which means it listed for itself rather than reading the retry's superset",
                provider.listObjectsCalls.get() > listsAfterBounded
            );
        }
    }

    /**
     * The same refusal on the async rail, which is the one split discovery actually uses.
     *
     * <p>Worth its own test rather than trusting the two to agree: the guard is a separate statement inside
     * {@code boundedListingAsync}'s completion lambda, and with only the synchronous tests above it could be
     * deleted without failing anything.
     */
    public void testTheAsyncRailRefusesToPublishToo() throws Exception {
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(cacheEnabled())) {
            DatasetListingService service = new DatasetListingService(Settings.EMPTY, cache, null, null, null);
            TreeProvider provider = fixture();

            PlainActionFuture<FileList> future = new PlainActionFuture<>();
            service.boundedListingAsync(
                GLOB,
                StoragePath.of(GLOB),
                provider,
                "",
                "",
                hints(),
                Map.of(),
                new ListingExtents(1000),
                PlanningMemory.NONE,
                () -> false,
                1,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                future
            );
            assertEquals("the bounded drain applies the filter itself", List.of(KEPT), paths(future.get()));

            long listsAfterBounded = provider.listObjectsCalls.get();
            FileList whole = unbounded(service, provider);

            assertEquals("a query that wants the whole pattern gets its own file set", List.of(OLD, KEPT), paths(whole));
            assertTrue(
                "which means it listed for itself rather than reading the bounded entry",
                provider.listObjectsCalls.get() > listsAfterBounded
            );
        }
    }

    /**
     * And the retry must not be published where nothing walked at all — the route that defeats both halves of a
     * rule written about the walk.
     *
     * <p>The splice is a literal folder name, so a drain on the spliced pattern asks the store only for
     * {@code month=6} and never lists {@code month=06}. The retry lists the original pattern, so it does, and
     * nothing takes it back out afterwards: the month equality is integral, which is the splice's job and not the
     * folder post-filter's, and this glob's first segment is {@code *}, so under this fixture's default strategy
     * it neither walks nor value-filters. Only the closed range on {@code year} is applied, and the extra file is
     * inside it. The retry therefore holds a file the spliced drain the unbounded rail runs cannot reach — and
     * because nothing withdrew, nothing suppressed a filter, so before this was fixed the listing carried a real
     * count and was published.
     *
     * <p>Worse than the walk routes, because the extra file's rows satisfy the {@code WHERE} — month 6 of 2024 —
     * so the rows the query returns change, not only the file a schema is taken from.
     */
    public void testTheSpellingMissRetryIsNotPublishedWhereNothingWalks() throws Exception {
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(cacheEnabled())) {
            DatasetListingService service = new DatasetListingService(Settings.EMPTY, cache, null, null, null);
            TreeProvider provider = spellingFixture();

            FileList boundedFirst = service.boundedListing(
                SPELLING_GLOB,
                StoragePath.of(SPELLING_GLOB),
                provider,
                "",
                "",
                spellingHints(),
                Map.of(),
                new ListingExtents(1),
                PlanningMemory.NONE,
                () -> false
            );
            assertEquals(
                "the retry lists the spelling the splice never asked for",
                List.of(SPELLING_ALT, SPELLING_KEPT),
                paths(boundedFirst)
            );

            long listsAfterBounded = provider.listObjectsCalls.get();
            FileList whole = service.cachedListing(
                SPELLING_GLOB,
                StoragePath.of(SPELLING_GLOB),
                provider,
                "",
                "",
                spellingHints(),
                Map.of(),
                PlanningMemory.NONE,
                () -> false
            );

            assertEquals("a query that wants the whole pattern gets its own file set", List.of(SPELLING_KEPT), paths(whole));
            assertTrue(
                "which means it listed for itself rather than reading the retry's wider set",
                provider.listObjectsCalls.get() > listsAfterBounded
            );
        }
    }

    /**
     * And the same refusal where the retry fans out, which is the rail a bounded listing reaches in production.
     *
     * <p>The retry is unbounded, so with concurrency to spare and a key-ordered store it drains the prefix's
     * folders in parallel instead of flat. That is a second recorder of the glob-match count, inside
     * {@code fanOutAsync} rather than the flat drain, and the flag saying the count is not comparable has to be
     * threaded to it. Nothing about the divergence changes: the narrowed pass is bounded, so it is still flat, and
     * the retry still lists the spelling the splice never asked for. The concurrency is what the sync tests cannot
     * reach -- at 1 the fan-out is ineligible, so with those tests alone the fan-out could record a real count
     * and publish the wider set with nothing failing. The prefixes listed are asserted too, so this test fails
     * rather than passing silently where the fan-out declines and the flat drain answers instead: that is the
     * rail the sync test covers, and it would leave this one green on a tree where the code under test never ran.
     */
    public void testTheSpellingMissRetryIsNotPublishedWhenTheRetryFansOut() throws Exception {
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(cacheEnabled())) {
            DatasetListingService service = new DatasetListingService(Settings.EMPTY, cache, null, null, null);
            TreeProvider provider = spellingFixture();

            PlainActionFuture<FileList> future = new PlainActionFuture<>();
            service.boundedListingAsync(
                SPELLING_GLOB,
                StoragePath.of(SPELLING_GLOB),
                provider,
                "",
                "",
                spellingHints(),
                Map.of(),
                new ListingExtents(1),
                PlanningMemory.NONE,
                () -> false,
                4,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                future
            );
            assertEquals(
                "the retry lists the spelling the splice never asked for",
                List.of(SPELLING_ALT, SPELLING_KEPT),
                paths(future.get())
            );
            // What was listed, not that slots were attempted: `buildSlots` lists children before it can decide
            // anything, so counting that counts the routes where it then declines and the flat drain runs -- the
            // drain the sync test above already covers. Only a folder slot addresses a prefix below the pattern's
            // own; both flat passes list `logs/` itself. Asserted that way rather than as a request count, which
            // would also have to change if slots were ever cut finer.
            assertThat("and the retry got there by fanning out", provider.listedPrefixes, hasItem(startsWith("s3://bucket/logs/x/")));

            long listsAfterBounded = provider.listObjectsCalls.get();
            FileList whole = service.cachedListing(
                SPELLING_GLOB,
                StoragePath.of(SPELLING_GLOB),
                provider,
                "",
                "",
                spellingHints(),
                Map.of(),
                PlanningMemory.NONE,
                () -> false
            );

            assertEquals("a query that wants the whole pattern gets its own file set", List.of(SPELLING_KEPT), paths(whole));
            assertTrue(
                "which means it listed for itself rather than reading the retry's wider set",
                provider.listObjectsCalls.get() > listsAfterBounded
            );
        }
    }

    /** The two spellings of month 6, plus the out-of-range file whose glob match spends the bound. */
    private static TreeProvider spellingFixture() {
        return new TreeProvider(
            List.of(
                new StorageEntry(StoragePath.of(SPELLING_DROPPED), 10, Instant.EPOCH),
                new StorageEntry(StoragePath.of(SPELLING_ALT), 10, Instant.EPOCH),
                new StorageEntry(StoragePath.of(SPELLING_KEPT), 10, Instant.EPOCH)
            )
        );
    }

    /**
     * {@code month} is an integral equality, so it splices into the glob; the closed range on {@code year} does not,
     * and is applied after the glob has already counted a file against the bound.
     */
    private static List<PartitionFilterHintExtractor.PartitionFilterHint> spellingHints() {
        return List.of(
            new PartitionFilterHintExtractor.PartitionFilterHint("month", PartitionFilterHintExtractor.Operator.EQUALS, List.of(6)),
            new PartitionFilterHintExtractor.PartitionFilterHint(
                "year",
                PartitionFilterHintExtractor.Operator.GREATER_THAN_OR_EQUAL,
                List.of(2024)
            ),
            new PartitionFilterHintExtractor.PartitionFilterHint(
                "year",
                PartitionFilterHintExtractor.Operator.LESS_THAN_OR_EQUAL,
                List.of(2024)
            )
        );
    }

    /** {@code year=abc} is what the walk cannot type, so it withdraws and re-lists with the year filter suppressed. */
    private static TreeProvider retryFixture() {
        return new TreeProvider(
            List.of(
                new StorageEntry(StoragePath.of(RETRY_OTHER_YEAR), 10, Instant.EPOCH),
                new StorageEntry(StoragePath.of(RETRY_OUT_OF_RANGE), 10, Instant.EPOCH),
                new StorageEntry(StoragePath.of(RETRY_IN_RANGE), 10, Instant.EPOCH),
                new StorageEntry(StoragePath.of("s3://bucket/logs/year=abc/_SUCCESS"), 0, Instant.EPOCH)
            )
        );
    }

    /**
     * {@code year} is spliced into the glob; the closed range on {@code month} is not, so it is applied as a filter
     * after the glob has already counted the file against the bound.
     */
    private static List<PartitionFilterHintExtractor.PartitionFilterHint> retryHints() {
        return List.of(
            new PartitionFilterHintExtractor.PartitionFilterHint("year", PartitionFilterHintExtractor.Operator.EQUALS, List.of(2024)),
            new PartitionFilterHintExtractor.PartitionFilterHint(
                "month",
                PartitionFilterHintExtractor.Operator.GREATER_THAN_OR_EQUAL,
                List.of(6)
            ),
            new PartitionFilterHintExtractor.PartitionFilterHint(
                "month",
                PartitionFilterHintExtractor.Operator.LESS_THAN_OR_EQUAL,
                List.of(7)
            )
        );
    }

    /** Lists a flat entry set as a directory tree, so the partition walk has children to descend. */
    private static final class TreeProvider implements StorageProvider {
        private final List<StorageEntry> entries;
        final AtomicLong listObjectsCalls = new AtomicLong();
        final List<String> listedPrefixes = Collections.synchronizedList(new ArrayList<>());

        TreeProvider(List<StorageEntry> entries) {
            List<StorageEntry> sorted = new ArrayList<>(entries);
            sorted.sort(Comparator.comparing(e -> e.path().toString()));
            this.entries = sorted;
        }

        private static String withSlash(String prefix) {
            return prefix.endsWith("/") ? prefix : prefix + "/";
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            listObjectsCalls.incrementAndGet();
            String p = withSlash(prefix.toString());
            listedPrefixes.add(p);
            List<StorageEntry> under = new ArrayList<>();
            for (StorageEntry e : entries) {
                if (e.path().toString().startsWith(p)) {
                    under.add(e);
                }
            }
            Iterator<StorageEntry> it = under.iterator();
            return new StorageIterator() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public StorageEntry next() {
                    return it.next();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            String p = withSlash(prefix.toString());
            List<StorageEntry> files = new ArrayList<>();
            Set<String> dirs = new LinkedHashSet<>();
            for (StorageEntry e : entries) {
                String path = e.path().toString();
                if (path.startsWith(p) == false) {
                    continue;
                }
                String rest = path.substring(p.length());
                int slash = rest.indexOf('/');
                if (slash < 0) {
                    files.add(e);
                } else {
                    dirs.add(p + rest.substring(0, slash));
                }
            }
            List<StoragePath> dirPaths = new ArrayList<>(dirs.size());
            for (String d : dirs) {
                dirPaths.add(StoragePath.of(d));
            }
            return new StorageChildren(files, dirPaths);
        }

        @Override
        public boolean listsInKeyOrder() {
            return true;
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean exists(StoragePath path) {
            return false;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() throws IOException {}
    }
}
