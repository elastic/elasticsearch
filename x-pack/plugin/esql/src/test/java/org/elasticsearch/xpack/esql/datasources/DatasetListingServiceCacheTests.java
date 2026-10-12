/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
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
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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

    /** Lists a flat entry set as a directory tree, so the partition walk has children to descend. */
    private static final class TreeProvider implements StorageProvider {
        private final List<StorageEntry> entries;
        final AtomicLong listObjectsCalls = new AtomicLong();

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
