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
import org.elasticsearch.xpack.esql.datasources.glob.PlanningMemory;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The async listing entry points of {@link DatasetListingService}: what a query is charged for a listing, however the
 * listing reached it. The charge must not depend on who happened to list first, or the circuit breaker would trip on a
 * cold node and not on a warm one.
 */
public class DatasetListingServiceAsyncTests extends ESTestCase {

    private static final String GLOB = "s3://bucket/data/*/*.csv";

    private static Settings cacheEnabled() {
        return Settings.builder()
            .put("esql.external.cache.size", "10mb")
            .put("esql.external.cache.enabled", true)
            .put("esql.external.cache.listing.ttl", "30s")
            .build();
    }

    private static List<StorageEntry> twoFolders() {
        return List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/d1/a.csv"), 10, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/d2/b.csv"), 10, Instant.EPOCH)
        );
    }

    private static void list(
        DatasetListingService service,
        StorageProvider provider,
        PlanningMemory memory,
        java.util.concurrent.Executor executor,
        PlainActionFuture<FileList> future
    ) {
        service.cachedListingAsync(GLOB, StoragePath.of(GLOB), provider, "", "", null, Map.of(), memory, () -> false, 4, executor, future);
    }

    /** A listing computed here and the same listing served from the cache cost the query the same. */
    public void testCachedListingAsyncChargesTheSameForAComputeAndAHit() throws Exception {
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(cacheEnabled())) {
            DatasetListingService service = new DatasetListingService(Settings.EMPTY, cache, null, null, null);
            StubProvider provider = new StubProvider(twoFolders());
            AtomicLong computed = new AtomicLong();
            AtomicLong served = new AtomicLong();

            PlainActionFuture<FileList> first = new PlainActionFuture<>();
            list(service, provider, computed::addAndGet, EsExecutors.DIRECT_EXECUTOR_SERVICE, first);
            assertEquals(2, first.actionGet().fileCount());

            PlainActionFuture<FileList> second = new PlainActionFuture<>();
            list(service, provider, served::addAndGet, EsExecutors.DIRECT_EXECUTOR_SERVICE, second);
            assertEquals(2, second.actionGet().fileCount());

            assertEquals(2L * FileList.LISTING_BYTES_PER_ENTRY, computed.get());
            assertEquals("a hit reserves what the walk would have", computed.get(), served.get());
            assertEquals("the second query listed nothing", 2, provider.listObjectsCalls.get());
        }
    }

    /** A query that waited on another's listing allocated nothing in the walk, and is charged as for a hit. */
    public void testCachedListingAsyncChargesAFollowerForTheListingItWaitedOn() throws Exception {
        try (ExternalSourceCacheService cache = new ExternalSourceCacheService(cacheEnabled())) {
            DatasetListingService service = new DatasetListingService(Settings.EMPTY, cache, null, null, null);
            StubProvider provider = new StubProvider(twoFolders());
            List<Runnable> queued = new ArrayList<>();
            AtomicLong leaderCharge = new AtomicLong();
            AtomicLong followerCharge = new AtomicLong();
            PlainActionFuture<FileList> leader = new PlainActionFuture<>();
            PlainActionFuture<FileList> follower = new PlainActionFuture<>();

            list(service, provider, leaderCharge::addAndGet, queued::add, leader);
            list(service, provider, followerCharge::addAndGet, queued::add, follower);
            assertFalse("the leader's per-folder lists have not run", leader.isDone());
            assertFalse("the follower waits on them", follower.isDone());
            assertEquals("one fan-out, not one per query", 2, queued.size());

            queued.forEach(Runnable::run);

            assertEquals(2, leader.actionGet().fileCount());
            assertEquals(2, follower.actionGet().fileCount());
            assertEquals(2L * FileList.LISTING_BYTES_PER_ENTRY, leaderCharge.get());
            assertEquals(leaderCharge.get(), followerCharge.get());
            assertEquals("the folders were listed once", 2, provider.listObjectsCalls.get());
        }
    }

    /** Lists the entries under a prefix, and reports two folders under {@code data/} so a listing fans out. */
    private static final class StubProvider implements StorageProvider {
        private final List<StorageEntry> entries;
        final java.util.concurrent.atomic.AtomicInteger listObjectsCalls = new java.util.concurrent.atomic.AtomicInteger();

        StubProvider(List<StorageEntry> entries) {
            this.entries = entries;
        }

        @Override
        public boolean listsInKeyOrder() {
            return true;
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            listObjectsCalls.incrementAndGet();
            String p = prefix.toString().endsWith("/") ? prefix.toString() : prefix + "/";
            Iterator<StorageEntry> it = entries.stream().filter(e -> e.path().toString().startsWith(p)).iterator();
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
            return new StorageChildren(List.of(), List.of(StoragePath.of("s3://bucket/data/d1"), StoragePath.of("s3://bucket/data/d2")));
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
        public boolean exists(StoragePath path) throws IOException {
            return true;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}
    }
}
