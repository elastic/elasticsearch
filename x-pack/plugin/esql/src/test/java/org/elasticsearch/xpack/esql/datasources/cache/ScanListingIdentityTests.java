/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.DatasetListingService;
import org.elasticsearch.xpack.esql.datasources.DecompressionCodecRegistry;
import org.elasticsearch.xpack.esql.datasources.FileSplitProvider;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.StorageProviderRegistry;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.SplitDiscoveryContext;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A scan that lists the dataset for itself must cache that listing under the identity its own storage provider
 * reports, which is the identity resolution used. Nothing else in the suite covers it, and getting it wrong is
 * silent: the two listings land under different keys, both are correct, and the warm second query that the
 * shared cache exists to serve is never served.
 * <p>
 * Lives in this package because the assertion is on the key that reached {@link ExternalSourceCacheService}'s
 * listing cache, and that accessor is package-private.
 */
public class ScanListingIdentityTests extends ESTestCase {

    private static final String IDENTITY = "endpoint=https://store.example";
    private static final String SECRET = "secret-digest-7f3a";
    private static final String GLOB = "s3://bucket/data/*.csv";

    public void testTheScansListingIsCachedUnderTheIdentityItsProviderReports() throws Exception {
        List<StorageEntry> listed = List.of(
            new StorageEntry(StoragePath.of("s3://bucket/data/a.csv"), 128L, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://bucket/data/b.csv"), 128L, Instant.EPOCH)
        );
        StorageProviderRegistry registry = new StorageProviderRegistry(Settings.EMPTY);
        registry.registerFactory("s3", reportingFactory(new ListingOnlyProvider(listed)));

        try (ExternalSourceCacheService cacheService = new ExternalSourceCacheService(cacheEnabled())) {
            DatasetListingService listingService = new DatasetListingService(Settings.EMPTY, cacheService, null, null, null);
            FileSplitProvider splitter = new FileSplitProvider(
                1L << 20,
                new DecompressionCodecRegistry(),
                registry,
                null,
                Settings.EMPTY,
                null,
                listingService,
                null
            );

            // A truncated listing is a prefix of the dataset, so the scan must discover the rest itself — the only
            // path on which it calls the listing cache at all.
            splitter.discoverSplits(
                new SplitDiscoveryContext(
                    new SimpleSourceMetadata(List.of(), "csv", GLOB),
                    truncated(GlobExpander.fileListOf(listed.subList(0, 1), GLOB)),
                    // Non-empty, because the registry answers an empty config with the unconfigured provider and
                    // reports no identity for it — correctly, and resolution would report the same "" — so an empty
                    // config here would make this assertion pass for a reason production never takes.
                    Map.of("endpoint", "https://store.example"),
                    PartitionMetadata.EMPTY,
                    List.of()
                )
            );

            List<ListingCacheKey> keys = new ArrayList<>();
            cacheService.listingCache().forEach((k, v) -> keys.add(k));
            assertEquals("the scan's listing must be cached exactly once; keys=" + keys, 1, keys.size());
            assertEquals("the scan must key on what its provider reported", IDENTITY, keys.get(0).storageIdentity());
            assertEquals("and on the secrets that provider consumed", SECRET, keys.get(0).secretIdentity());
        }
    }

    private static Settings cacheEnabled() {
        return Settings.builder()
            .put("esql.external.cache.size", "10mb")
            .put("esql.external.cache.enabled", true)
            .put("esql.external.cache.listing.ttl", "30s")
            .build();
    }

    /** A factory that reports an identity, so the test can tell a threaded identity from a defaulted one. */
    private static StorageProviderFactory reportingFactory(StorageProvider provider) {
        return new StorageProviderFactory() {
            @Override
            public StorageProvider create(Settings settings) {
                return provider;
            }

            @Override
            public Configured<StorageProvider> createTrackingConsumedKeys(Settings settings, Map<String, Object> config) {
                return new Configured<>(provider, Set.of("endpoint"), IDENTITY, SECRET);
            }
        };
    }

    /** {@code fileList} with {@link FileList#isTruncated()} forced on; every other answer is its own. */
    private static FileList truncated(FileList delegate) {
        return new FileList() {
            @Override
            public boolean isTruncated() {
                return true;
            }

            @Override
            public int fileCount() {
                return delegate.fileCount();
            }

            @Override
            public StoragePath path(int i) {
                return delegate.path(i);
            }

            @Override
            public long size(int i) {
                return delegate.size(i);
            }

            @Override
            public long lastModifiedMillis(int i) {
                return delegate.lastModifiedMillis(i);
            }

            @Override
            public String originalPattern() {
                return delegate.originalPattern();
            }

            @Override
            public PartitionMetadata partitionMetadata() {
                return delegate.partitionMetadata();
            }

            @Override
            public boolean isResolved() {
                return delegate.isResolved();
            }

            @Override
            public boolean isEmpty() {
                return delegate.isEmpty();
            }

            @Override
            public long estimatedBytes() {
                return delegate.estimatedBytes();
            }
        };
    }

    /** Lists the entries it was given and refuses everything else: this test only exercises discovery. */
    private static final class ListingOnlyProvider implements StorageProvider {
        private final List<StorageEntry> entries;

        ListingOnlyProvider(List<StorageEntry> entries) {
            this.entries = entries;
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            Iterator<StorageEntry> it = entries.iterator();
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
            // Directory-aware listing is a different discovery path from the flat listObjects this exercises.
            return null;
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            throw new UnsupportedOperationException("this test does not read objects");
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            throw new UnsupportedOperationException("this test does not read objects");
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            throw new UnsupportedOperationException("this test does not read objects");
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
