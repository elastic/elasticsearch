/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Reproductions for the {@code partition_detection} dataset setting having no effect on the read path.
 *
 * <p>Each test states what the setting promises the user and fails while that promise is unmet. The two
 * {@code testMachineryHonours*} tests are the control: they call the {@code expandGlob} overload that DOES take a
 * {@link PartitionConfig} and pass today, which proves the detection machinery is implemented and working. Every
 * other test drives {@link GlobExpander#expand} — the overload {@code ExternalSourceResolver} actually calls, which
 * carries no {@link PartitionConfig} and hard-passes {@code null} — and fails, because the user's setting cannot
 * reach the detector through it.
 */
public class PartitionDetectionSettingReproTests extends ESTestCase {

    private static final int MAX = Integer.MAX_VALUE;

    /** A Hive-shaped tree: the partition key is spelled in the directory name. */
    private static final String HIVE_PATTERN = "s3://bucket/data/**/*.parquet";
    private static final List<StorageEntry> HIVE_TREE = List.of(
        entry("s3://bucket/data/year=2024/a.parquet"),
        entry("s3://bucket/data/year=2025/b.parquet")
    );

    /** A NON-Hive tree: bare directory segments, exactly what {@code partition_path} exists to describe. */
    private static final String FLAT_PATTERN = "s3://bucket/logs/**/*.parquet";
    private static final List<StorageEntry> FLAT_TREE = List.of(
        entry("s3://bucket/logs/2024/a.parquet"),
        entry("s3://bucket/logs/2025/b.parquet")
    );

    // ---------------------------------------------------------------------------------------------------------
    // Controls: the detection machinery works when a PartitionConfig is handed to it.
    // ---------------------------------------------------------------------------------------------------------

    public void testMachineryHonoursNoneWhenGivenAPartitionConfig() throws IOException {
        FileList listing = GlobExpander.expandGlob(HIVE_PATTERN, provider(HIVE_TREE), null, true, config("none", null), Map.of());
        assertNull("the NONE strategy suppresses detection when the config reaches the expander", listing.partitionMetadata());
    }

    public void testMachineryHonoursTemplateWhenGivenAPartitionConfig() throws IOException {
        FileList listing = GlobExpander.expandGlob(FLAT_PATTERN, provider(FLAT_TREE), null, true, config("template", "{year}"), Map.of());
        assertEquals("the TEMPLATE strategy names the column after the template placeholder", Set.of("year"), columnsOf(listing));
    }

    // ---------------------------------------------------------------------------------------------------------
    // Reproductions: the same settings, driven through the entry point the resolver uses.
    // ---------------------------------------------------------------------------------------------------------

    /**
     * {@code partition_detection: none} is documented as the way to turn partition detection off. Through the
     * resolver's entry point it does not: the Hive detector runs anyway and injects a {@code year} column that is
     * in none of the user's files.
     */
    public void testNoneDisablesDetection() throws IOException {
        FileList listing = expandAsResolverDoes(HIVE_PATTERN, HIVE_TREE, Map.of("partition_detection", "none"));
        assertNull("partition_detection:none must suppress partition detection", listing.partitionMetadata());
    }

    /**
     * {@code partition_detection: none} and {@code hive_partitioning: false} both mean "do not derive columns from
     * the path". They must agree. Today only the second one works, so the user who follows the documented setting
     * gets a different schema from the user who follows the undocumented one.
     */
    public void testNoneAgreesWithHivePartitioningFalse() throws IOException {
        FileList viaDocumentedSetting = expandAsResolverDoes(HIVE_PATTERN, HIVE_TREE, Map.of("partition_detection", "none"));
        FileList viaUndocumentedSetting = expandAsResolverDoes(HIVE_PATTERN, HIVE_TREE, Map.of("hive_partitioning", "false"));
        assertEquals(
            "partition_detection:none and hive_partitioning:false must produce the same schema",
            columnsOf(viaUndocumentedSetting),
            columnsOf(viaDocumentedSetting)
        );
    }

    /**
     * A {@code partition_path} template is the only way to describe a non-Hive layout. Through the resolver's entry
     * point it is dropped: the Hive detector finds no {@code key=value} segment, so the user gets no partition
     * columns at all — nothing to filter on, and therefore nothing to prune on either.
     */
    public void testTemplateExtractsColumnsFromANonHiveLayout() throws IOException {
        FileList listing = expandAsResolverDoes(
            FLAT_PATTERN,
            FLAT_TREE,
            Map.of("partition_detection", "template", "partition_path", "{year}")
        );
        assertEquals("partition_path must name the partition column on a non-Hive layout", Set.of("year"), columnsOf(listing));
    }

    /**
     * Setting {@code partition_path} alone promotes AUTO to TEMPLATE ({@code PartitionConfig.fromConfig}). That
     * promotion is unreachable from the read path, so the template is ignored here too.
     */
    public void testPartitionPathAlonePromotesAutoToTemplate() throws IOException {
        FileList listing = expandAsResolverDoes(FLAT_PATTERN, FLAT_TREE, Map.of("partition_path", "{year}"));
        assertEquals("partition_path alone must select template detection", Set.of("year"), columnsOf(listing));
    }

    /**
     * An explicit {@code template} strategy must beat Hive-shaped directory names — that is the whole point of
     * naming a strategy rather than letting AUTO guess. Today the Hive detector runs regardless, so a user whose
     * directories happen to look Hive-shaped cannot override the interpretation.
     */
    public void testTemplateStrategyOverridesAHiveShapedLayout() throws IOException {
        FileList listing = expandAsResolverDoes(
            HIVE_PATTERN,
            HIVE_TREE,
            Map.of("partition_detection", "template", "partition_path", "{bucket}")
        );
        assertEquals("an explicit template strategy must not fall back to Hive detection", Set.of("bucket"), columnsOf(listing));
    }

    /**
     * The four strategies are meant to be four different behaviours. Through the read path they collapse onto one:
     * every value produces byte-identical partition columns, which is what "the setting does nothing" looks like
     * when you cannot see the wiring.
     */
    public void testStrategiesAreDistinguishable() throws IOException {
        Set<Set<String>> distinctResults = new LinkedHashSet<>();
        for (String strategy : List.of("auto", "hive", "template", "none")) {
            distinctResults.add(columnsOf(expandAsResolverDoes(HIVE_PATTERN, HIVE_TREE, settingsFor(strategy))));
        }
        assertNotEquals(
            "all four partition_detection values produced the same schema, so the setting has no effect",
            1,
            distinctResults.size()
        );
    }

    /**
     * The listing cache is keyed on {@code GlobExpander.listingCacheDiscriminator}, which binds the path pattern, the
     * filter hints and the {@code hivePartitioning} flag — but not the partition strategy. The cached {@link FileList} carries its
     * {@code PartitionMetadata}, so once the strategy affects detection, two datasets differing only in
     * {@code partition_detection} collide on one cache entry and one of them is served the other's partition
     * columns. This is latent today precisely because the setting is inert — it becomes a wrong answer the moment
     * the setting is wired, so it belongs in the same change.
     */
    public void testListingCacheIdentityBindsThePartitionStrategy() throws IOException {
        FileList none = GlobExpander.expandGlob(HIVE_PATTERN, provider(HIVE_TREE), null, true, config("none", null), Map.of());
        FileList hive = GlobExpander.expandGlob(HIVE_PATTERN, provider(HIVE_TREE), null, true, config("hive", null), Map.of());
        assertNotEquals("the two strategies must produce different listings for this to matter", columnsOf(none), columnsOf(hive));

        String discriminator = GlobExpander.listingCacheDiscriminator(HIVE_PATTERN, null, true);
        assertTrue(
            "the listing cache identity ["
                + discriminator
                + "] does not mention the partition strategy, so two datasets differing only in partition_detection "
                + "share one cache entry",
            discriminator.contains("none") || discriminator.contains("NONE")
        );
    }

    // ---------------------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------------------

    /**
     * Expands exactly the way {@code ExternalSourceResolver} does: it reduces the dataset's settings to the single
     * {@code hivePartitioning} boolean and calls the {@link GlobExpander#expand} overload that carries no
     * {@link PartitionConfig}. Mirrors {@code ExternalSourceResolver.isHivePartitioningEnabled}.
     */
    private static FileList expandAsResolverDoes(String pattern, List<StorageEntry> tree, Map<String, Object> settings) throws IOException {
        Object hive = settings.get(PartitionConfig.CONFIG_PARTITIONING_HIVE);
        boolean hivePartitioning = hive == null || "false".equalsIgnoreCase(hive.toString()) == false;
        return GlobExpander.expand(pattern, provider(tree), null, hivePartitioning, MAX, MAX);
    }

    private static Map<String, Object> settingsFor(String strategy) {
        return "template".equals(strategy)
            ? Map.of("partition_detection", strategy, "partition_path", "{bucket}")
            : Map.of("partition_detection", strategy);
    }

    private static PartitionConfig config(String strategy, String template) {
        return PartitionConfig.fromConfig(
            template == null ? Map.of("partition_detection", strategy) : Map.of("partition_detection", strategy, "partition_path", template)
        );
    }

    private static Set<String> columnsOf(FileList listing) {
        PartitionMetadata metadata = listing.partitionMetadata();
        return metadata == null ? Set.of() : metadata.partitionColumns().keySet();
    }

    private static StorageEntry entry(String path) {
        return new StorageEntry(StoragePath.of(path), 1L, Instant.EPOCH);
    }

    private static StorageProvider provider(List<StorageEntry> listing) {
        return new StubProvider(listing);
    }

    private static class StubProvider implements StorageProvider {
        private final List<StorageEntry> listing;

        StubProvider(List<StorageEntry> listing) {
            this.listing = listing;
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubStorageObject(path, 0);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            return new StubStorageObject(path, length);
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            return new StubStorageObject(path, length);
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            List<StorageEntry> matched = new ArrayList<>();
            for (StorageEntry e : listing) {
                if (e.path().toString().startsWith(prefix.toString())) {
                    matched.add(e);
                }
            }
            return new StorageIterator() {
                private final Iterator<StorageEntry> it = matched.iterator();

                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public StorageEntry next() {
                    if (it.hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    return it.next();
                }

                @Override
                public void close() {}
            };
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
        public void close() {}
    }

    private static class StubStorageObject implements StorageObject {
        private final StoragePath path;
        private final long length;

        StubStorageObject(StoragePath path, long length) {
            this.path = path;
            this.length = length;
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
            return length;
        }

        @Override
        public Instant lastModified() {
            return Instant.EPOCH;
        }

        @Override
        public boolean exists() {
            return false;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }
}
