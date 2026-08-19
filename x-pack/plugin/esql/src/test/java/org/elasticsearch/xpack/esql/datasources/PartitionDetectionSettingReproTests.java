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
 * Regression pins for the {@code partition_detection} and {@code partition_path} dataset settings reaching the read
 * path.
 *
 * <p>These began as reproductions. Against {@code main} the resolver reduced every partition setting to one
 * boolean and called expander entry points that carried no {@link PartitionConfig}, so the Hive detector ran
 * whatever the user asked for; seven of these tests were red for that reason. They pass now, and a red one means
 * the settings have stopped reaching the detector.
 *
 * <p>The two {@code testMachineryHonours*} tests date from that period, when they were the controls: they handed a
 * resolved config straight to the detectors and passed while the rest failed, which located the fault in the wiring
 * rather than the detectors. They no longer differ in kind from the others — every test now drives the same
 * settings-map entry point — but they are kept as the narrowest statement of what each strategy does.
 */
public class PartitionDetectionSettingReproTests extends ESTestCase {

    private static final int MAX = Integer.MAX_VALUE;

    /** A Hive-shaped tree: the partition key is spelled in the directory name. */
    private static final String HIVE_PATTERN = "s3://bucket/data/**" + "/*.parquet";
    private static final List<StorageEntry> HIVE_TREE = List.of(
        entry("s3://bucket/data/year=2024/a.parquet"),
        entry("s3://bucket/data/year=2025/b.parquet")
    );

    /** A NON-Hive tree: bare directory segments, exactly what {@code partition_path} exists to describe. */
    private static final String FLAT_PATTERN = "s3://bucket/logs/**" + "/*.parquet";
    private static final List<StorageEntry> FLAT_TREE = List.of(
        entry("s3://bucket/logs/2024/a.parquet"),
        entry("s3://bucket/logs/2025/b.parquet")
    );

    // ---------------------------------------------------------------------------------------------------------
    // Controls: the detection machinery works when a PartitionConfig is handed to it.
    // ---------------------------------------------------------------------------------------------------------

    public void testMachineryHonoursNoneWhenGivenAPartitionConfig() throws IOException {
        FileList listing = GlobExpander.expandGlob(HIVE_PATTERN, provider(HIVE_TREE), null, partitionSettings("none", null));
        assertNull("the NONE strategy suppresses detection when the config reaches the expander", listing.partitionMetadata());
    }

    public void testMachineryHonoursTemplateWhenGivenAPartitionConfig() throws IOException {
        FileList listing = GlobExpander.expandGlob(FLAT_PATTERN, provider(FLAT_TREE), null, partitionSettings("template", "{year}"));
        assertEquals("the TEMPLATE strategy names the column after the template placeholder", Set.of("year"), columnsOf(listing));
    }

    // ---------------------------------------------------------------------------------------------------------
    // Reproductions: the same settings, driven through the entry point the resolver uses.
    // ---------------------------------------------------------------------------------------------------------

    /**
     * {@code partition_detection: none} is documented as the way to turn partition detection off. Against
     * {@code main} it did not: the Hive detector ran anyway and injected a {@code year} column present in none of
     * the user's files.
     */
    public void testNoneDisablesDetection() throws IOException {
        FileList listing = expandAsResolverDoes(HIVE_PATTERN, HIVE_TREE, Map.of("partition_detection", "none"));
        assertNull("partition_detection:none must suppress partition detection", listing.partitionMetadata());
    }

    /**
     * {@code partition_detection: none} and {@code hive_partitioning: false} both mean "do not derive columns from
     * the path". They must agree. Against {@code main} only the second worked, so the documented setting and the undocumented one produced
     * different schemas.
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
     * The load-bearing BWC shape. {@code {partition_detection: hive, hive_partitioning: "false"}} was registerable
     * before this validation existed, and read with detection OFF because the boolean short-circuited first. It
     * must keep reading that way now that the two settings resolve through one strategy — verified here at the
     * read-path grain, not just at {@code fromConfig}.
     */
    public void testGrandfatheredHiveFalseWithExplicitStrategyStillDisablesDetection() throws IOException {
        FileList listing = expandAsResolverDoes(
            HIVE_PATTERN,
            HIVE_TREE,
            Map.of("partition_detection", "hive", PartitionConfig.CONFIG_PARTITIONING_HIVE, "false")
        );
        assertNull("hive_partitioning:false must still win over an explicit strategy", listing.partitionMetadata());
    }

    /**
     * A stored template that names no columns — {@code year={year}} is not a whole-segment placeholder — must keep
     * reading as it did before the setting reached the read path, which for this layout means Hive detection.
     * {@code TemplatePartitionDetector}'s constructor rejects such a template, so the guard in
     * {@code resolveDetector} is what stops this shape from throwing on every query.
     */
    public void testStoredPlaceholderlessTemplateFallsBackToHive() throws IOException {
        FileList listing = expandAsResolverDoes(
            HIVE_PATTERN,
            HIVE_TREE,
            Map.of("partition_detection", "template", "partition_path", "year={year}/month={month}")
        );
        assertEquals("a placeholderless template must not throw, and must keep the Hive columns", Set.of("year"), columnsOf(listing));
    }

    /**
     * A {@code partition_path} template is the only way to describe a non-Hive layout. Against {@code main} it was
     * dropped: the Hive detector found no {@code key=value} segment, so the dataset had no partition columns to
     * filter or prune on.
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
     * Setting {@code partition_path} alone promotes AUTO to TEMPLATE ({@code PartitionConfig.fromConfig}). Against
     * {@code main} that promotion never reached the read path.
     */
    public void testPartitionPathAlonePromotesAutoToTemplate() throws IOException {
        FileList listing = expandAsResolverDoes(FLAT_PATTERN, FLAT_TREE, Map.of("partition_path", "{year}"));
        assertEquals("partition_path alone must select template detection", Set.of("year"), columnsOf(listing));
    }

    /**
     * An explicit {@code template} strategy must beat Hive-shaped directory names — that is the whole point of
     * naming a strategy rather than letting AUTO guess. Against {@code main} the Hive detector ran regardless, so a
     * Hive-shaped layout could not be reinterpreted.
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
     * The four strategies are four behaviours. Against {@code main} they collapsed onto one: every value produced
     * identical partition columns.
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
     * columns. This was latent while the setting was unread; wiring it makes the collision a wrong answer, so the key binds the
     * strategy in the same change.
     */
    public void testListingCacheIdentityBindsThePartitionStrategy() throws IOException {
        FileList none = GlobExpander.expandGlob(HIVE_PATTERN, provider(HIVE_TREE), null, partitionSettings("none", null));
        FileList hive = GlobExpander.expandGlob(HIVE_PATTERN, provider(HIVE_TREE), null, partitionSettings("hive", null));
        assertNotEquals("the two strategies must produce different listings for this to matter", columnsOf(none), columnsOf(hive));

        String discriminator = GlobExpander.listingCacheDiscriminator(HIVE_PATTERN, null, partitionSettings("none", null));
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
     * Expands the way {@code ExternalSourceResolver} does: it hands the dataset's settings map to
     * {@link GlobExpander#expand}, which resolves the {@link PartitionConfig} once at the boundary.
     */
    private static FileList expandAsResolverDoes(String pattern, List<StorageEntry> tree, Map<String, Object> settings) throws IOException {
        return GlobExpander.expand(pattern, provider(tree), null, settings, MAX, MAX);
    }

    private static Map<String, Object> settingsFor(String strategy) {
        return "template".equals(strategy)
            ? Map.of("partition_detection", strategy, "partition_path", "{bucket}")
            : Map.of("partition_detection", strategy);
    }

    /** The dataset settings a user would register for a given strategy. */
    private static Map<String, Object> partitionSettings(String strategy, String template) {
        return template == null
            ? Map.of("partition_detection", strategy)
            : Map.of("partition_detection", strategy, "partition_path", template);
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
