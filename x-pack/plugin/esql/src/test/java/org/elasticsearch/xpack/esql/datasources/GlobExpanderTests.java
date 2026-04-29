/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
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
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.hamcrest.Matchers.containsString;

public class GlobExpanderTests extends ESTestCase {

    // -- isMultiFile --

    public void testIsMultiFileWithGlob() {
        assertTrue(GlobExpander.isMultiFile("s3://bucket/*.parquet"));
        assertTrue(GlobExpander.isMultiFile("s3://bucket/data?.csv"));
        assertTrue(GlobExpander.isMultiFile("s3://bucket/{a,b}.parquet"));
        assertTrue(GlobExpander.isMultiFile("s3://bucket/[abc].parquet"));
    }

    public void testIsMultiFileWithComma() {
        assertTrue(GlobExpander.isMultiFile("s3://bucket/a.parquet,s3://bucket/b.parquet"));
    }

    public void testIsMultiFileLiteral() {
        assertFalse(GlobExpander.isMultiFile("s3://bucket/data.parquet"));
        assertFalse(GlobExpander.isMultiFile(null));
    }

    /**
     * RFC 3986 §3.2.2 requires brackets around IPv6 host literals in URL authorities:
     *   http://[::1]:8080/path        s3://[fe80::1]/bucket/file.parquet
     * An IPv6 URL with no glob metacharacters in the PATH is a single concrete URL,
     * not a glob pattern.
     */
    public void testIsMultiFileIpv6HostIsNotAGlobPattern() {
        assertFalse(GlobExpander.isMultiFile("http://[::1]:8080/logs/data.parquet"));
        assertFalse(GlobExpander.isMultiFile("s3://[fe80::1]/bucket/hits.parquet"));
    }

    /**
     * When a real glob IS in the path, the URL is a glob pattern — but the IPv6 authority
     * brackets must not themselves count as glob characters. Only the path component is
     * inspected for glob metacharacters.
     */
    public void testIsMultiFileIpv6HostWithGlobInPath() {
        assertTrue(GlobExpander.isMultiFile("http://[::1]/logs/2026-*/data.parquet"));
    }

    // -- expandGlob --

    public void testExpandGlobLiteralReturnsUnresolved() throws IOException {
        StubProvider provider = new StubProvider(List.of());
        FileList result = GlobExpander.expandGlob("s3://bucket/data.parquet", provider);
        assertFalse(result.isResolved());
    }

    public void testExpandGlobMatchesFiles() throws IOException {
        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/file1.parquet", 100),
            entry("s3://bucket/data/file2.parquet", 200),
            entry("s3://bucket/data/file3.csv", 50)
        );
        StubProvider provider = new StubProvider(listing);

        FileList result = GlobExpander.expandGlob("s3://bucket/data/*.parquet", provider);
        assertTrue(result.isResolved());
        assertEquals(2, result.fileCount());
        assertEquals("s3://bucket/data/file1.parquet", result.path(0).toString());
        assertEquals("s3://bucket/data/file2.parquet", result.path(1).toString());
    }

    public void testExpandGlobNoMatchReturnsEmpty() throws IOException {
        List<StorageEntry> listing = List.of(entry("s3://bucket/data/file.csv", 50));
        StubProvider provider = new StubProvider(listing);

        FileList result = GlobExpander.expandGlob("s3://bucket/data/*.parquet", provider);
        assertTrue(result.isEmpty());
    }

    public void testExpandGlobPreservesPattern() throws IOException {
        List<StorageEntry> listing = List.of(entry("s3://bucket/data/f.parquet", 10));
        StubProvider provider = new StubProvider(listing);

        FileList result = GlobExpander.expandGlob("s3://bucket/data/*.parquet", provider);
        assertEquals("s3://bucket/data/*.parquet", result.originalPattern());
    }

    /**
     * When an EXTERNAL URL has an IPv6 host and a glob in the path, the authority brackets
     * must be passed through unchanged and must not be treated as a glob character class.
     * The glob expansion must match only on the path component.
     */
    public void testExpandGlobIpv6HostWithGlobInPath() throws IOException {
        List<StorageEntry> listing = List.of(
            entry("http://[::1]/logs/2026-05/data.parquet", 100),
            entry("http://[::1]/logs/2026-06/data.parquet", 200)
        );
        StubProvider provider = new StubProvider(listing);

        FileList result = GlobExpander.expandGlob("http://[::1]/logs/2026-*/data.parquet", provider);
        assertTrue(result.isResolved());
        assertEquals(2, result.fileCount());
        assertEquals("http://[::1]/logs/2026-05/data.parquet", result.path(0).toString());
        assertEquals("http://[::1]/logs/2026-06/data.parquet", result.path(1).toString());
    }

    // -- expandCommaSeparated --

    public void testExpandCommaSeparatedMixedGlobAndLiteral() throws IOException {
        List<StorageEntry> listing = List.of(entry("s3://bucket/data/a.parquet", 100), entry("s3://bucket/data/b.parquet", 200));
        StubProvider provider = new StubProvider(listing);
        provider.existingPaths.add("s3://bucket/extra.parquet");

        FileList result = GlobExpander.expandCommaSeparated("s3://bucket/data/*.parquet, s3://bucket/extra.parquet", provider);
        assertTrue(result.isResolved());
        assertEquals(3, result.fileCount());
    }

    public void testExpandCommaSeparatedAllMissing() throws IOException {
        StubProvider provider = new StubProvider(List.of());
        FileList result = GlobExpander.expandCommaSeparated("s3://bucket/missing.parquet", provider);
        assertTrue(result.isEmpty());
    }

    // -- partition-aware glob rewriting --

    public void testRewriteGlobWithEqualsHint() {
        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024));
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/year=*/*.parquet", hints);
        assertEquals("s3://bucket/year=2024/*.parquet", rewritten);
    }

    public void testRewriteGlobWithInHint() {
        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.IN, 2023, 2024));
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/year=*/*.parquet", hints);
        assertEquals("s3://bucket/year={2023,2024}/*.parquet", rewritten);
    }

    public void testRewriteGlobWithRangeHintNoRewrite() {
        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.GREATER_THAN_OR_EQUAL, 2020));
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/year=*/*.parquet", hints);
        assertEquals("s3://bucket/year=*/*.parquet", rewritten);
    }

    public void testRewriteGlobMultipleHints() {
        var hints = List.of(
            hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024),
            hint("month", PartitionFilterHintExtractor.Operator.IN, 1, 2, 3)
        );
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/year=*/month=*/*.parquet", hints);
        assertEquals("s3://bucket/year=2024/month={1,2,3}/*.parquet", rewritten);
    }

    public void testRewriteGlobNonWildcardNotRewritten() {
        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024));
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/year=2023/*.parquet", hints);
        assertEquals("s3://bucket/year=2023/*.parquet", rewritten);
    }

    public void testRewriteGlobNoHintsNoChange() {
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/year=*/*.parquet", List.of());
        assertEquals("s3://bucket/year=*/*.parquet", rewritten);
    }

    public void testExpandGlobWithHivePartitionDetection() throws IOException {
        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/year=2024/file1.parquet", 100),
            entry("s3://bucket/data/year=2023/file2.parquet", 200)
        );
        StubProvider provider = new StubProvider(listing);

        FileList result = GlobExpander.expandGlob("s3://bucket/data/year=*/*.parquet", provider, null, true);
        assertTrue(result.isResolved());
        assertEquals(2, result.fileCount());
        assertNotNull(result.partitionMetadata());
        assertFalse(result.partitionMetadata().isEmpty());
        assertTrue(result.partitionMetadata().partitionColumns().containsKey("year"));
    }

    public void testExpandGlobWithHivePartitioningDisabled() throws IOException {
        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/year=2024/file1.parquet", 100),
            entry("s3://bucket/data/year=2023/file2.parquet", 200)
        );
        StubProvider provider = new StubProvider(listing);

        FileList result = GlobExpander.expandGlob("s3://bucket/data/year=*/*.parquet", provider, null, false);
        assertTrue(result.isResolved());
        assertEquals(2, result.fileCount());
        assertNull(result.partitionMetadata());
    }

    public void testExpandGlobNonHivePathsNoPartitionMetadata() throws IOException {
        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/2024/file1.parquet", 100),
            entry("s3://bucket/data/2023/file2.parquet", 200)
        );
        StubProvider provider = new StubProvider(listing);

        @SuppressWarnings("RegexpMultiline")
        FileList result = GlobExpander.expandGlob("s3://bucket/data/**/*.parquet", provider, null, true);
        assertTrue(result.isResolved());
        assertNull(result.partitionMetadata());
    }

    // -- template-based glob rewriting --

    public void testRewriteGlobWithTemplateHints() {
        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024));
        PartitionConfig config = new PartitionConfig(PartitionConfig.Strategy.TEMPLATE, "{year}/{month}");
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/*/*/*.parquet", hints, config);
        // First wildcard maps to {year} → rewritten to 2024
        assertEquals("s3://bucket/2024/*/*.parquet", rewritten);
    }

    public void testRewriteGlobWithTemplateInHints() {
        var hints = List.of(hint("month", PartitionFilterHintExtractor.Operator.IN, 1, 2));
        PartitionConfig config = new PartitionConfig(PartitionConfig.Strategy.TEMPLATE, "{year}/{month}");
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/*/*/*.parquet", hints, config);
        // Second wildcard maps to {month} → rewritten to {1,2}
        assertEquals("s3://bucket/*/{1,2}/*.parquet", rewritten);
    }

    public void testRewriteGlobWithTemplateRangeHintsNoRewrite() {
        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.GREATER_THAN_OR_EQUAL, 2020));
        PartitionConfig config = new PartitionConfig(PartitionConfig.Strategy.TEMPLATE, "{year}/{month}");
        // Range hints are not rewritable, so pattern should be unchanged
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/*/*/*.parquet", hints, config);
        assertEquals("s3://bucket/*/*/*.parquet", rewritten);
    }

    public void testRewriteGlobWithTemplateNoMatchingHints() {
        var hints = List.of(hint("region", PartitionFilterHintExtractor.Operator.EQUALS, "us-east"));
        PartitionConfig config = new PartitionConfig(PartitionConfig.Strategy.TEMPLATE, "{year}/{month}");
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/*/*/*.parquet", hints, config);
        // "region" not in template, so Hive rewriting also won't match → unchanged
        assertEquals("s3://bucket/*/*/*.parquet", rewritten);
    }

    public void testRewriteGlobWithTemplateThreeColumns() {
        var hints = List.of(
            hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024),
            hint("day", PartitionFilterHintExtractor.Operator.EQUALS, 15)
        );
        PartitionConfig config = new PartitionConfig(PartitionConfig.Strategy.TEMPLATE, "{year}/{month}/{day}");
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/*/*/*/*.parquet", hints, config);
        assertEquals("s3://bucket/2024/*/15/*.parquet", rewritten);
    }

    public void testExpandGlobWithPartitionConfig() throws IOException {
        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/2024/01/file1.parquet", 100),
            entry("s3://bucket/data/2023/12/file2.parquet", 200)
        );
        StubProvider provider = new StubProvider(listing);
        PartitionConfig config = new PartitionConfig(PartitionConfig.Strategy.TEMPLATE, "{year}/{month}");

        @SuppressWarnings("RegexpMultiline")
        FileList result = GlobExpander.expandGlob("s3://bucket/data/**/*.parquet", provider, null, true, config, Map.of());
        assertTrue(result.isResolved());
        assertEquals(2, result.fileCount());
        assertNotNull(result.partitionMetadata());
        assertTrue(result.partitionMetadata().partitionColumns().containsKey("year"));
        assertTrue(result.partitionMetadata().partitionColumns().containsKey("month"));
    }

    public void testExpandGlobWithNonePartitionConfig() throws IOException {
        List<StorageEntry> listing = List.of(
            entry("s3://bucket/data/year=2024/file1.parquet", 100),
            entry("s3://bucket/data/year=2023/file2.parquet", 200)
        );
        StubProvider provider = new StubProvider(listing);
        PartitionConfig config = new PartitionConfig(PartitionConfig.Strategy.NONE, null);

        FileList result = GlobExpander.expandGlob("s3://bucket/data/year=*/*.parquet", provider, null, true, config, Map.of());
        assertTrue(result.isResolved());
        assertNull(result.partitionMetadata());
    }

    // -- max discovered files cap --

    public void testExpandGlobExceedsMaxDiscoveredFilesThrows() {
        List<StorageEntry> listing = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            listing.add(entry("s3://bucket/data/file" + i + ".parquet", 100));
        }
        StubProvider provider = new StubProvider(listing);

        QlIllegalArgumentException e = expectThrows(
            QlIllegalArgumentException.class,
            () -> GlobExpander.expandGlob("s3://bucket/data/*.parquet", provider, null, true, 10, Integer.MAX_VALUE)
        );
        assertThat(e.getMessage(), containsString("Glob pattern discovered too many files"));
        assertThat(e.getMessage(), containsString("limit 10"));
        assertThat(e.getMessage(), containsString("esql.external.max_discovered_files"));
    }

    public void testExpandGlobAtExactLimitSucceeds() throws IOException {
        List<StorageEntry> listing = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            listing.add(entry("s3://bucket/data/file" + i + ".parquet", 100));
        }
        StubProvider provider = new StubProvider(listing);

        FileList result = GlobExpander.expandGlob("s3://bucket/data/*.parquet", provider, null, true, 10, Integer.MAX_VALUE);
        assertTrue(result.isResolved());
        assertEquals(10, result.fileCount());
    }

    public void testExpandGlobBelowLimitSucceeds() throws IOException {
        List<StorageEntry> listing = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            listing.add(entry("s3://bucket/data/file" + i + ".parquet", 100));
        }
        StubProvider provider = new StubProvider(listing);

        FileList result = GlobExpander.expandGlob("s3://bucket/data/*.parquet", provider, null, true, 10, Integer.MAX_VALUE);
        assertTrue(result.isResolved());
        assertEquals(5, result.fileCount());
    }

    public void testExpandCommaSeparatedGlobalCapAcrossSegments() {
        List<StorageEntry> listing = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            listing.add(entry("s3://bucket/data/file" + i + ".parquet", 100));
        }
        StubProvider provider = new StubProvider(listing);
        provider.existingPaths.add("s3://bucket/extra1.parquet");
        provider.existingPaths.add("s3://bucket/extra2.parquet");
        provider.existingPaths.add("s3://bucket/extra3.parquet");

        QlIllegalArgumentException e = expectThrows(
            QlIllegalArgumentException.class,
            () -> GlobExpander.expandCommaSeparated(
                "s3://bucket/data/*.parquet, s3://bucket/extra1.parquet, s3://bucket/extra2.parquet, s3://bucket/extra3.parquet",
                provider,
                null,
                true,
                9,
                Integer.MAX_VALUE
            )
        );
        assertThat(e.getMessage(), containsString("Glob pattern discovered too many files"));
    }

    // -- brace-only HeadObject discovery --

    public void testExpandGlobBraceOnlyUsesExists() throws IOException {
        StubProvider provider = new StubProvider(List.of());
        provider.existingPaths.add("s3://bucket/a.parquet");
        provider.existingPaths.add("s3://bucket/b.parquet");

        FileList result = GlobExpander.expandGlob("s3://bucket/{a,b}.parquet", provider, null, true);
        assertTrue(result.isResolved());
        assertEquals(2, result.fileCount());
        assertEquals("s3://bucket/a.parquet", result.path(0).toString());
        assertEquals("s3://bucket/b.parquet", result.path(1).toString());
    }

    public void testExpandGlobBraceOnlyMissingFileSkipped() throws IOException {
        StubProvider provider = new StubProvider(List.of());
        provider.existingPaths.add("s3://bucket/a.parquet");
        provider.existingPaths.add("s3://bucket/c.parquet");

        FileList result = GlobExpander.expandGlob("s3://bucket/{a,b,c}.parquet", provider, null, true);
        assertTrue(result.isResolved());
        assertEquals(2, result.fileCount());
    }

    public void testExpandGlobBraceOnlyAllMissingReturnsEmpty() throws IOException {
        StubProvider provider = new StubProvider(List.of());

        FileList result = GlobExpander.expandGlob("s3://bucket/{a,b}.parquet", provider, null, true);
        assertTrue(result.isEmpty());
    }

    public void testExpandGlobBraceWithWildcardFallsBackToListing() throws IOException {
        List<StorageEntry> listing = List.of(entry("s3://bucket/a/file.parquet", 100), entry("s3://bucket/b/file.parquet", 200));
        StubProvider provider = new StubProvider(listing);

        FileList result = GlobExpander.expandGlob("s3://bucket/{a,b}/*.parquet", provider, null, true);
        assertTrue(result.isResolved());
        assertEquals(2, result.fileCount());
    }

    public void testExpandGlobBraceExceedsCapFallsBackToListing() throws IOException {
        StringBuilder pattern = new StringBuilder("s3://bucket/{");
        List<StorageEntry> listing = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            if (i > 0) pattern.append(',');
            pattern.append("file").append(i);
            listing.add(entry("s3://bucket/file" + i + ".parquet", 100));
        }
        pattern.append("}.parquet");
        StubProvider provider = new StubProvider(listing);

        FileList result = GlobExpander.expandGlob(pattern.toString(), provider, null, true, 10000, 5);
        assertTrue(result.isResolved());
        assertEquals(200, result.fileCount());
    }

    public void testExpandGlobBraceOnlyWithHiveSegments() throws IOException {
        StubProvider provider = new StubProvider(List.of());
        provider.existingPaths.add("s3://bucket/year=2024/data.parquet");
        provider.existingPaths.add("s3://bucket/year=2025/data.parquet");

        FileList result = GlobExpander.expandGlob("s3://bucket/year={2024,2025}/data.parquet", provider, null, true);
        assertTrue(result.isResolved());
        assertEquals(2, result.fileCount());
    }

    // -- fully-resolved hint pattern --

    public void testExpandGlobFullyResolvedByHintsFindsFile() throws IOException {
        StubProvider provider = new StubProvider(List.of());
        provider.existingPaths.add("s3://bucket/year=2024/data.parquet");

        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024));
        FileList result = GlobExpander.expandGlob("s3://bucket/year=*/data.parquet", provider, hints, true);
        assertTrue(result.isResolved());
        assertEquals(1, result.fileCount());
        assertEquals("s3://bucket/year=2024/data.parquet", result.path(0).toString());
    }

    public void testExpandGlobFullyResolvedByHintsFileMissing() throws IOException {
        StubProvider provider = new StubProvider(List.of());

        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024));
        FileList result = GlobExpander.expandGlob("s3://bucket/year=*/data.parquet", provider, hints, true);
        assertTrue(result.isEmpty());
    }

    public void testExpandGlobFullyResolvedByHintsPreservesPartitionMetadata() throws IOException {
        StubProvider provider = new StubProvider(List.of());
        provider.existingPaths.add("s3://bucket/year=2024/data.parquet");

        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024));
        FileList result = GlobExpander.expandGlob("s3://bucket/year=*/data.parquet", provider, hints, true);
        assertTrue(result.isResolved());
        assertNotNull(result.partitionMetadata());
        assertTrue(result.partitionMetadata().partitionColumns().containsKey("year"));
    }

    public void testExpandGlobLiteralWithoutHintsStillReturnsUnresolved() throws IOException {
        StubProvider provider = new StubProvider(List.of());
        FileList result = GlobExpander.expandGlob("s3://bucket/year=2024/data.parquet", provider, null, true);
        assertFalse(result.isResolved());
    }

    public void testExpandGlobPartiallyResolvedByHintsContinuesWithListing() throws IOException {
        List<StorageEntry> listing = List.of(
            entry("s3://bucket/year=2024/file1.parquet", 100),
            entry("s3://bucket/year=2024/file2.parquet", 200)
        );
        StubProvider provider = new StubProvider(listing);

        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024));
        FileList result = GlobExpander.expandGlob("s3://bucket/year=*/*.parquet", provider, hints, true);
        assertTrue(result.isResolved());
        assertEquals(2, result.fileCount());
    }

    // -- helpers --

    private static PartitionFilterHintExtractor.PartitionFilterHint hint(
        String column,
        PartitionFilterHintExtractor.Operator op,
        Object... values
    ) {
        return new PartitionFilterHintExtractor.PartitionFilterHint(column, op, List.of(values));
    }

    private static StorageEntry entry(String path, long length) {
        return new StorageEntry(StoragePath.of(path), length, Instant.EPOCH);
    }

    private static class StubProvider implements StorageProvider {
        private final List<StorageEntry> listing;
        private final List<String> existingPaths = new ArrayList<>();

        StubProvider(List<StorageEntry> listing) {
            this.listing = listing;
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubStorageObject(path);
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
            return new StorageIterator() {
                private final Iterator<StorageEntry> it = listing.iterator();

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
            return existingPaths.contains(path.toString());
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

        StubStorageObject(StoragePath path) {
            this(path, 0);
        }

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
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }
}
