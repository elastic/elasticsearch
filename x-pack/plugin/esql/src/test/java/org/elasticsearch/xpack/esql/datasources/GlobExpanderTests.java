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
import java.util.HashMap;
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

    /**
     * A single-digit IN value must match both its bare and zero-padded folder spelling (6 → month=6 or month=06),
     * or a query like {@code month IN (6, 11)} silently drops the zero-padded month=06 while month=11 still matches.
     * Multi-digit values (11, years) are emitted unchanged.
     */
    public void testRewriteGlobWithInHintEmitsZeroPaddedSpellings() {
        var hints = List.of(hint("month", PartitionFilterHintExtractor.Operator.IN, 6, 11));
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/month=*/*.parquet", hints);
        assertEquals("s3://bucket/month={6,06,11}/*.parquet", rewritten);
    }

    /**
     * An IN value that contains a brace delimiter ({@code ,} or <code>&#125;</code>) cannot be expressed as a glob
     * brace alternative — the parser would split it and drop the folder that literally contains the delimiter. The
     * rewrite must be vetoed, leaving the wildcard so the full glob is listed (a superset) and the row filter narrows.
     */
    public void testRewriteGlobWithInHintVetoedWhenValueHoldsBraceDelimiter() {
        var hints = List.of(hint("region", PartitionFilterHintExtractor.Operator.IN, "a,b", "c"));
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/region=*/*.parquet", hints);
        assertEquals("s3://bucket/region=*/*.parquet", rewritten);
    }

    /**
     * An IN value with characters Hive/Spark percent-escape in folder names (`:` `/` etc.) must emit both the bare and
     * escaped spelling — the on-disk folder is `category=ns%3Aclick`, so a rewrite to bare `ns:click` would miss it.
     */
    public void testRewriteGlobWithInHintEmitsHiveEscapedSpellings() {
        var hints = List.of(hint("category", PartitionFilterHintExtractor.Operator.IN, "login", "ns:click"));
        String rewritten = GlobExpander.rewriteGlobWithHints("s3://bucket/category=*/*.parquet", hints);
        assertThat(rewritten, containsString("ns:click"));
        assertThat(rewritten, containsString("ns%3Aclick"));
        assertThat(rewritten, containsString("login"));
    }

    /**
     * End-to-end: an IN over a Hive-escaped partition folder lists it. Folders `category=login/` and the escaped
     * `category=ns%3Aclick/` (Spark's on-disk spelling of value `ns:click`); `WHERE category IN ("login","ns:click")`
     * must list both, not silently drop the escaped one. Red before the escape-spelling fix (esql-planning#1176).
     */
    public void testInListMatchesHiveEscapedFolder() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of(
                "s3://bucket/data/",
                List.of(
                    entry("s3://bucket/data/category=login/a.parquet", 100),
                    entry("s3://bucket/data/category=ns%3Aclick/b.parquet", 200)
                )
            )
        );

        var hints = List.of(hint("category", PartitionFilterHintExtractor.Operator.IN, "login", "ns:click"));
        FileList result = GlobExpander.expand("s3://bucket/data/category=*/*.parquet", provider, hints, true, MAX, MAX);

        List<String> paths = new ArrayList<>();
        for (int i = 0; i < result.fileCount(); i++) {
            paths.add(result.path(i).toString());
        }
        assertEquals("the escaped category=ns%3Aclick folder must be listed alongside category=login", 2, result.fileCount());
        assertTrue(paths.contains("s3://bucket/data/category=ns%3Aclick/b.parquet"));
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
        // Single-digit IN values carry their zero-padded spelling too, so month=01/02/03 folders match.
        assertEquals("s3://bucket/year=2024/month={1,01,2,02,3,03}/*.parquet", rewritten);
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
        // Second wildcard maps to {month} → rewritten to {1,01,2,02} so zero-padded folders match too
        assertEquals("s3://bucket/*/{1,01,2,02}/*.parquet", rewritten);
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

    // -- hints that prune the listing to nothing fall back to the un-hinted listing --

    /**
     * The rewritten glob names a folder that does not exist. Listing a superset of the files is always correct —
     * the row filter still runs — so the expansion must fall back to the original glob rather than report that the
     * pattern matched nothing, which the resolver turns into an error.
     */
    public void testHintPruningListingToEmptyFallsBackToUnhintedListing() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of("s3://bucket/data/", List.of(entry("s3://bucket/data/year=2024/a.parquet", 100)))
        );

        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2099));
        FileList result = GlobExpander.expand("s3://bucket/data/year=*/*.parquet", provider, hints, true, MAX, MAX);

        assertEquals(1, result.fileCount());
        assertEquals("s3://bucket/data/year=2024/a.parquet", result.path(0).toString());
    }

    /**
     * The trap this fix exists to avoid. {@code rewriteSegment} spells the hint's value with {@code String.valueOf},
     * so {@code WHERE month == 6} narrows the glob to {@code month=6} — but Hive writes a zero-padded {@code month=06}.
     * Reporting "matched no files" (or empty) there would turn an ordinary dataset into silent zero rows.
     */
    public void testZeroPaddedFolderAgainstUnpaddedHintFallsBack() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of("s3://bucket/data/", List.of(entry("s3://bucket/data/month=06/a.parquet", 100)))
        );

        var hints = List.of(hint("month", PartitionFilterHintExtractor.Operator.EQUALS, 6));
        FileList result = GlobExpander.expand("s3://bucket/data/month=*/*.parquet", provider, hints, true, MAX, MAX);

        assertEquals("the zero-padded folder must still be listed", 1, result.fileCount());
        assertEquals("s3://bucket/data/month=06/a.parquet", result.path(0).toString());
    }

    /**
     * The local filesystem provider throws when a directory does not exist, where an object store lists the missing
     * prefix as empty. A hint that narrows the prefix to a folder that was never created must behave the same on both.
     */
    public void testHintNarrowedPrefixThatThrowsFallsBackToUnhintedListing() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of("s3://bucket/data/", List.of(entry("s3://bucket/data/year=2024/a.parquet", 100)))
        );
        provider.throwOnUnknownPrefix = true;

        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2099));
        FileList result = GlobExpander.expand("s3://bucket/data/year=*/*.parquet", provider, hints, true, MAX, MAX);

        assertEquals(1, result.fileCount());
    }

    /**
     * The {@code _file.*} filters are exact — an all-pruned result is genuinely zero rows, with no spelling to
     * disambiguate — so an all-pruned listing is retained (the resolver needs an anchor and the row filter still
     * yields zero rows) rather than re-listed. The listing must not be recomputed a second time.
     */
    public void testFileMetadataPruneToEmptyRetainsAnchorWithoutRelisting() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of("s3://bucket/data/", List.of(entry("s3://bucket/data/a.parquet", 100), entry("s3://bucket/data/b.parquet", 200)))
        );

        var hints = List.of(hint(FileMetadataColumns.NAME, PartitionFilterHintExtractor.Operator.EQUALS, "nope.parquet"));
        FileList result = GlobExpander.expand("s3://bucket/data/*.parquet", provider, hints, true, MAX, MAX);

        assertEquals(2, result.fileCount());
        assertEquals("an exact _file.* prune is retained in one listing pass, not re-listed", 1, provider.listCallCount);
    }

    /**
     * A comma segment a rewrite narrows to empty must fall back on its own, not be masked by another segment that
     * still matches. Without per-segment fallback the {@code a/month=06} files are silently dropped while {@code b/}
     * keeps the aggregate non-empty.
     */
    public void testCommaSegmentRewrittenToEmptyFallsBackForThatSegment() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of(
                "s3://bucket/a/",
                List.of(entry("s3://bucket/a/month=06/x.parquet", 100)),
                "s3://bucket/b/",
                List.of(entry("s3://bucket/b/y.parquet", 200))
            )
        );

        var hints = List.of(hint("month", PartitionFilterHintExtractor.Operator.EQUALS, 6));
        FileList result = GlobExpander.expand("s3://bucket/a/month=*/*.parquet,s3://bucket/b/*.parquet", provider, hints, true, MAX, MAX);

        List<String> paths = new ArrayList<>();
        for (int i = 0; i < result.fileCount(); i++) {
            paths.add(result.path(i).toString());
        }
        assertEquals("the pruned segment's zero-padded file must survive alongside the other segment", 2, result.fileCount());
        assertTrue(paths.contains("s3://bucket/a/month=06/x.parquet"));
        assertTrue(paths.contains("s3://bucket/b/y.parquet"));
    }

    /**
     * A single glob with an IN-list mixing single- and multi-digit values on a zero-padded partition. `month=11`
     * matches the rewrite so the listing is non-empty and the empty-fallback never fires; without the zero-padded
     * spelling in the brace, `month=06` is silently dropped even though the row filter would keep it. (Same wrong-data
     * class as the human-reviewed comma bug, single-glob form; esql-planning#1176.)
     */
    public void testInListMixedDigitValuesMatchZeroPaddedFolders() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of(
                "s3://bucket/data/",
                List.of(entry("s3://bucket/data/month=06/a.parquet", 100), entry("s3://bucket/data/month=11/b.parquet", 200))
            )
        );

        var hints = List.of(hint("month", PartitionFilterHintExtractor.Operator.IN, 6, 11));
        FileList result = GlobExpander.expand("s3://bucket/data/month=*/*.parquet", provider, hints, true, MAX, MAX);

        List<String> paths = new ArrayList<>();
        for (int i = 0; i < result.fileCount(); i++) {
            paths.add(result.path(i).toString());
        }
        assertEquals("the zero-padded month=06 must be listed alongside month=11", 2, result.fileCount());
        assertTrue(paths.contains("s3://bucket/data/month=06/a.parquet"));
        assertTrue(paths.contains("s3://bucket/data/month=11/b.parquet"));
    }

    /** The local-filesystem flavor of the per-segment fallback: the rewritten segment prefix throws. */
    public void testCommaSegmentRewrittenPrefixThatThrowsFallsBack() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of(
                "s3://bucket/a/",
                List.of(entry("s3://bucket/a/month=06/x.parquet", 100)),
                "s3://bucket/b/",
                List.of(entry("s3://bucket/b/y.parquet", 200))
            )
        );
        provider.throwOnUnknownPrefix = true;

        var hints = List.of(hint("month", PartitionFilterHintExtractor.Operator.EQUALS, 6));
        FileList result = GlobExpander.expand("s3://bucket/a/month=*/*.parquet,s3://bucket/b/*.parquet", provider, hints, true, MAX, MAX);

        assertEquals(2, result.fileCount());
    }

    /**
     * The rewrite-channel fallback drops the rewrite but must keep the exact {@code _file.*} filters, or the fallback
     * over-lists. Here {@code month == 6} empties the rewritten glob; the fallback re-lists {@code month=*} but the
     * {@code _file.size > 100} filter must still exclude the small file.
     */
    public void testRewriteFallbackKeepsFileMetadataFilter() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of(
                "s3://bucket/data/",
                List.of(
                    entry("s3://bucket/data/month=06/big.parquet", 200),
                    entry("s3://bucket/data/month=06/small.parquet", 10),
                    entry("s3://bucket/data/month=07/other.parquet", 300)
                )
            )
        );

        var hints = List.of(
            hint("month", PartitionFilterHintExtractor.Operator.EQUALS, 6),
            hint(FileMetadataColumns.SIZE, PartitionFilterHintExtractor.Operator.GREATER_THAN, 100)
        );
        FileList result = GlobExpander.expand("s3://bucket/data/month=*/*.parquet", provider, hints, true, MAX, MAX);

        List<String> paths = new ArrayList<>();
        for (int i = 0; i < result.fileCount(); i++) {
            paths.add(result.path(i).toString());
        }
        assertEquals("the fallback keeps the size filter, dropping the small file", 2, result.fileCount());
        assertTrue(paths.contains("s3://bucket/data/month=06/big.parquet"));
        assertTrue(paths.contains("s3://bucket/data/month=07/other.parquet"));
    }

    /**
     * A query whose rewrite AND file filter both exclude everything still leaves an anchor: the rewrite fallback
     * re-lists {@code month=*}, the exact size filter prunes it to empty, and retention keeps the file so the
     * resolver never sees zero files (the row filter yields zero rows downstream).
     */
    public void testComposedPruneToEmptyRetainsAnchor() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of("s3://bucket/data/", List.of(entry("s3://bucket/data/month=06/small.parquet", 10)))
        );

        var hints = List.of(
            hint("month", PartitionFilterHintExtractor.Operator.EQUALS, 6),
            hint(FileMetadataColumns.SIZE, PartitionFilterHintExtractor.Operator.GREATER_THAN, 1_000_000)
        );
        FileList result = GlobExpander.expand("s3://bucket/data/month=*/*.parquet", provider, hints, true, MAX, MAX);

        assertTrue(result.isResolved());
        assertEquals(1, result.fileCount());
        assertEquals("s3://bucket/data/month=06/small.parquet", result.path(0).toString());
    }

    /** A pattern that genuinely matches nothing still comes back empty — the caller's loud error is preserved. */
    public void testUnhintedListingAlsoEmptyStaysEmpty() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(Map.of("s3://bucket/data/", List.of()));

        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2099));
        FileList result = GlobExpander.expand("s3://bucket/data/year=*/*.parquet", provider, hints, true, MAX, MAX);

        assertTrue(result.isResolved());
        assertEquals(0, result.fileCount());
    }

    /** A hint that narrows nothing must not trigger a second listing pass. */
    public void testUnhintedEmptyListingIsNotRetried() throws IOException {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(Map.of("s3://bucket/data/", List.of()));

        FileList result = GlobExpander.expand("s3://bucket/data/*.parquet", provider, null, true, MAX, MAX);

        assertEquals(0, result.fileCount());
        assertEquals("no hints, so no fallback listing", 1, provider.listCallCount);
    }

    /**
     * The rewrite fallback re-lists the full glob to tell a spelling-miss from a genuinely empty partition. If that
     * full listing exceeds {@code max_discovered_files} the discovery cap fires — the same error the un-filtered
     * query would raise. That cap error is preserved deliberately; deciding the two cases needs the full listing.
     */
    public void testRewriteFallbackBeyondDiscoveryCapKeepsCapError() {
        PrefixAwareStubProvider provider = new PrefixAwareStubProvider(
            Map.of(
                "s3://bucket/data/",
                List.of(
                    entry("s3://bucket/data/year=2024/a.parquet", 100),
                    entry("s3://bucket/data/year=2024/b.parquet", 200),
                    entry("s3://bucket/data/year=2024/c.parquet", 300)
                )
            )
        );

        var hints = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2099));
        var e = expectThrows(
            QlIllegalArgumentException.class,
            () -> GlobExpander.expand("s3://bucket/data/year=*/*.parquet", provider, hints, true, 2, MAX)
        );
        assertThat(e.getMessage(), containsString("discovered too many files"));
    }

    // -- listing cache discriminator --

    /**
     * The property the listing cache key rests on: equal discriminators must mean equal listings. Hints reach the
     * listing through the glob rewrite and the {@code _file.*} filters, and nothing else. This catches a new listing
     * channel added to the expansion but not the discriminator — <b>only for the hint shapes below</b>, so extend
     * {@code hintSets} whenever a new channel or hint kind is introduced, or it can slip through.
     */
    public void testDiscriminatorDeterminesTheListing() throws IOException {
        Map<String, List<StorageEntry>> tree = Map.of(
            "s3://bucket/data/",
            List.of(entry("s3://bucket/data/year=2024/a.parquet", 100), entry("s3://bucket/data/year=2025/b.parquet", 200)),
            "s3://bucket/data/year=2024/",
            List.of(entry("s3://bucket/data/year=2024/a.parquet", 100)),
            "s3://bucket/data/year=2025/",
            List.of(entry("s3://bucket/data/year=2025/b.parquet", 200))
        );
        String pattern = "s3://bucket/data/year=*/*.parquet";

        List<List<PartitionFilterHintExtractor.PartitionFilterHint>> hintSets = List.of(
            List.of(),
            List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024)),
            List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2025)),
            List.of(hint("region", PartitionFilterHintExtractor.Operator.EQUALS, "us")),
            List.of(hint(FileMetadataColumns.NAME, PartitionFilterHintExtractor.Operator.EQUALS, "a.parquet")),
            List.of(hint(FileMetadataColumns.SIZE, PartitionFilterHintExtractor.Operator.GREATER_THAN, 150))
        );

        Map<String, List<String>> listingByDiscriminator = new HashMap<>();
        for (var hints : hintSets) {
            for (boolean hive : List.of(true, false)) {
                String discriminator = GlobExpander.listingCacheDiscriminator(pattern, hints, hive);
                List<String> files = new ArrayList<>();
                FileList expanded = GlobExpander.expand(pattern, new PrefixAwareStubProvider(tree), hints, hive, MAX, MAX);
                for (int i = 0; i < expanded.fileCount(); i++) {
                    files.add(expanded.path(i).toString());
                }
                List<String> previous = listingByDiscriminator.putIfAbsent(discriminator, files);
                if (previous != null) {
                    assertEquals("same discriminator must mean the same listing", previous, files);
                }
            }
        }
    }

    /**
     * A hint on an ordinary data column reaches neither channel, so it must leave the discriminator alone —
     * otherwise every distinct WHERE literal would get its own cache entry and the listing cache would stop
     * hitting for filtered queries.
     */
    public void testDiscriminatorIgnoresHintsThatCannotNarrowTheListing() {
        String pattern = "s3://bucket/data/*.parquet";
        String unhinted = GlobExpander.listingCacheDiscriminator(pattern, null, true);

        var dataColumnHint = List.of(hint("user_id", PartitionFilterHintExtractor.Operator.EQUALS, 42));
        assertEquals(unhinted, GlobExpander.listingCacheDiscriminator(pattern, dataColumnHint, true));

        // A `year=*` segment is absent from this pattern, so even a rewritable hint cannot narrow it.
        var absentPartitionHint = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024));
        assertEquals(unhinted, GlobExpander.listingCacheDiscriminator(pattern, absentPartitionHint, true));
    }

    /** Every input that changes the listing must change the discriminator. */
    public void testDiscriminatorSeparatesHintsThatNarrowTheListing() {
        String keyed = "s3://bucket/data/year=*/*.parquet";
        String plain = "s3://bucket/data/*.parquet";
        String unhintedKeyed = GlobExpander.listingCacheDiscriminator(keyed, null, true);

        var year2024 = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024));
        var year2025 = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2025));
        assertNotEquals(unhintedKeyed, GlobExpander.listingCacheDiscriminator(keyed, year2024, true));
        assertNotEquals(
            GlobExpander.listingCacheDiscriminator(keyed, year2024, true),
            GlobExpander.listingCacheDiscriminator(keyed, year2025, true)
        );

        // hive_partitioning gates the rewrite and selects the partition metadata carried by the cached listing.
        assertNotEquals(unhintedKeyed, GlobExpander.listingCacheDiscriminator(keyed, null, false));

        var fileName = List.of(hint(FileMetadataColumns.NAME, PartitionFilterHintExtractor.Operator.EQUALS, "a.parquet"));
        assertNotEquals(
            GlobExpander.listingCacheDiscriminator(plain, null, true),
            GlobExpander.listingCacheDiscriminator(plain, fileName, true)
        );

        // A value's type is part of its identity: _file.name == "6" and _file.name == 6 do not filter alike.
        var nameSix = List.of(hint(FileMetadataColumns.NAME, PartitionFilterHintExtractor.Operator.EQUALS, "6"));
        var nameSixInt = List.of(hint(FileMetadataColumns.NAME, PartitionFilterHintExtractor.Operator.EQUALS, 6));
        assertNotEquals(
            GlobExpander.listingCacheDiscriminator(plain, nameSix, true),
            GlobExpander.listingCacheDiscriminator(plain, nameSixInt, true)
        );
    }

    /** In a comma-separated list only the pattern segments are rewritten, so only they may move the discriminator. */
    public void testDiscriminatorHandlesCommaSeparatedPaths() {
        String paths = "s3://bucket/a/year=*/*.parquet,s3://bucket/b/plain.parquet";
        var year2024 = List.of(hint("year", PartitionFilterHintExtractor.Operator.EQUALS, 2024));

        String unhinted = GlobExpander.listingCacheDiscriminator(paths, null, true);
        String hinted = GlobExpander.listingCacheDiscriminator(paths, year2024, true);

        assertNotEquals(unhinted, hinted);
        assertThat(hinted, containsString("s3://bucket/a/year=2024/*.parquet"));
        assertThat(hinted, containsString("s3://bucket/b/plain.parquet"));
    }

    /**
     * Filter literals carry arbitrary characters, including whatever delimiters the encoding uses. Two distinct
     * filters must never encode to the same discriminator — a collision would serve one filter's narrowed listing to
     * the other. Exercises values holding the control characters a naive separator-joined encoding would break on.
     */
    public void testDiscriminatorDoesNotCollideOnValuesContainingDelimiters() {
        String pattern = "s3://bucket/data/*.parquet";

        // A single value that splices in what would be a second value under separator-joining.
        var oneSplicedValue = List.of(
            hint(FileMetadataColumns.NAME, PartitionFilterHintExtractor.Operator.IN, "a\u0001java.lang.String:b")
        );
        var twoValues = List.of(hint(FileMetadataColumns.NAME, PartitionFilterHintExtractor.Operator.IN, "a", "b"));
        assertNotEquals(
            GlobExpander.listingCacheDiscriminator(pattern, oneSplicedValue, true),
            GlobExpander.listingCacheDiscriminator(pattern, twoValues, true)
        );

        // A value carrying the top-level joiner must not fake the pattern/hints boundary.
        var nullByteValue = List.of(hint(FileMetadataColumns.NAME, PartitionFilterHintExtractor.Operator.EQUALS, "x\u0000y"));
        var plainValue = List.of(hint(FileMetadataColumns.NAME, PartitionFilterHintExtractor.Operator.EQUALS, "xy"));
        assertNotEquals(
            GlobExpander.listingCacheDiscriminator(pattern, nullByteValue, true),
            GlobExpander.listingCacheDiscriminator(pattern, plainValue, true)
        );
    }

    // -- helpers --

    private static final int MAX = Integer.MAX_VALUE;

    private static PartitionFilterHintExtractor.PartitionFilterHint hint(
        String column,
        PartitionFilterHintExtractor.Operator op,
        Object... values
    ) {
        return new PartitionFilterHintExtractor.PartitionFilterHint(column, op, List.of(values));
    }

    /**
     * Lists only the entries under the requested prefix, as a real provider does. {@link StubProvider} returns its
     * whole listing whatever the prefix, which makes a glob narrowed onto a missing folder indistinguishable from an
     * un-narrowed one — the reason the listing layer's pruning bugs never surfaced in these tests.
     *
     * <p>{@code throwOnUnknownPrefix} models {@code LocalStorageProvider}, which throws on a missing directory where
     * an object store returns an empty listing.
     */
    private static class PrefixAwareStubProvider implements StorageProvider {
        private final Map<String, List<StorageEntry>> listingsByPrefix;
        boolean throwOnUnknownPrefix = false;
        int listCallCount = 0;

        PrefixAwareStubProvider(Map<String, List<StorageEntry>> listingsByPrefix) {
            this.listingsByPrefix = listingsByPrefix;
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            return new StubStorageObject(path, 0, false);
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
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
            listCallCount++;
            List<StorageEntry> entries = listingsByPrefix.get(prefix.toString());
            if (entries == null) {
                if (throwOnUnknownPrefix) {
                    throw new IOException("Directory does not exist: " + prefix);
                }
                entries = List.of();
            }
            List<StorageEntry> snapshot = entries;
            return new StorageIterator() {
                private final Iterator<StorageEntry> it = snapshot.iterator();

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
            return new StubStorageObject(path, 0, existingPaths.contains(path.toString()));
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
            return length;
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

    // -- applyFileMetadataFilters --

    public void testFileMetadataFilterByModifiedTime() {
        Instant cutoff = Instant.parse("2024-06-01T00:00:00Z");
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://b/old.parquet"), 100, Instant.parse("2024-01-15T00:00:00Z")),
            new StorageEntry(StoragePath.of("s3://b/new.parquet"), 200, Instant.parse("2024-07-15T00:00:00Z")),
            new StorageEntry(StoragePath.of("s3://b/newer.parquet"), 300, Instant.parse("2024-12-01T00:00:00Z"))
        );

        var hint = new PartitionFilterHintExtractor.PartitionFilterHint(
            "_file.modified",
            PartitionFilterHintExtractor.Operator.GREATER_THAN,
            List.of(cutoff.toEpochMilli())
        );

        List<StorageEntry> filtered = GlobExpander.applyFileMetadataFilters(entries, List.of(hint));
        assertEquals(2, filtered.size());
        assertEquals("s3://b/new.parquet", filtered.get(0).path().toString());
        assertEquals("s3://b/newer.parquet", filtered.get(1).path().toString());
    }

    public void testFileMetadataFilterBySize() {
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://b/tiny.parquet"), 10, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/small.parquet"), 1000, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/big.parquet"), 1000000, Instant.EPOCH)
        );

        var hint = new PartitionFilterHintExtractor.PartitionFilterHint(
            "_file.size",
            PartitionFilterHintExtractor.Operator.GREATER_THAN_OR_EQUAL,
            List.of(1000L)
        );

        List<StorageEntry> filtered = GlobExpander.applyFileMetadataFilters(entries, List.of(hint));
        assertEquals(2, filtered.size());
        assertEquals("s3://b/small.parquet", filtered.get(0).path().toString());
        assertEquals("s3://b/big.parquet", filtered.get(1).path().toString());
    }

    public void testFileMetadataFilterByName() {
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://b/events_2024.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/events_2025.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/other.parquet"), 100, Instant.EPOCH)
        );

        var hint = new PartitionFilterHintExtractor.PartitionFilterHint(
            "_file.name",
            PartitionFilterHintExtractor.Operator.EQUALS,
            List.of("events_2024.parquet")
        );

        List<StorageEntry> filtered = GlobExpander.applyFileMetadataFilters(entries, List.of(hint));
        assertEquals(1, filtered.size());
        assertEquals("s3://b/events_2024.parquet", filtered.get(0).path().toString());
    }

    public void testFileMetadataFilterIgnoresNonFileHints() {
        List<StorageEntry> entries = List.of(new StorageEntry(StoragePath.of("s3://b/file.parquet"), 100, Instant.EPOCH));

        var hint = new PartitionFilterHintExtractor.PartitionFilterHint(
            "year",
            PartitionFilterHintExtractor.Operator.EQUALS,
            List.of(2024)
        );

        List<StorageEntry> filtered = GlobExpander.applyFileMetadataFilters(entries, List.of(hint));
        assertEquals(1, filtered.size());
    }

    public void testFileMetadataFilterNullTimestampIsConservative() {
        List<StorageEntry> entries = List.of(new StorageEntry(StoragePath.of("s3://b/file.parquet"), 100, null));

        var hint = new PartitionFilterHintExtractor.PartitionFilterHint(
            "_file.modified",
            PartitionFilterHintExtractor.Operator.GREATER_THAN,
            List.of(Instant.parse("2024-06-01T00:00:00Z").toEpochMilli())
        );

        // Null timestamp → conservative, don't filter
        List<StorageEntry> filtered = GlobExpander.applyFileMetadataFilters(entries, List.of(hint));
        assertEquals(1, filtered.size());
    }

    public void testFileMetadataFilterCombinesMultipleHints() {
        Instant cutoff = Instant.parse("2024-06-01T00:00:00Z");
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://b/old_small.parquet"), 10, Instant.parse("2024-01-01T00:00:00Z")),
            new StorageEntry(StoragePath.of("s3://b/old_big.parquet"), 1000000, Instant.parse("2024-01-01T00:00:00Z")),
            new StorageEntry(StoragePath.of("s3://b/new_small.parquet"), 10, Instant.parse("2024-07-01T00:00:00Z")),
            new StorageEntry(StoragePath.of("s3://b/new_big.parquet"), 1000000, Instant.parse("2024-07-01T00:00:00Z"))
        );

        var timeHint = new PartitionFilterHintExtractor.PartitionFilterHint(
            "_file.modified",
            PartitionFilterHintExtractor.Operator.GREATER_THAN,
            List.of(cutoff.toEpochMilli())
        );
        var sizeHint = new PartitionFilterHintExtractor.PartitionFilterHint(
            "_file.size",
            PartitionFilterHintExtractor.Operator.GREATER_THAN,
            List.of(100L)
        );

        // Both hints must match: modified > cutoff AND size > 100
        List<StorageEntry> filtered = GlobExpander.applyFileMetadataFilters(entries, List.of(timeHint, sizeHint));
        assertEquals(1, filtered.size());
        assertEquals("s3://b/new_big.parquet", filtered.get(0).path().toString());
    }

    public void testFileMetadataFilterByModifiedIn() {
        Instant t1 = Instant.parse("2024-01-15T00:00:00Z");
        Instant t2 = Instant.parse("2024-07-15T00:00:00Z");
        Instant t3 = Instant.parse("2024-12-01T00:00:00Z");
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://b/a.parquet"), 100, t1),
            new StorageEntry(StoragePath.of("s3://b/b.parquet"), 200, t2),
            new StorageEntry(StoragePath.of("s3://b/c.parquet"), 300, t3)
        );

        // IN with two timestamps — should match a.parquet (t1) and c.parquet (t3)
        var hint = new PartitionFilterHintExtractor.PartitionFilterHint(
            "_file.modified",
            PartitionFilterHintExtractor.Operator.IN,
            List.of(t1.toEpochMilli(), t3.toEpochMilli())
        );

        List<StorageEntry> filtered = GlobExpander.applyFileMetadataFilters(entries, List.of(hint));
        assertEquals(2, filtered.size());
        assertEquals("s3://b/a.parquet", filtered.get(0).path().toString());
        assertEquals("s3://b/c.parquet", filtered.get(1).path().toString());
    }

    public void testFileMetadataFilterBySizeIn() {
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://b/a.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/b.parquet"), 200, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/c.parquet"), 300, Instant.EPOCH)
        );

        var hint = new PartitionFilterHintExtractor.PartitionFilterHint(
            "_file.size",
            PartitionFilterHintExtractor.Operator.IN,
            List.of(100L, 300L)
        );

        List<StorageEntry> filtered = GlobExpander.applyFileMetadataFilters(entries, List.of(hint));
        assertEquals(2, filtered.size());
        assertEquals("s3://b/a.parquet", filtered.get(0).path().toString());
        assertEquals("s3://b/c.parquet", filtered.get(1).path().toString());
    }

    public void testFileMetadataFilterByNameIn() {
        List<StorageEntry> entries = List.of(
            new StorageEntry(StoragePath.of("s3://b/events.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/logs.parquet"), 100, Instant.EPOCH),
            new StorageEntry(StoragePath.of("s3://b/metrics.parquet"), 100, Instant.EPOCH)
        );

        var hint = new PartitionFilterHintExtractor.PartitionFilterHint(
            "_file.name",
            PartitionFilterHintExtractor.Operator.IN,
            List.of("events.parquet", "metrics.parquet")
        );

        List<StorageEntry> filtered = GlobExpander.applyFileMetadataFilters(entries, List.of(hint));
        assertEquals(2, filtered.size());
        assertEquals("s3://b/events.parquet", filtered.get(0).path().toString());
        assertEquals("s3://b/metrics.parquet", filtered.get(1).path().toString());
    }
}
