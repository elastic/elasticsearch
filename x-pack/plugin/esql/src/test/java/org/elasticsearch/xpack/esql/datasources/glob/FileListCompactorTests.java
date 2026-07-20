/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.HivePartitionDetector;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.hamcrest.Matchers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Round-trip characterization of {@link FileListCompactor}.
 * <p>
 * Compaction re-encodes a listing to save memory; it must not change what the listing <em>means</em>.
 * The single invariant asserted throughout is that a compacted list is indistinguishable from the raw
 * list it came from — same file count, and for every index the same path, size and modification time.
 * The listed object key is the only thing that identifies an object in the store, so a representation
 * that cannot replay it exactly produces reads against keys that do not exist.
 * <p>
 * Fixtures route through the real {@link HivePartitionDetector} rather than hand-building
 * {@link PartitionMetadata}: hand-built metadata carries the on-disk value spelling verbatim and so
 * cannot exercise the typing and percent-decoding that the detector applies.
 */
public class FileListCompactorTests extends ESTestCase {

    /**
     * Builds a raw listing the way production does — entries first, then partition detection over
     * those entries. Passes {@code null} metadata when nothing was detected, matching
     * {@link GlobExpander}'s behaviour when hive partitioning is off or finds no partitions.
     */
    private static GenericFileList listOf(String pattern, String... keys) {
        Instant mtime = Instant.ofEpochMilli(1_700_000_000_000L);
        List<StorageEntry> entries = new ArrayList<>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            entries.add(new StorageEntry(StoragePath.of(keys[i]), 100L * (i + 1), mtime));
        }
        PartitionMetadata pm = HivePartitionDetector.INSTANCE.detect(entries, Map.of());
        return new GenericFileList(entries, pattern, pm == null || pm.isEmpty() ? null : pm);
    }

    /** Asserts the compacted list replays the raw listing exactly, and returns it for further pinning. */
    private static FileList assertRoundTrip(String basePath, GenericFileList raw) {
        FileList compact = FileListCompactor.compact(basePath, raw);
        assertEquals("file count", raw.fileCount(), compact.fileCount());
        for (int i = 0; i < raw.fileCount(); i++) {
            assertEquals("path at " + i, raw.path(i).toString(), compact.path(i).toString());
            assertEquals("size at " + i, raw.size(i), compact.size(i));
            assertEquals("mtime at " + i, raw.lastModifiedMillis(i), compact.lastModifiedMillis(i));
        }
        return compact;
    }

    private static void assertPathsDistinct(FileList list) {
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < list.fileCount(); i++) {
            String path = list.path(i).toString();
            assertTrue("duplicate reconstructed path [" + path + "] at index " + i, seen.add(path));
        }
    }

    // ------------------------------------------------------------------
    // Partition columns embedded in the pattern prefix
    // ------------------------------------------------------------------

    /**
     * The reported failure: a pattern prefix containing {@code key=value} segments is the standard AWS
     * S3 log layout. Those segments live in the base path AND are claimed as partition columns, so a
     * reconstruction that appends partition columns to the base emits them twice.
     */
    public void testPrefixEmbeddedPartitionColumns() {
        String base = "s3://b/env=prod/year=2024/";
        assertRoundTrip(base, listOf(base + "**/*.parquet", base + "month=06/part-0.parquet", base + "month=12/part-0.parquet"));
    }

    // ------------------------------------------------------------------
    // Value spelling — the detector types and decodes, reconstruction must not re-spell
    // ------------------------------------------------------------------

    /** Zero-padded month directories are the Hive writer convention; typing to an integer drops the pad. */
    public void testZeroPaddedPartitionValue() {
        String base = "s3://b/data/";
        assertRoundTrip(base, listOf(base + "**/*.parquet", base + "month=06/a.parquet", base + "month=06/b.parquet"));
    }

    /** Percent-escaped directory names are decoded on the way in and must be re-escaped on the way out. */
    public void testPercentEscapedPartitionValue() {
        String base = "s3://b/data/";
        assertRoundTrip(base, listOf(base + "**/*.parquet", base + "ts=a%3Ab/k=a%20b/f.parquet"));
    }

    /** Scientific notation types to a double, whose toString is a different string. */
    public void testScientificNotationPartitionValue() {
        String base = "s3://b/data/";
        assertRoundTrip(base, listOf(base + "**/*.parquet", base + "x=1e5/f.parquet"));
    }

    /**
     * A {@code key=value} segment whose value contains a dot is not treated as a partition column
     * ({@link HivePartitionDetector} excludes dotted segments), so the listing carries no partition
     * metadata and compacts through the segment dictionary, which replays the value verbatim. This
     * pins that the dotted-value exclusion keeps such layouts faithful without the Hive encoding.
     */
    public void testDottedPartitionValueFallsBackToDictionary() {
        String base = "s3://b/data/";
        FileList compact = assertRoundTrip(base, listOf(base + "**/*.parquet", base + "x=2.50/f.parquet"));
        assertThat(compact, Matchers.instanceOf(DictionaryFileList.class));
    }

    /** Boolean casing is normalized by typing. */
    public void testBooleanPartitionValueCasing() {
        String base = "s3://b/data/";
        assertRoundTrip(base, listOf(base + "**/*.parquet", base + "flag=True/f.parquet"));
    }

    /**
     * Two directories whose values type to the same object must stay two directories. Grouping on the
     * typed value collapses them and reconstructs both files under one spelling.
     */
    public void testMixedValueSpellingDoesNotCollapse() {
        String base = "s3://b/data/";
        FileList compact = assertRoundTrip(base, listOf(base + "**/*.parquet", base + "month=06/a.parquet", base + "month=6/b.parquet"));
        assertPathsDistinct(compact);
    }

    /** A partition key colliding with a metadata name surfaces renamed; the directory on disk did not. */
    public void testReservedPartitionKeyKeepsOnDiskName() {
        String base = "s3://b/data/";
        assertRoundTrip(base, listOf(base + "**/*.parquet", base + "_index=foo/f.parquet"));
    }

    /** The Hive null sentinel is a real directory name and must survive verbatim. */
    public void testHiveDefaultPartitionSentinel() {
        String base = "s3://b/data/";
        assertRoundTrip(
            base,
            listOf(base + "**/*.parquet", base + "year=__HIVE_DEFAULT_PARTITION__/f.parquet", base + "year=2024/f.parquet")
        );
    }

    // ------------------------------------------------------------------
    // Directory structure — not every directory is a partition
    // ------------------------------------------------------------------

    /**
     * Real CloudTrail layouts interleave partition directories with plain ones. A reconstruction built
     * only from partition columns cannot re-emit the plain directories, so distinct objects collapse
     * onto a single key.
     */
    public void testNonPartitionDirectoriesSurvive() {
        String base = "s3://b/data/";
        FileList compact = assertRoundTrip(
            base,
            listOf(base + "**/*.parquet", base + "year=2024/batch-01/f.parquet", base + "year=2024/batch-02/f.parquet")
        );
        assertPathsDistinct(compact);
    }

    /**
     * A listing sorted by key interleaves a subdirectory's files between its parent's files. Any encoding
     * that stores files grouped by directory in contiguous ranges permutes them relative to the listing.
     */
    public void testInterleavedDirectoriesKeepListingOrder() {
        String base = "s3://b/d/";
        FileList compact = assertRoundTrip(
            base,
            listOf(base + "**/*.parquet", base + "x=1/a.parquet", base + "x=1/b-01/f.parquet", base + "x=1/z.parquet")
        );
        assertThat(compact, Matchers.instanceOf(HiveFileList.class));
    }

    /** The cross-file key check is order-insensitive, so a listing may mix directory orders. */
    public void testMixedDirectoryOrder() {
        String base = "s3://b/d/";
        FileList compact = assertRoundTrip(base, listOf(base + "**/*.parquet", base + "a=1/b=2/f1.parquet", base + "b=2/a=1/f2.parquet"));
        assertPathsDistinct(compact);
    }

    // ------------------------------------------------------------------
    // Base path that is not a prefix of the listed keys
    // ------------------------------------------------------------------

    /**
     * A comma-separated resource has no glob metacharacter, so the pattern prefix is the whole comma
     * string. No listed key starts with it, and an encoding that prepends the base regardless produces
     * a key built from the entire resource specification.
     */
    public void testCommaSeparatedResourceBaseMismatch() {
        String base = "s3://b/x.parquet,s3://b/y.parquet/";
        GenericFileList raw = listOf("s3://b/x.parquet,s3://b/y.parquet", "s3://b/x.parquet", "s3://b/y.parquet");
        assertRoundTrip(base, raw);
    }

    /** Same defect on the hive rail: a base unrelated to the listed keys must not be prepended. */
    public void testAlienBaseWithPartitionsIsNotPrepended() {
        String base = "s3://other/data/";
        GenericFileList raw = listOf("s3://b/*" + "*/*.parquet", "s3://b/year=2024/f.parquet", "s3://b/year=2025/f.parquet");
        assertRoundTrip(base, raw);
    }

    // ------------------------------------------------------------------
    // Shared-extension factoring and edges
    // ------------------------------------------------------------------

    /** A leaf with an interior dot yields a compound shared extension. */
    public void testDottedLeafNames() {
        String base = "s3://b/data/";
        assertRoundTrip(base, listOf(base + "**/*.parquet", base + "year=2024/a.b.parquet", base + "year=2024/c.b.parquet"));
    }

    /** A hidden file alongside a normal one disables extension sharing. */
    public void testHiddenFileDisablesSharedExtension() {
        String base = "s3://b/data/";
        assertRoundTrip(base, listOf(base + "**/*.parquet", base + "year=2024/.hidden", base + "year=2024/f.parquet"));
    }

    /** Single-file listings skip fingerprinting; the path must still replay. */
    public void testSingleFileListing() {
        String base = "s3://b/data/";
        assertRoundTrip(base, listOf(base + "**/*.parquet", base + "month=06/f.parquet"));
    }

    /** An empty listing is returned unchanged regardless of base normalization. */
    public void testEmptyListingReturnedUnchanged() {
        GenericFileList raw = listOf("s3://b/data/*" + "*/*.parquet");
        assertSame(raw, FileListCompactor.compact("s3://b/data", raw));
    }

    /** A base without a trailing slash is normalized before stripping. */
    public void testBaseWithoutTrailingSlash() {
        assertRoundTrip(
            "s3://b/data",
            listOf("s3://b/data/*" + "*/*.parquet", "s3://b/data/month=06/f.parquet", "s3://b/data/month=12/f.parquet")
        );
    }
}
