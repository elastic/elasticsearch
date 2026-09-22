/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FileFingerprint;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;
import org.elasticsearch.xpack.esql.datasources.HivePartitionDetector;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.WarningSinks;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

/**
 * Locks the {@link FileList} file-set-fingerprint contract the dataset-level aggregate cache key
 * depends on: the fingerprint is a pure function of the file SET (order-independent), and ANY
 * membership or per-file (mtime, size) change produces a different fingerprint — which is exactly what
 * makes a fingerprint-derived cache key correct-or-miss with no invalidation protocol.
 */
public class FileSetFingerprintsTests extends ESTestCase {

    private static StorageEntry entry(String path, long size, long mtimeMillis) {
        return new StorageEntry(StoragePath.of(path), size, Instant.ofEpochMilli(mtimeMillis));
    }

    private static List<StorageEntry> sampleEntries(int count) {
        List<StorageEntry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            entries.add(entry("s3://bucket/data/part-" + i + ".ndjson", 1000L + i, 5_000L + i));
        }
        return entries;
    }

    public void testFingerprintIsOrderIndependent() {
        List<StorageEntry> entries = sampleEntries(20);
        FileList inOrder = GlobExpander.fileListOf(entries, "s3://bucket/data/*.ndjson");
        List<StorageEntry> shuffled = new ArrayList<>(entries);
        Collections.shuffle(shuffled, new Random(randomLong()));
        FileList outOfOrder = GlobExpander.fileListOf(shuffled, "s3://bucket/data/*.ndjson");

        assertNotNull(inOrder.fileSetFingerprint());
        assertNotNull(outOfOrder.fileSetFingerprint());
        assertEquals(inOrder.fileSetFingerprint(), outOfOrder.fileSetFingerprint());
    }

    public void testMtimeChangeChangesFingerprint() {
        List<StorageEntry> entries = sampleEntries(5);
        FileList before = GlobExpander.fileListOf(entries, "p");
        List<StorageEntry> touched = new ArrayList<>(entries);
        StorageEntry victim = touched.get(2);
        touched.set(2, entry(victim.path().toString(), victim.length(), victim.lastModified().toEpochMilli() + 1));
        FileList after = GlobExpander.fileListOf(touched, "p");
        assertFingerprintsDiffer(before, after);
    }

    public void testSizeChangeChangesFingerprint() {
        List<StorageEntry> entries = sampleEntries(5);
        FileList before = GlobExpander.fileListOf(entries, "p");
        List<StorageEntry> touched = new ArrayList<>(entries);
        StorageEntry victim = touched.get(4);
        touched.set(4, entry(victim.path().toString(), victim.length() + 1, victim.lastModified().toEpochMilli()));
        FileList after = GlobExpander.fileListOf(touched, "p");
        assertFingerprintsDiffer(before, after);
    }

    public void testAddedFileChangesFingerprint() {
        List<StorageEntry> entries = sampleEntries(5);
        FileList before = GlobExpander.fileListOf(entries, "p");
        List<StorageEntry> grown = new ArrayList<>(entries);
        grown.add(entry("s3://bucket/data/part-99.ndjson", 7L, 9L));
        FileList after = GlobExpander.fileListOf(grown, "p");
        assertFingerprintsDiffer(before, after);
    }

    public void testRemovedFileChangesFingerprint() {
        List<StorageEntry> entries = sampleEntries(5);
        FileList before = GlobExpander.fileListOf(entries, "p");
        List<StorageEntry> shrunk = new ArrayList<>(entries);
        shrunk.remove(1);
        FileList after = GlobExpander.fileListOf(shrunk, "p");
        assertFingerprintsDiffer(before, after);
    }

    public void testCompactionPreservesFingerprint() {
        // Dictionary compaction preserves the file set exactly, so the pass-through fingerprint must
        // equal the raw list's — the dataset-aggregate key must not depend on which representation the
        // listing cache happens to hold.
        FileList raw = GlobExpander.fileListOf(sampleEntries(10), "s3://bucket/data/*.ndjson");
        FileList compact = GlobExpander.compact(raw, "s3://bucket/data/");
        assertNotSame(raw, compact);
        assertNotNull(compact.fileSetFingerprint());
        assertEquals(raw.fileSetFingerprint(), compact.fileSetFingerprint());
    }

    /**
     * A per-file identity must mean the same thing whichever representation the listing cache holds. The
     * resolver usually holds a COMPACTED listing — {@code expandAndCompact} compacts every non-truncated one — so
     * an identity only the uncompacted form could produce would be missing exactly where a cache keyed on it has
     * to be looked up, and that cache would be built, pass its tests, and never be hit on a wide glob.
     * <p>
     * Both compacted forms are exercised, and each is asserted to BE that form, so the test cannot pass by
     * compacting everything to one type and exercising only that.
     */
    public void testFileFingerprintIsIdenticalAcrossEveryListRepresentation() {
        for (FileList raw : representativeListings()) {
            FileList compact = GlobExpander.compact(raw, "s3://bucket/data/");
            assertNotSame("the listing must actually compact", raw, compact);
            for (int i = 0; i < raw.fileCount(); i++) {
                assertEquals(
                    "file " + i + " must have one identity whether the listing is " + compact.getClass().getSimpleName() + " or not",
                    raw.fileFingerprint(i),
                    compact.fileFingerprint(i)
                );
            }
        }
        assertEquals(
            "both compacted forms must be exercised",
            Set.of(DictionaryFileList.class, DirectoryGroupedFileList.class),
            representativeListings().stream().map(raw -> GlobExpander.compact(raw, "s3://bucket/data/").getClass()).collect(toSet())
        );
    }

    /**
     * Folding every file's fingerprint the way the set fingerprint is folded reproduces the set fingerprint, for
     * the raw listing and for both compacted forms. That is what makes the two agree about what "the same file"
     * means: a cache keyed on one file of a listing and a cache keyed on all of them hash the same thing.
     */
    public void testFoldOfFileFingerprintsIsTheSetFingerprint() {
        for (FileList raw : representativeListings()) {
            FileList compact = GlobExpander.compact(raw, "s3://bucket/data/");
            for (FileList listing : List.of(raw, compact)) {
                assertEquals(
                    "fold over " + listing.getClass().getSimpleName(),
                    listing.fileSetFingerprint(),
                    foldFileFingerprints(listing)
                );
            }
        }
    }

    /** Listings that compact to each of the two compacted forms. */
    private static List<FileList> representativeListings() {
        // Flat files under one prefix compact through the segment dictionary.
        FileList flat = GlobExpander.fileListOf(sampleEntries(10), "s3://bucket/data/*.ndjson");
        // A subdirectory's files interleaved between its parent's force the directory-grouped form — but only once
        // the listing carries its Hive partition metadata, as a real resolve's does. Without it the same paths
        // compact through the dictionary, which is what the both-forms assertion exists to catch.
        List<StorageEntry> interleavedEntries = List.of(
            entry("s3://bucket/data/x=1/a.parquet", 100, 5_000),
            entry("s3://bucket/data/x=1/b-01/f.parquet", 200, 5_001),
            entry("s3://bucket/data/x=1/z.parquet", 300, 5_002)
        );
        PartitionMetadata partitions = HivePartitionDetector.INSTANCE.detect(interleavedEntries, WarningSinks.FAILING);
        FileList interleaved = new GenericFileList(
            interleavedEntries,
            "s3://bucket/data/**/*.parquet",
            partitions == null || partitions.isEmpty() ? null : partitions
        );
        return List.of(flat, interleaved);
    }

    /** The set fingerprint recomputed from the per-file ones, finalized as {@link FileSetFingerprints#compute} does. */
    private static FileSetFingerprint foldFileFingerprints(FileList listing) {
        long high = 0;
        long low = 0;
        for (int i = 0; i < listing.fileCount(); i++) {
            FileFingerprint fingerprint = listing.fileFingerprint(i);
            high += fingerprint.high();
            low += fingerprint.low();
        }
        int n = listing.fileCount();
        return new FileSetFingerprint(MurmurHash3.fmix(high + n), MurmurHash3.fmix(low ^ n));
    }

    public void testSentinelsCarryNoFingerprint() {
        assertNull(FileList.UNRESOLVED.fileSetFingerprint());
        assertNull(FileList.EMPTY.fileSetFingerprint());
    }

    public void testSingleFileListingCarriesNoFingerprint() {
        // A single-file listing can never key a dataset aggregate (that needs fileCount >= 2), so the
        // Murmur3 fold is skipped and the accessor returns null — keeping the common single-file resolve
        // off the hash path. A two-file listing is the positive control.
        FileList single = GlobExpander.fileListOf(sampleEntries(1), "s3://bucket/data/part-0.ndjson");
        assertNull(single.fileSetFingerprint());
        FileList two = GlobExpander.fileListOf(sampleEntries(2), "s3://bucket/data/*.ndjson");
        assertNotNull(two.fileSetFingerprint());
    }

    private static void assertFingerprintsDiffer(FileList a, FileList b) {
        FileSetFingerprint fingerprintA = a.fileSetFingerprint();
        FileSetFingerprint fingerprintB = b.fileSetFingerprint();
        assertNotNull(fingerprintA);
        assertNotNull(fingerprintB);
        // A single-field change must perturb the fingerprint: at least one lane differs. (Both lanes flip
        // in practice via the per-lane perturbation; asserting only "not identical" avoids a spurious
        // failure if one lane's avalanche happens to coincide.)
        assertFalse(fingerprintA.high() == fingerprintB.high() && fingerprintA.low() == fingerprintB.low());
    }
}
