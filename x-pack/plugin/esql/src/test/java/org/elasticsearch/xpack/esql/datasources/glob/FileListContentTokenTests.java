/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Locks the {@link FileList} content-token contract the dataset-level aggregate cache key depends on:
 * the token is a pure function of the file SET (order-independent), and ANY membership or per-file
 * (mtime, size) change produces a different token — which is exactly what makes a token-derived cache
 * key correct-or-miss with no invalidation protocol.
 */
public class FileListContentTokenTests extends ESTestCase {

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

    public void testTokenIsOrderIndependent() {
        List<StorageEntry> entries = sampleEntries(20);
        FileList inOrder = GlobExpander.fileListOf(entries, "s3://bucket/data/*.ndjson");
        List<StorageEntry> shuffled = new ArrayList<>(entries);
        Collections.shuffle(shuffled, new Random(randomLong()));
        FileList outOfOrder = GlobExpander.fileListOf(shuffled, "s3://bucket/data/*.ndjson");

        assertNotNull(inOrder.contentToken());
        assertNotNull(outOfOrder.contentToken());
        assertEquals(inOrder.contentToken(), outOfOrder.contentToken());
    }

    public void testMtimeChangeChangesToken() {
        List<StorageEntry> entries = sampleEntries(5);
        FileList before = GlobExpander.fileListOf(entries, "p");
        List<StorageEntry> touched = new ArrayList<>(entries);
        StorageEntry victim = touched.get(2);
        touched.set(2, entry(victim.path().toString(), victim.length(), victim.lastModified().toEpochMilli() + 1));
        FileList after = GlobExpander.fileListOf(touched, "p");
        assertTokensDiffer(before, after);
    }

    public void testSizeChangeChangesToken() {
        List<StorageEntry> entries = sampleEntries(5);
        FileList before = GlobExpander.fileListOf(entries, "p");
        List<StorageEntry> touched = new ArrayList<>(entries);
        StorageEntry victim = touched.get(4);
        touched.set(4, entry(victim.path().toString(), victim.length() + 1, victim.lastModified().toEpochMilli()));
        FileList after = GlobExpander.fileListOf(touched, "p");
        assertTokensDiffer(before, after);
    }

    public void testAddedFileChangesToken() {
        List<StorageEntry> entries = sampleEntries(5);
        FileList before = GlobExpander.fileListOf(entries, "p");
        List<StorageEntry> grown = new ArrayList<>(entries);
        grown.add(entry("s3://bucket/data/part-99.ndjson", 7L, 9L));
        FileList after = GlobExpander.fileListOf(grown, "p");
        assertTokensDiffer(before, after);
    }

    public void testRemovedFileChangesToken() {
        List<StorageEntry> entries = sampleEntries(5);
        FileList before = GlobExpander.fileListOf(entries, "p");
        List<StorageEntry> shrunk = new ArrayList<>(entries);
        shrunk.remove(1);
        FileList after = GlobExpander.fileListOf(shrunk, "p");
        assertTokensDiffer(before, after);
    }

    public void testCompactionPreservesToken() {
        // Dictionary compaction preserves the file set exactly, so the pass-through token must equal
        // the raw list's — the dataset-aggregate key must not depend on which representation the
        // listing cache happens to hold.
        FileList raw = GlobExpander.fileListOf(sampleEntries(10), "s3://bucket/data/*.ndjson");
        FileList compact = GlobExpander.compact(raw, "s3://bucket/data/");
        assertNotSame(raw, compact);
        assertNotNull(compact.contentToken());
        assertEquals(raw.contentToken(), compact.contentToken());
    }

    public void testSentinelsCarryNoToken() {
        assertNull(FileList.UNRESOLVED.contentToken());
        assertNull(FileList.EMPTY.contentToken());
    }

    private static void assertTokensDiffer(FileList a, FileList b) {
        ContentToken tokenA = a.contentToken();
        ContentToken tokenB = b.contentToken();
        assertNotNull(tokenA);
        assertNotNull(tokenB);
        // Both 64-bit lanes flipping on a single-field change is the whole point of the per-lane
        // perturbation; a same-high collision here would mean the mix dropped a lane.
        assertFalse(tokenA.high() == tokenB.high() && tokenA.low() == tokenB.low());
    }
}
