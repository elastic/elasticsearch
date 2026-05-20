/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.InputStream;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Defense-in-depth: directly exercise {@link ExternalRowCountCache} key semantics. The cache
 * keys files by {@code (path, length)} — same-path-different-length and same-length-different-path
 * must resolve to distinct entries. Without this isolation a file rewritten to a new size, or two
 * differently-named files of identical size, would cross-contaminate row counts.
 */
public class ExternalRowCountCacheTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ExternalRowCountCache.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        ExternalRowCountCache.clearForTests();
        super.tearDown();
    }

    public void testMissReturnsEmpty() {
        StorageObject o = obj("memory://a.csv", 100L);
        assertTrue(ExternalRowCountCache.lookup(o).isEmpty());
    }

    public void testPutThenLookupReturnsValue() {
        StorageObject o = obj("memory://a.csv", 100L);
        ExternalRowCountCache.put(o, 42L);
        OptionalLong v = ExternalRowCountCache.lookup(o);
        assertTrue(v.isPresent());
        assertEquals(42L, v.getAsLong());
    }

    public void testSamePathDifferentLengthsAreDistinct() {
        StorageObject smaller = obj("memory://same.csv", 100L);
        StorageObject larger = obj("memory://same.csv", 200L);
        ExternalRowCountCache.put(smaller, 10L);
        assertEquals(10L, ExternalRowCountCache.lookup(smaller).getAsLong());
        assertTrue("different length must not see another length's entry", ExternalRowCountCache.lookup(larger).isEmpty());
    }

    public void testSameLengthDifferentPathsAreDistinct() {
        StorageObject a = obj("memory://a.csv", 100L);
        StorageObject b = obj("memory://b.csv", 100L);
        ExternalRowCountCache.put(a, 7L);
        assertEquals(7L, ExternalRowCountCache.lookup(a).getAsLong());
        assertTrue("different path must not see another path's entry", ExternalRowCountCache.lookup(b).isEmpty());
    }

    public void testPathLengthMtimeOverloadMatchesStorageObjectLookup() throws Exception {
        StorageObject o = obj("memory://c.csv", 150L);
        ExternalRowCountCache.put(o, 13L);
        OptionalLong viaObject = ExternalRowCountCache.lookup(o);
        OptionalLong viaTriple = ExternalRowCountCache.lookup("memory://c.csv", 150L, o.lastModified().toEpochMilli());
        assertTrue(viaObject.isPresent());
        assertTrue(viaTriple.isPresent());
        assertEquals(viaObject.getAsLong(), viaTriple.getAsLong());
        // A (path, length, mtime) lookup with the wrong length is a miss.
        assertTrue(ExternalRowCountCache.lookup("memory://c.csv", 151L, o.lastModified().toEpochMilli()).isEmpty());
        // A (path, length, mtime) lookup with the wrong mtime is a miss — same-length file mutation
        // produces a fresh mtime, which is exactly what makes that case automatically invalidate.
        assertTrue(ExternalRowCountCache.lookup("memory://c.csv", 150L, o.lastModified().toEpochMilli() + 1).isEmpty());
    }

    public void testSameLengthDifferentMtimesAreDistinct() {
        // The cache's safety story for same-length file mutations: a fresh mtime is a fresh key,
        // so the previous version's count cannot leak across the mutation boundary.
        Instant tNow = Instant.now();
        Instant tLater = tNow.plusMillis(1);
        StorageObject before = objWithMtime("memory://mutated.csv", 100L, tNow);
        StorageObject after = objWithMtime("memory://mutated.csv", 100L, tLater);
        ExternalRowCountCache.put(before, 50L);
        assertEquals(50L, ExternalRowCountCache.lookup(before).getAsLong());
        assertTrue(
            "same (path, length) but different mtime must miss — proves no stale serve on same-length mutation",
            ExternalRowCountCache.lookup(after).isEmpty()
        );
    }

    public void testStorageObjectLengthIOExceptionDegradesToMiss() {
        StorageObject throwsOnLength = new StorageObject() {
            @Override
            public InputStream newStream() {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length() throws java.io.IOException {
                throw new java.io.IOException("simulated length() failure");
            }

            @Override
            public Instant lastModified() {
                return Instant.now();
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://broken.csv");
            }
        };
        // Must not propagate the IOException; degrades to a clean miss.
        assertTrue(ExternalRowCountCache.lookup(throwsOnLength).isEmpty());
    }

    private StorageObject obj(String pathStr, long lengthBytes) {
        return objWithMtime(pathStr, lengthBytes, Instant.now());
    }

    private StorageObject objWithMtime(String pathStr, long lengthBytes, Instant mtime) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream newStream(long position, long length) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long length() {
                return lengthBytes;
            }

            @Override
            public Instant lastModified() {
                return mtime;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of(pathStr);
            }
        };
    }

    @SuppressWarnings("unused")
    private static String uniquePath() {
        return "memory://" + UUID.randomUUID() + ".csv";
    }
}
