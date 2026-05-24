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

/** Key semantics for {@link ExternalRowCountCache}: (path, mtime) — fresh mtime ⇒ fresh key. */
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
        StorageObject o = obj("memory://a.csv");
        assertTrue(ExternalRowCountCache.lookup(o).isEmpty());
    }

    public void testPutThenLookupReturnsValue() {
        StorageObject o = obj("memory://a.csv");
        ExternalRowCountCache.put(o, 42L);
        OptionalLong v = ExternalRowCountCache.lookup(o);
        assertTrue(v.isPresent());
        assertEquals(42L, v.getAsLong());
    }

    public void testDifferentPathsAreDistinct() {
        StorageObject a = obj("memory://a.csv");
        StorageObject b = obj("memory://b.csv");
        ExternalRowCountCache.put(a, 7L);
        assertEquals(7L, ExternalRowCountCache.lookup(a).getAsLong());
        assertTrue("different path must not see another path's entry", ExternalRowCountCache.lookup(b).isEmpty());
    }

    public void testSamePathDifferentMtimesAreDistinct() {
        // File mutation advances mtime → fresh key → no stale serve across mutation.
        Instant tNow = Instant.now();
        Instant tLater = tNow.plusMillis(1);
        StorageObject before = objWithMtime("memory://mutated.csv", tNow);
        StorageObject after = objWithMtime("memory://mutated.csv", tLater);
        ExternalRowCountCache.put(before, 50L);
        assertEquals(50L, ExternalRowCountCache.lookup(before).getAsLong());
        assertTrue(
            "fresh mtime must produce a fresh key — no stale serve across a mutation",
            ExternalRowCountCache.lookup(after).isEmpty()
        );
    }

    public void testPathMtimeOverloadMatchesStorageObjectLookup() throws Exception {
        StorageObject o = obj("memory://c.csv");
        ExternalRowCountCache.put(o, 13L);
        long mtimeMillis = o.lastModified().toEpochMilli();
        OptionalLong viaObject = ExternalRowCountCache.lookup(o);
        OptionalLong viaPair = ExternalRowCountCache.lookup("memory://c.csv", mtimeMillis);
        assertTrue(viaObject.isPresent());
        assertTrue(viaPair.isPresent());
        assertEquals(viaObject.getAsLong(), viaPair.getAsLong());
        assertTrue(ExternalRowCountCache.lookup("memory://c.csv", mtimeMillis + 1).isEmpty());
    }

    public void testStreamOnlySourceIsStillCacheable() {
        // Stream-only sources (bzip2, zstd-streamed) throw from length() but lastModified() works — cache key is mtime.
        StorageObject streamOnly = new StorageObject() {
            private final Instant mtime = Instant.now();

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
                throw new UnsupportedOperationException("Decompressed length is unknown for stream-only compression");
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
                return StoragePath.of("memory://stream-only.csv.bz2");
            }
        };
        ExternalRowCountCache.put(streamOnly, 99L);
        OptionalLong v = ExternalRowCountCache.lookup(streamOnly);
        assertTrue("stream-only sources are cacheable: lookup must hit on the same mtime", v.isPresent());
        assertEquals(99L, v.getAsLong());
    }

    public void testNullMtimeIsNotCacheable() {
        // Null mtime ⇒ no trusted identity ⇒ cache drops both put and lookup.
        StorageObject noMtime = new StorageObject() {
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
                return 0L;
            }

            @Override
            public Instant lastModified() {
                return null;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://no-mtime");
            }
        };
        ExternalRowCountCache.put(noMtime, 5L);
        assertTrue("null-mtime sources must not produce a cache entry", ExternalRowCountCache.lookup(noMtime).isEmpty());
    }

    public void testStorageObjectLastModifiedIOExceptionDegradesToMiss() {
        StorageObject throwsOnMtime = new StorageObject() {
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
                return 0L;
            }

            @Override
            public Instant lastModified() throws java.io.IOException {
                throw new java.io.IOException("simulated lastModified() failure");
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return StoragePath.of("memory://broken-mtime");
            }
        };
        assertTrue(ExternalRowCountCache.lookup(throwsOnMtime).isEmpty());
    }

    private StorageObject obj(String pathStr) {
        return objWithMtime(pathStr, Instant.now());
    }

    private StorageObject objWithMtime(String pathStr, Instant mtime) {
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
                return 0L;
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
}
