/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.esql.datasource.http.local.LocalStorageProvider;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A local-filesystem storage provider that counts what a query asks of the store: how many times it lists, how
 * many keys it visits, and which objects it opens for reading.
 * <p>
 * Both halves exist for the same reason, and it is the reason every suite built on this provider counts requests
 * instead of timing anything: the cost of answering a query over external data is requests, and a clock on a local
 * filesystem cannot see them. {@link LocalStorageProvider} walks the whole tree into a list before handing out the
 * first entry, so a bounded listing does exactly the work an unbounded one does and only the kept set differs —
 * the saving a bound buys is a request never issued, which exists only against an object store. A wasted read is
 * nearly free here for the same reason. So the properties worth asserting are how many passes a query issues, how
 * many keys it visits and which objects it opens, none of which is a duration.
 * <p>
 * Everything is delegated to a real {@link LocalStorageProvider} with the scheme rewritten, so ordering, recursion
 * and entry metadata are whatever production does. The only thing added is counting — a double that answered
 * listings itself would be a second implementation of the thing under test.
 * <p>
 * Counters are static because the provider is built per node by the factory and the assertions are made from the
 * test thread. Call {@link #reset()} before the query under test.
 */
public final class CountingStorageProvider implements StorageProvider {

    /** The scheme this provider serves. Datasets under test are registered as {@code countfs:///<abs-path>}. */
    public static final String SCHEME = "countfs";

    /** Listing passes issued: one increment per {@code listObjects} call, whatever it returns. */
    private static final AtomicLong LIST_OBJECT_CALLS = new AtomicLong();
    /** Keys handed out across every pass — the drain's {@code listed} counter, observed from the store side. */
    private static final AtomicLong KEYS_VISITED = new AtomicLong();
    /** Distinct objects opened at least once. The "files scanned" figure a limit pushdown is judged on. */
    private static final Set<String> PATHS_READ = ConcurrentHashMap.newKeySet();

    private final LocalStorageProvider delegate = new LocalStorageProvider();

    public static void reset() {
        LIST_OBJECT_CALLS.set(0);
        KEYS_VISITED.set(0);
        PATHS_READ.clear();
    }

    public static long listingPasses() {
        return LIST_OBJECT_CALLS.get();
    }

    public static long keysVisited() {
        return KEYS_VISITED.get();
    }

    /** The objects a query opened, as paths, so a failure can name the files it should not have touched. */
    public static Set<String> pathsRead() {
        return Set.copyOf(PATHS_READ);
    }

    /** Rewrites {@code countfs:///x} to {@code file:///x} so the real local provider serves it. */
    private static StoragePath toFileScheme(StoragePath path) {
        return StoragePath.of("file://" + path.localPath());
    }

    /**
     * Lists in lexicographic key order, which is what makes this double faithful rather than merely local.
     * <p>
     * S3, GCS and Azure all return keys sorted, and that ordering is load-bearing for every bug in this area: date
     * partitions sort oldest-first, queries ask about recent data, so a bound applied to an unfiltered listing spends
     * itself on the oldest keys and reaches none of the matches. A local filesystem walk has no such guarantee — its
     * order comes from the directory structure — so a test built straight on it would sometimes find a match inside
     * the bound and pass for a reason that has nothing to do with the code under test. Sorting here removes that
     * luck and reproduces the object-store geometry exactly.
     */
    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        LIST_OBJECT_CALLS.incrementAndGet();
        List<StorageEntry> entries = new ArrayList<>();
        try (StorageIterator inner = delegate.listObjects(toFileScheme(prefix), recursive)) {
            while (inner.hasNext()) {
                StorageEntry entry = inner.next();
                // Hand the key back under the scheme the query asked for, or the glob will not match it.
                entries.add(
                    new StorageEntry(StoragePath.of(SCHEME + "://" + entry.path().localPath()), entry.length(), entry.lastModified())
                );
            }
        }
        entries.sort(Comparator.comparing(e -> e.path().toString()));

        Iterator<StorageEntry> ordered = entries.iterator();
        return new StorageIterator() {
            @Override
            public boolean hasNext() {
                return ordered.hasNext();
            }

            @Override
            public StorageEntry next() {
                KEYS_VISITED.incrementAndGet();
                return ordered.next();
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public StorageChildren listChildren(StoragePath prefix, int limit) throws IOException {
        return delegate.listChildren(toFileScheme(prefix), limit);
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        return counting(path, delegate.newObject(toFileScheme(path)));
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return counting(path, delegate.newObject(toFileScheme(path), length));
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        return counting(path, delegate.newObject(toFileScheme(path), length, lastModified));
    }

    /**
     * Wraps {@code object} so every byte it serves is counted against {@code path}.
     * <p>
     * Counting in {@link StorageObject#newStream} alone is enough, and that is the point: every other read method on
     * the interface is a default implemented in terms of it, so a reader cannot take bytes by a route this does not
     * see. The cost is that a provider's native {@code readBytes}/{@code readBytesAsync} is bypassed in favour of the
     * interface default over this {@code newStream} -- irrelevant on a local filesystem, and the price of a count
     * that cannot be walked around.
     */
    private static StorageObject counting(StoragePath path, StorageObject object) {
        return new StorageObject() {
            @Override
            public InputStream newStream(long position, long length) throws IOException {
                PATHS_READ.add(path.toString());
                return object.newStream(position, length);
            }

            // Not defaulted: the interface deliberately provides no default, so a decorator cannot silently fall
            // back to an identity that bypasses its delegate's credential scope.
            @Override
            public StorageIdentity storageIdentity() {
                return object.storageIdentity();
            }

            @Override
            public long length() throws IOException {
                return object.length();
            }

            @Override
            public long knownLength() {
                return object.knownLength();
            }

            @Override
            public String contentGeneration() {
                return object.contentGeneration();
            }

            @Override
            public Instant lastModified() throws IOException {
                return object.lastModified();
            }

            @Override
            public boolean exists() throws IOException {
                return object.exists();
            }

            @Override
            public StoragePath path() {
                // The path the query asked for, under this provider's scheme -- not the file:// one the delegate holds.
                return path;
            }
        };
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        return delegate.exists(toFileScheme(path));
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of(SCHEME);
    }

    /** This double sorts what it serves, so it lists in key order whatever the underlying filesystem does. */
    @Override
    public boolean listsInKeyOrder() {
        return true;
    }

    @Override
    public boolean supportsStableMetadata() {
        return delegate.supportsStableMetadata();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
