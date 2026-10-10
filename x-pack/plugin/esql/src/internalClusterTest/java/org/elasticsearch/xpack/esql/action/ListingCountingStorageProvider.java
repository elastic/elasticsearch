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
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A local-filesystem storage provider that counts what a query asks of the store.
 * <p>
 * Listing cost is an object-store effect: the saving a bound buys is a request never issued, and wall-clock on a
 * local filesystem cannot see it — {@code LocalStorageProvider} walks the tree into a list before handing out the
 * first entry, so a bounded listing does the same work as an unbounded one. A test that timed this would measure
 * nothing. What it can see, and what actually describes the cost, is <em>how many listing passes a query issues and
 * how many keys it visits</em>, and those are what this provider records.
 * <p>
 * Everything is delegated to a real {@link LocalStorageProvider} with the scheme rewritten, so ordering, recursion
 * and entry metadata are whatever production does. The only thing added is counting — a double that answered
 * listings itself would be a second implementation of the thing under test.
 * <p>
 * Counters are static because the provider is built per node by the factory and the assertions are made from the
 * test thread. Call {@link #reset()} before the query under test.
 */
public final class ListingCountingStorageProvider implements StorageProvider {

    /** The scheme this provider serves. Datasets under test are registered as {@code countfs:///<abs-path>}. */
    public static final String SCHEME = "countfs";

    /** Listing passes issued: one increment per {@code listObjects} call, whatever it returns. */
    private static final AtomicLong LIST_OBJECT_CALLS = new AtomicLong();
    /** Keys handed out across every pass — the drain's {@code listed} counter, observed from the store side. */
    private static final AtomicLong KEYS_VISITED = new AtomicLong();
    /** Directory-granular descents, which is how the prefix fan-out and the partition walk enumerate. */
    private static final AtomicLong LIST_CHILDREN_CALLS = new AtomicLong();

    private final LocalStorageProvider delegate = new LocalStorageProvider();

    public static void reset() {
        LIST_OBJECT_CALLS.set(0);
        KEYS_VISITED.set(0);
        LIST_CHILDREN_CALLS.set(0);
    }

    public static long listingPasses() {
        return LIST_OBJECT_CALLS.get();
    }

    public static long keysVisited() {
        return KEYS_VISITED.get();
    }

    public static long childDescents() {
        return LIST_CHILDREN_CALLS.get();
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
        LIST_CHILDREN_CALLS.incrementAndGet();
        return delegate.listChildren(toFileScheme(prefix), limit);
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        return delegate.newObject(toFileScheme(path));
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        return delegate.newObject(toFileScheme(path), length);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        return delegate.newObject(toFileScheme(path), length, lastModified);
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
