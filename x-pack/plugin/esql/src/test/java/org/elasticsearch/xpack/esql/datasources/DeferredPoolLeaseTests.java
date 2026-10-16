/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FileSourceFactory.DeferredPoolLease;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The deferred lease is the one decorator whose own behaviour is not just forwarding: it decides when the pooled
 * provider is borrowed, and what happens to calls after the lease is returned. Both are invisible to the forwarding
 * gate — a lease that borrowed eagerly, or kept serving after close, forwards every call perfectly.
 */
public class DeferredPoolLeaseTests extends ESTestCase {

    private static final StoragePath PATH = StoragePath.of("s3://bucket/file.csv");

    /**
     * A driver that is planned and never started must not pin an SDK client, so building the lease borrows nothing.
     */
    public void testBuildingTheLeaseBorrowsNothing() {
        AtomicInteger borrows = new AtomicInteger();
        new DeferredPoolLease(() -> countingProvider(borrows));

        assertEquals("the supplier ran before any storage call", 0, borrows.get());
    }

    /** The borrow happens on the first forwarded call, and only once however many calls follow. */
    public void testTheFirstCallBorrowsAndLaterCallsReuse() throws IOException {
        AtomicInteger borrows = new AtomicInteger();
        DeferredPoolLease lease = new DeferredPoolLease(() -> countingProvider(borrows));

        lease.exists(PATH);
        assertEquals(1, borrows.get());

        lease.exists(PATH);
        lease.supportedSchemes();
        assertEquals("the lease did not forward objectMetadata to the borrowed provider", 1L, lease.objectMetadata(PATH).length());
        assertEquals("each call borrowed again instead of reusing the lease", 1, borrows.get());
    }

    /** Returning an unused lease has nothing to return, so it must not borrow one on the way out. */
    public void testClosingAnUnusedLeaseBorrowsNothing() throws IOException {
        AtomicInteger borrows = new AtomicInteger();
        DeferredPoolLease lease = new DeferredPoolLease(() -> countingProvider(borrows));

        lease.close();

        assertEquals("close borrowed a provider just to release it", 0, borrows.get());
    }

    /**
     * After the lease is returned the pooled provider may be serving another query, so a late call must fail rather
     * than reach it. Checked on every forwarded method, because the check lives in the one place they all resolve
     * through and a future override could bypass it.
     */
    public void testEveryCallAfterCloseIsRefused() throws IOException {
        DeferredPoolLease lease = new DeferredPoolLease(() -> countingProvider(new AtomicInteger()));
        lease.exists(PATH);
        lease.close();

        expectThrows(IllegalStateException.class, () -> lease.exists(PATH));
        expectThrows(IllegalStateException.class, () -> lease.objectMetadata(PATH));
        expectThrows(IllegalStateException.class, () -> lease.newObject(PATH));
        expectThrows(IllegalStateException.class, () -> lease.newObject(PATH, 1L));
        expectThrows(IllegalStateException.class, () -> lease.newObject(PATH, 1L, Instant.EPOCH));
        expectThrows(IllegalStateException.class, () -> lease.listObjects(PATH, false));
        expectThrows(IllegalStateException.class, () -> lease.listChildren(PATH, 1));
        expectThrows(IllegalStateException.class, lease::supportedSchemes);
        expectThrows(IllegalStateException.class, lease::supportsStableMetadata);
        expectThrows(IllegalStateException.class, lease::listsInKeyOrder);
    }

    private static StorageProvider countingProvider(AtomicInteger borrows) {
        borrows.incrementAndGet();
        return new StorageProvider() {
            @Override
            public StorageObject newObject(StoragePath path) {
                return null;
            }

            @Override
            public StorageObject newObject(StoragePath path, long length) {
                return null;
            }

            @Override
            public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
                return null;
            }

            @Override
            public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
                return null;
            }

            @Override
            public StorageChildren listChildren(StoragePath prefix, int limit) {
                return null;
            }

            @Override
            public boolean exists(StoragePath path) {
                return true;
            }

            @Override
            public StorageEntry objectMetadata(StoragePath path) {
                return new StorageEntry(PATH, 1L, Instant.EPOCH);
            }

            @Override
            public List<String> supportedSchemes() {
                return List.of();
            }

            @Override
            public void close() {}
        };
    }
}
