/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LimitedBreaker;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.set.Sets;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.junit.Assert.assertTrue;

/// A test-only [BytesRefRecycler] that uses two forms of leak detection:
///
/// - Circuit-breaker accounting: each [#obtain] call charges [PageCacheRecycler#BYTE_PAGE_SIZE]
///   bytes against the supplied [org.elasticsearch.common.breaker.CircuitBreaker].
///
/// - Active teardown tracking: outstanding page acquisitions are recorded in a static map.
///   [#ensureAllPagesAreReleased], fails the test if any pages were not returned, mirroring the
///   `ACQUIRED_ARRAYS` pattern in [org.elasticsearch.common.util.MockBigArrays].
///
public class MockBytesRefRecycler extends BytesRefRecycler {

    /// Name of the dedicated test-only circuit breaker used to track page acquisitions.
    public static final String MOCK_BYTES_RECYCLER_BREAKER = "mock_bytes_recycler";

    /// Tracks byte usage for all pages obtained through any [MockBytesRefRecycler] instance.
    /// Its limit is effectively unreachable so it never trips.
    public static final LimitedBreaker TRACKING_BREAKER = new LimitedBreaker(
        MOCK_BYTES_RECYCLER_BREAKER,
        ByteSizeValue.ofBytes(Long.MAX_VALUE / 2)
    );

    private static final Set<Object> ACQUIRED_PAGES = ConcurrentHashMap.newKeySet();

    public MockBytesRefRecycler(PageCacheRecycler recycler) {
        super(recycler);
    }

    /// Asserts that all pages obtained through any [MockBytesRefRecycler] instance have been released.
    public static void ensureAllPagesAreReleased() throws Exception {
        final Set<Object> masterCopy = new HashSet<>(ACQUIRED_PAGES);
        if (masterCopy.isEmpty() == false) {
            try {
                assertBusy(() -> assertTrue(Sets.haveEmptyIntersection(masterCopy, ACQUIRED_PAGES)));
            } catch (AssertionError ex) {
                masterCopy.retainAll(ACQUIRED_PAGES);
                ACQUIRED_PAGES.removeAll(masterCopy);
                if (masterCopy.isEmpty() == false) {
                    throw new RuntimeException(masterCopy.size() + " pages have not been released");
                }
            }
        }
    }

    @Override
    public Recycler.V<BytesRef> obtain() {
        final Recycler.V<BytesRef> page = super.obtain();
        TRACKING_BREAKER.addWithoutBreaking(PageCacheRecycler.BYTE_PAGE_SIZE);
        final Object key = new Object();
        ACQUIRED_PAGES.add(key);
        return new Recycler.V<>() {
            private final AtomicReference<AssertionError> originalRelease = new AtomicReference<>();

            @Override
            public BytesRef v() {
                return page.v();
            }

            @Override
            public boolean isRecycled() {
                return page.isRecycled();
            }

            @Override
            public void close() {
                if (originalRelease.compareAndSet(null, new AssertionError()) == false) {
                    throw new IllegalStateException("Double release. Original release attached as cause", originalRelease.get());
                }
                TRACKING_BREAKER.addWithoutBreaking(-PageCacheRecycler.BYTE_PAGE_SIZE);
                ACQUIRED_PAGES.remove(key);
                page.close();
            }
        };
    }
}
