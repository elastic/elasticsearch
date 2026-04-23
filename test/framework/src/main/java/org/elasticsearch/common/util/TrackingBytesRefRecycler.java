/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.junit.Assert.assertTrue;

/// A [Recycler] for [BytesRef] pages that similarly to [MockBigArrays], uses two forms of leak detection:
///
/// - Circuit-breaker accounting: each [#obtain] call charges [PageCacheRecycler#BYTE_PAGE_SIZE] bytes against the
///   [CircuitBreaker] supplied at construction time, so that [org.elasticsearch.test.InternalTestCluster#ensureEstimatedStats]
///   catches unreleased pages in integration tests.
/// - Active teardown tracking: outstanding page acquisitions are recorded in a static set. [#ensureAllPagesAreReleased]
///   fails the test if any pages were not returned, mirroring the `ACQUIRED_ARRAYS` pattern in [MockBigArrays].
public class TrackingBytesRefRecycler implements Recycler<BytesRef> {

    private static final Set<Object> ACQUIRED_PAGES = ConcurrentHashMap.newKeySet();

    private final Recycler<BytesRef> delegate;

    @Nullable
    private final CircuitBreaker breaker;

    public TrackingBytesRefRecycler(Recycler<BytesRef> delegate, @Nullable CircuitBreaker breaker) {
        this.delegate = delegate;
        this.breaker = breaker;
    }

    /// Asserts that all pages obtained through any [TrackingBytesRefRecycler] instance have been released.
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
        final Recycler.V<BytesRef> page = delegate.obtain();
        if (breaker != null) {
            breaker.addWithoutBreaking(PageCacheRecycler.BYTE_PAGE_SIZE);
        }
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
                if (breaker != null) {
                    breaker.addWithoutBreaking(-PageCacheRecycler.BYTE_PAGE_SIZE);
                }
                ACQUIRED_PAGES.remove(key);
                page.close();
            }
        };
    }

    @Override
    public int pageSize() {
        return delegate.pageSize();
    }
}
