/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.recycler.Recycler.V;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/// A [PageRecycler] wrapper for tests that delegates all page allocations to an inner
/// [PageRecycler] and adds two test-time safeguards on top:
///
/// - Leak tracking: every outstanding page is recorded in a static set so that [#ensureAllPagesAreReleased] can detect
///   unreleased pages synchronously at teardown, without relying on garbage collection. This mirrors the
///   `ACQUIRED_ARRAYS` pattern in [MockBigArrays].
/// - Double-release detection: closing a page twice throws [IllegalStateException].
///
public class MockPageCacheRecycler implements PageRecycler {

    private static final Set<Object> ACQUIRED_PAGES = ConcurrentHashMap.newKeySet();

    /// Asserts that every page obtained through any [MockPageCacheRecycler] instance has been released.
    public static void ensureAllPagesAreReleased() throws Exception {
        final Set<Object> masterCopy = new HashSet<>(ACQUIRED_PAGES);
        if (masterCopy.isEmpty() == false) {
            if (ESTestCase.waitUntil(() -> Sets.haveEmptyIntersection(masterCopy, ACQUIRED_PAGES)) == false) {
                masterCopy.retainAll(ACQUIRED_PAGES);
                ACQUIRED_PAGES.removeAll(masterCopy);
                if (masterCopy.isEmpty() == false) {
                    throw new AssertionError(masterCopy.size() + " pages have not been released");
                }
            }
        }
    }

    private final PageRecycler delegate;
    private final Random random;

    /// Returns `recycler` unchanged if it is already a [MockPageCacheRecycler]; otherwise wraps
    /// it so that every page allocation is tracked and double-releases are caught.
    public static MockPageCacheRecycler wrap(PageRecycler recycler) {
        return recycler instanceof MockPageCacheRecycler mock ? mock : new MockPageCacheRecycler(recycler);
    }

    public MockPageCacheRecycler(Settings settings) {
        this(new PageCacheRecycler(settings));
    }

    private MockPageCacheRecycler(PageRecycler delegate) {
        this.delegate = delegate;
        // we always initialize with 0 here since we really only wanna have some random bytes / ints / longs
        // and given the fact that it's called concurrently it won't reproduces anyway the same order other than in a unittest
        // for the latter 0 is just fine
        random = new Random(0);
    }

    private <T> V<T> wrap(final V<T> v) {
        final Object key = new Object();
        ACQUIRED_PAGES.add(key);
        return new V<T>() {

            private final AtomicReference<AssertionError> originalRelease = new AtomicReference<>();

            @Override
            public void close() {
                if (originalRelease.compareAndSet(null, new AssertionError()) == false) {
                    throw new IllegalStateException("Double release. Original release attached as cause", originalRelease.get());
                }
                ACQUIRED_PAGES.remove(key);
                final T ref = v();
                if (ref instanceof Object[]) {
                    Arrays.fill((Object[]) ref, 0, Array.getLength(ref), null);
                } else if (ref instanceof byte[]) {
                    Arrays.fill((byte[]) ref, 0, Array.getLength(ref), (byte) random.nextInt(256));
                } else {
                    for (int i = 0; i < Array.getLength(ref); ++i) {
                        Array.set(ref, i, (byte) random.nextInt(256));
                    }
                }
                v.close();
            }

            @Override
            public T v() {
                return v.v();
            }

            @Override
            public boolean isRecycled() {
                return v.isRecycled();
            }

        };
    }

    @Override
    public V<byte[]> bytePage(boolean clear) {
        final V<byte[]> page = delegate.bytePage(clear);
        if (clear == false) {
            Arrays.fill(page.v(), 0, page.v().length, (byte) random.nextInt(1 << 8));
        }
        return wrap(page);
    }

    @Override
    public V<Object[]> objectPage() {
        return wrap(delegate.objectPage());
    }

}
