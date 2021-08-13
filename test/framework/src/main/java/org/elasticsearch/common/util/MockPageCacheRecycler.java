/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.recycler.Recycler.V;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.LeakTracker;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Random;

public class MockPageCacheRecycler extends PageCacheRecycler {

    private final Random random;

    public MockPageCacheRecycler(Settings settings) {
        super(settings);
        // we always initialize with 0 here since we really only wanna have some random bytes / ints / longs
        // and given the fact that it's called concurrently it won't reproduces anyway the same order other than in a unittest
        // for the latter 0 is just fine
        random = new Random(0);
    }

    private <T> V<T> wrap(final V<T> v) {
        return new V<T>() {

            private final LeakTracker.Leak<V<T>> leak = LeakTracker.INSTANCE.track(v);

            @Override
            public void close() {
                boolean leakReleased = leak.close(v);
                assert leakReleased : "leak should not have been released already";
                final T ref = v();
                if (ref instanceof Object[]) {
                    Arrays.fill((Object[])ref, 0, Array.getLength(ref), null);
                } else if (ref instanceof byte[]) {
                    Arrays.fill((byte[])ref, 0, Array.getLength(ref), (byte) random.nextInt(256));
                } else if (ref instanceof long[]) {
                    Arrays.fill((long[])ref, 0, Array.getLength(ref), random.nextLong());
                } else if (ref instanceof int[]) {
                    Arrays.fill((int[])ref, 0, Array.getLength(ref), random.nextInt());
                } else if (ref instanceof double[]) {
                    Arrays.fill((double[])ref, 0, Array.getLength(ref), random.nextDouble() - 0.5);
                } else if (ref instanceof float[]) {
                    Arrays.fill((float[])ref, 0, Array.getLength(ref), random.nextFloat() - 0.5f);
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
        final V<byte[]> page = super.bytePage(clear);
        if (clear == false) {
            Arrays.fill(page.v(), 0, page.v().length, (byte)random.nextInt(1<<8));
        }
        return wrap(page);
    }

    @Override
    public V<int[]> intPage(boolean clear) {
        final V<int[]> page = super.intPage(clear);
        if (clear == false) {
            Arrays.fill(page.v(), 0, page.v().length, random.nextInt());
        }
        return wrap(page);
    }

    @Override
    public V<long[]> longPage(boolean clear) {
        final V<long[]> page = super.longPage(clear);
        if (clear == false) {
            Arrays.fill(page.v(), 0, page.v().length, random.nextLong());
        }
        return wrap(page);
    }

    @Override
    public V<Object[]> objectPage() {
        return wrap(super.objectPage());
    }

}
