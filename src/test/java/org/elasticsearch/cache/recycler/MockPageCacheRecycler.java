/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cache.recycler;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.recycler.Recycler.V;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.threadpool.ThreadPool;

import java.lang.reflect.Array;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

public class MockPageCacheRecycler extends PageCacheRecycler {

    private static final ConcurrentMap<Object, Throwable> ACQUIRED_PAGES = Maps.newConcurrentMap();

    public static void ensureAllPagesAreReleased() {
        if (ACQUIRED_PAGES.size() > 0) {
            final Throwable t = ACQUIRED_PAGES.entrySet().iterator().next().getValue();
            throw new RuntimeException(ACQUIRED_PAGES.size() + " pages have not been released", t);
        }
        ACQUIRED_PAGES.clear();
    }

    private final Random random;

    @Inject
    public MockPageCacheRecycler(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool);
        final long seed = settings.getAsLong(TestCluster.SETTING_CLUSTER_NODE_SEED, 0L);
        random = new Random(seed);
    }

    private <T> V<T> wrap(final V<T> v) {
        ACQUIRED_PAGES.put(v, new Throwable());
        return new V<T>() {

            @Override
            public boolean release() throws ElasticsearchException {
                final Throwable t = ACQUIRED_PAGES.remove(v);
                if (t == null) {
                    throw new IllegalStateException("Releasing a page that has not been acquired");
                }
                final T ref = v();
                for (int i = 0; i < Array.getLength(ref); ++i) {
                    if (ref instanceof Object[]) {
                        Array.set(ref, i, null);
                    } else {
                        Array.set(ref, i, (byte) random.nextInt(256));
                    }
                }
                return v.release();
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
        if (!clear) {
            random.nextBytes(page.v());
        }
        return wrap(page);
    }

    @Override
    public V<int[]> intPage(boolean clear) {
        final V<int[]> page = super.intPage(clear);
        if (!clear) {
            for (int i = 0; i < page.v().length; ++i) {
                page.v()[i] = random.nextInt();
            }
        }
        return wrap(page);
    }

    @Override
    public V<long[]> longPage(boolean clear) {
        final V<long[]> page = super.longPage(clear);
        if (!clear) {
            for (int i = 0; i < page.v().length; ++i) {
                page.v()[i] = random.nextLong();
            }
        }
        return wrap(page);
    }

    @Override
    public V<double[]> doublePage(boolean clear) {
        final V<double[]> page = super.doublePage(clear);
        if (!clear) {
            for (int i = 0; i < page.v().length; ++i) {
                page.v()[i] = random.nextDouble() - 0.5;
            }
        }
        return wrap(page);
    }

    @Override
    public V<Object[]> objectPage() {
        return wrap(super.objectPage());
    }

}
