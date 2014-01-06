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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.recycler.Recycler.V;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestCluster;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Random;

public class MockPageCacheRecycler extends PageCacheRecycler {

    private final Random random;

    @Inject
    public MockPageCacheRecycler(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool);
        final long seed = settings.getAsLong(TestCluster.SETTING_CLUSTER_NODE_SEED, 0L);
        random = new Random(seed);
    }

    @Override
    public V<byte[]> bytePage(boolean clear) {
        final V<byte[]> page = super.bytePage(clear);
        if (!clear) {
            random.nextBytes(page.v());
        }
        return page;
    }

    @Override
    public V<int[]> intPage(boolean clear) {
        final V<int[]> page = super.intPage(clear);
        if (!clear) {
            for (int i = 0; i < page.v().length; ++i) {
                page.v()[i] = random.nextInt();
            }
        }
        return page;
    }

    @Override
    public V<long[]> longPage(boolean clear) {
        final V<long[]> page = super.longPage(clear);
        if (!clear) {
            for (int i = 0; i < page.v().length; ++i) {
                page.v()[i] = random.nextLong();
            }
        }
        return page;
    }

    @Override
    public V<double[]> doublePage(boolean clear) {
        final V<double[]> page = super.doublePage(clear);
        if (!clear) {
            for (int i = 0; i < page.v().length; ++i) {
                page.v()[i] = random.nextDouble() - 0.5;
            }
        }
        return page;
    }

}
