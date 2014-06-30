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

package org.elasticsearch.common.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
 * Asserts that Guava's caches can get stuck in an overflow state where they
 * never clear themselves based on their "weight" policy if the weight grows
 * beyond MAX_INT. If the noEvictionIf* methods start failing after upgrading
 * Guava then the problem with Guava's caches can probably be considered fixed
 * and {@code ByteSizeValue#MAX_GUAVA_CACHE_SIZE} can likely be removed.
 */
public class GuavaCacheOverflowTest extends ElasticsearchTestCase {
    private final int tenMeg = ByteSizeValue.parseBytesSizeValue("10MB").bytesAsInt();

    private Cache<Integer, Object> cache;

    @Test
    public void noEvictionIfWeightMaxWeightIs32GB() {
        checkNoEviction("32GB");
    }

    @Test
    public void noEvictionIfWeightMaxWeightIsGreaterThan32GB() {
        checkNoEviction(between(33, 50) + "GB");
    }

    @Test
    public void evictionIfWeightSlowlyGoesOverMaxWeight() {
        buildCache("30GB");
        // Add about 100GB of weight to the cache
        int entries = 10240;
        fillCache(entries);

        // And as expected, some are purged.
        int missing = 0;
        for (int i = 0; i < 31; i++) {
            if (cache.getIfPresent(i + tenMeg) == null) {
                missing++;
            }
        }
        assertThat(missing, both(greaterThan(0)).and(lessThan(entries)));
    }

    private void buildCache(String size) {
        cache = CacheBuilder.newBuilder().concurrencyLevel(16).maximumWeight(ByteSizeValue.parseBytesSizeValue(size).bytes())
                .weigher(new Weigher<Integer, Object>() {
                    @Override
                    public int weigh(Integer key, Object value) {
                        return key;
                    }
                }).build();
    }

    private void fillCache(int entries) {
        for (int i = 0; i < entries; i++) {
            cache.put(i + tenMeg, i);
        }
    }

    private void checkNoEviction(String size) {
        buildCache(size);
        // Adds ~100GB worth of weight to the cache
        fillCache(10240);
        // But nothing has been purged!
        for (int i = 0; i < 10000; i++) {
            assertNotNull(cache.getIfPresent(i + tenMeg));
        }
    }
}
