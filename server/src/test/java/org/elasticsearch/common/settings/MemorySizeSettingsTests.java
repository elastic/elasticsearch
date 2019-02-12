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

package org.elasticsearch.common.settings;

import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.IndexingMemoryController;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class MemorySizeSettingsTests extends ESTestCase {

    public void testPageCacheLimitHeapSetting() {
        assertMemorySizeSetting(PageCacheRecycler.LIMIT_HEAP_SETTING, "cache.recycler.page.limit.heap",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1)));
    }

    public void testIndexBufferSizeSetting() {
        assertMemorySizeSetting(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING, "indices.memory.index_buffer_size",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1)));
    }

    public void testQueryCacheSizeSetting() {
        assertMemorySizeSetting(IndicesQueryCache.INDICES_CACHE_QUERY_SIZE_SETTING, "indices.queries.cache.size",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1)));
    }

    public void testIndicesRequestCacheSetting() {
        assertMemorySizeSetting(IndicesRequestCache.INDICES_CACHE_QUERY_SIZE, "indices.requests.cache.size",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.01)));
    }

    public void testCircuitBreakerSettings() {
        // default is chosen based on actual heap size
        double defaultTotalPercentage;
        if (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() < new ByteSizeValue(1, ByteSizeUnit.GB).getBytes()) {
            defaultTotalPercentage = 0.95d;
        } else {
            defaultTotalPercentage = 0.7d;
        }
        assertMemorySizeSetting(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING, "indices.breaker.total.limit",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * defaultTotalPercentage)));
        assertMemorySizeSetting(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING, "indices.breaker.fielddata.limit",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.4)));
        assertMemorySizeSetting(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING, "indices.breaker.request.limit",
                new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.6)));
        assertMemorySizeSetting(HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
                "network.breaker.inflight_requests.limit", new ByteSizeValue((JvmInfo.jvmInfo().getMem().getHeapMax().getBytes())));
    }

    public void testIndicesFieldDataCacheSetting() {
        assertMemorySizeSetting(IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY, "indices.fielddata.cache.size",
                new ByteSizeValue(-1));
    }

    private void assertMemorySizeSetting(Setting<ByteSizeValue> setting, String settingKey, ByteSizeValue defaultValue) {
        assertThat(setting, notNullValue());
        assertThat(setting.getKey(), equalTo(settingKey));
        assertThat(setting.getProperties(), hasItem(Property.NodeScope));
        assertThat(setting.getDefault(Settings.EMPTY),
                equalTo(defaultValue));
        Settings settingWithPercentage = Settings.builder().put(settingKey, "25%").build();
        assertThat(setting.get(settingWithPercentage),
                equalTo(new ByteSizeValue((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.25))));
        Settings settingWithBytesValue = Settings.builder().put(settingKey, "1024b").build();
        assertThat(setting.get(settingWithBytesValue), equalTo(new ByteSizeValue(1024)));
    }

}
