/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.notNullValue;

public class MemorySizeSettingsTests extends ESTestCase {

    public void testPageCacheLimitHeapSetting() {
        assertMemorySizeSetting(
            PageCacheRecycler.LIMIT_HEAP_SETTING,
            "cache.recycler.page.limit.heap",
            ByteSizeValue.ofBytes((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1))
        );
    }

    public void testIndexBufferSizeSetting() {
        assertMemorySizeSetting(
            IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING,
            "indices.memory.index_buffer_size",
            ByteSizeValue.ofBytes((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1))
        );
    }

    public void testQueryCacheSizeSetting() {
        assertMemorySizeSetting(
            IndicesQueryCache.INDICES_CACHE_QUERY_SIZE_SETTING,
            "indices.queries.cache.size",
            ByteSizeValue.ofBytes((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.1))
        );
    }

    public void testIndicesRequestCacheSetting() {
        assertMemorySizeSetting(
            IndicesRequestCache.INDICES_CACHE_QUERY_SIZE,
            "indices.requests.cache.size",
            ByteSizeValue.ofBytes((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.01))
        );
    }

    public void testCircuitBreakerSettings() {
        // default is chosen based on actual heap size
        double defaultTotalPercentage;
        if (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() < new ByteSizeValue(1, ByteSizeUnit.GB).getBytes()) {
            defaultTotalPercentage = 0.95d;
        } else {
            defaultTotalPercentage = 0.7d;
        }
        assertMemorySizeSetting(
            HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING,
            "indices.breaker.total.limit",
            ByteSizeValue.ofBytes((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * defaultTotalPercentage))
        );
        assertMemorySizeSetting(
            HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING,
            "indices.breaker.fielddata.limit",
            ByteSizeValue.ofBytes((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.4))
        );
        assertMemorySizeSetting(
            HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING,
            "indices.breaker.request.limit",
            ByteSizeValue.ofBytes((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.6))
        );
        assertMemorySizeSetting(
            HierarchyCircuitBreakerService.IN_FLIGHT_REQUESTS_CIRCUIT_BREAKER_LIMIT_SETTING,
            "network.breaker.inflight_requests.limit",
            ByteSizeValue.ofBytes((JvmInfo.jvmInfo().getMem().getHeapMax().getBytes()))
        );
    }

    public void testIndicesFieldDataCacheSetting() {
        assertMemorySizeSetting(
            IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY,
            "indices.fielddata.cache.size",
            ByteSizeValue.ofBytes(-1)
        );
    }

    private void assertMemorySizeSetting(Setting<ByteSizeValue> setting, String settingKey, ByteSizeValue defaultValue) {
        assertThat(setting, notNullValue());
        assertThat(setting.getKey(), equalTo(settingKey));
        assertThat(setting.getProperties(), hasItem(Property.NodeScope));
        assertThat(setting.getDefault(Settings.EMPTY), equalTo(defaultValue));
        Settings settingWithPercentage = Settings.builder().put(settingKey, "25%").build();
        assertThat(
            setting.get(settingWithPercentage),
            equalTo(ByteSizeValue.ofBytes((long) (JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() * 0.25)))
        );
        Settings settingWithBytesValue = Settings.builder().put(settingKey, "1024b").build();
        assertThat(setting.get(settingWithBytesValue), equalTo(ByteSizeValue.ofBytes(1024)));
    }

}
