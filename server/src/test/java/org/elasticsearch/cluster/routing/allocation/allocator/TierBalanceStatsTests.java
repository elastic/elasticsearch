/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.cluster.routing.allocation.allocator.MetricStatsTests.createRandomMetricStats;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class TierBalanceStatsTests extends AbstractWireSerializingTestCase<ClusterBalanceStats.TierBalanceStats> {

    @Override
    protected Writeable.Reader<ClusterBalanceStats.TierBalanceStats> instanceReader() {
        return ClusterBalanceStats.TierBalanceStats::readFrom;
    }

    @Override
    protected ClusterBalanceStats.TierBalanceStats createTestInstance() {
        return createRandomTierBalanceStats();
    }

    private ClusterBalanceStats.TierBalanceStats createRandomTierBalanceStats() {
        return new ClusterBalanceStats.TierBalanceStats(
            createRandomMetricStats(),
            createRandomMetricStats(),
            createRandomMetricStats(),
            createRandomMetricStats(),
            createRandomMetricStats()
        );
    }

    @Override
    protected ClusterBalanceStats.TierBalanceStats mutateInstance(ClusterBalanceStats.TierBalanceStats instance) throws IOException {
        return createTestInstance();
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        ClusterBalanceStats.TierBalanceStats tierBalanceStats = createRandomTierBalanceStats();

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder = tierBalanceStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        // Convert to map for easy assertions
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        assertThat(
            map.keySet(),
            containsInAnyOrder(
                "shard_count",
                "undesired_shard_allocation_count",
                "forecast_write_load",
                "forecast_disk_usage",
                "actual_disk_usage"
            )
        );

        Map<String, Object> shardCountStats = (Map<String, Object>) map.get("shard_count");
        assertThat(shardCountStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
        assertEquals(shardCountStats.get("total"), tierBalanceStats.shardCount().total());
        assertEquals(shardCountStats.get("average"), tierBalanceStats.shardCount().average());
        assertEquals(shardCountStats.get("min"), tierBalanceStats.shardCount().min());
        assertEquals(shardCountStats.get("max"), tierBalanceStats.shardCount().max());
        assertEquals(shardCountStats.get("std_dev"), tierBalanceStats.shardCount().stdDev());

        Map<String, Object> undesiredShardAllocationCountStats = (Map<String, Object>) map.get("undesired_shard_allocation_count");
        assertThat(undesiredShardAllocationCountStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
        assertEquals(undesiredShardAllocationCountStats.get("total"), tierBalanceStats.undesiredShardAllocations().total());
        assertEquals(undesiredShardAllocationCountStats.get("average"), tierBalanceStats.undesiredShardAllocations().average());
        assertEquals(undesiredShardAllocationCountStats.get("min"), tierBalanceStats.undesiredShardAllocations().min());
        assertEquals(undesiredShardAllocationCountStats.get("max"), tierBalanceStats.undesiredShardAllocations().max());
        assertEquals(undesiredShardAllocationCountStats.get("std_dev"), tierBalanceStats.undesiredShardAllocations().stdDev());

        Map<String, Object> forecastWriteLoadStats = (Map<String, Object>) map.get("forecast_write_load");
        assertThat(forecastWriteLoadStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
        assertEquals(forecastWriteLoadStats.get("total"), tierBalanceStats.forecastWriteLoad().total());
        assertEquals(forecastWriteLoadStats.get("average"), tierBalanceStats.forecastWriteLoad().average());
        assertEquals(forecastWriteLoadStats.get("min"), tierBalanceStats.forecastWriteLoad().min());
        assertEquals(forecastWriteLoadStats.get("max"), tierBalanceStats.forecastWriteLoad().max());
        assertEquals(forecastWriteLoadStats.get("std_dev"), tierBalanceStats.forecastWriteLoad().stdDev());

        Map<String, Object> forecastDiskUsageStats = (Map<String, Object>) map.get("forecast_disk_usage");
        assertThat(forecastDiskUsageStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
        assertEquals(forecastDiskUsageStats.get("total"), tierBalanceStats.forecastShardSize().total());
        assertEquals(forecastDiskUsageStats.get("average"), tierBalanceStats.forecastShardSize().average());
        assertEquals(forecastDiskUsageStats.get("min"), tierBalanceStats.forecastShardSize().min());
        assertEquals(forecastDiskUsageStats.get("max"), tierBalanceStats.forecastShardSize().max());
        assertEquals(forecastDiskUsageStats.get("std_dev"), tierBalanceStats.forecastShardSize().stdDev());

        Map<String, Object> actualDiskUsageStats = (Map<String, Object>) map.get("actual_disk_usage");
        assertThat(actualDiskUsageStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
        assertEquals(actualDiskUsageStats.get("total"), tierBalanceStats.actualShardSize().total());
        assertEquals(actualDiskUsageStats.get("average"), tierBalanceStats.actualShardSize().average());
        assertEquals(actualDiskUsageStats.get("min"), tierBalanceStats.actualShardSize().min());
        assertEquals(actualDiskUsageStats.get("max"), tierBalanceStats.actualShardSize().max());
        assertEquals(actualDiskUsageStats.get("std_dev"), tierBalanceStats.actualShardSize().stdDev());
    }
}
