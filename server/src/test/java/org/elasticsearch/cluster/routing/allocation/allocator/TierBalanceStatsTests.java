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
        ClusterBalanceStats.MetricStats shardCount = createRandomMetricStats();
        ClusterBalanceStats.MetricStats undesiredShardAllocations = createRandomMetricStats();
        ClusterBalanceStats.MetricStats forecastWriteLoad = createRandomMetricStats();
        ClusterBalanceStats.MetricStats forecastDiskUsage = createRandomMetricStats();
        ClusterBalanceStats.MetricStats actualDiskUsage = createRandomMetricStats();
        ClusterBalanceStats.TierBalanceStats tierBalanceStats = new ClusterBalanceStats.TierBalanceStats(
            shardCount,
            undesiredShardAllocations,
            forecastWriteLoad,
            forecastDiskUsage,
            actualDiskUsage
        );

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
        assertEquals(shardCountStats.get("total"), shardCount.total());
        assertEquals(shardCountStats.get("average"), shardCount.average());
        assertEquals(shardCountStats.get("min"), shardCount.min());
        assertEquals(shardCountStats.get("max"), shardCount.max());
        assertEquals(shardCountStats.get("std_dev"), shardCount.stdDev());

        Map<String, Object> undesiredShardAllocationCountStats = (Map<String, Object>) map.get("undesired_shard_allocation_count");
        assertThat(undesiredShardAllocationCountStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
        assertEquals(undesiredShardAllocationCountStats.get("total"), undesiredShardAllocations.total());
        assertEquals(undesiredShardAllocationCountStats.get("average"), undesiredShardAllocations.average());
        assertEquals(undesiredShardAllocationCountStats.get("min"), undesiredShardAllocations.min());
        assertEquals(undesiredShardAllocationCountStats.get("max"), undesiredShardAllocations.max());
        assertEquals(undesiredShardAllocationCountStats.get("std_dev"), undesiredShardAllocations.stdDev());

        Map<String, Object> forecastWriteLoadStats = (Map<String, Object>) map.get("forecast_write_load");
        assertThat(forecastWriteLoadStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
        assertEquals(forecastWriteLoadStats.get("total"), forecastWriteLoad.total());
        assertEquals(forecastWriteLoadStats.get("average"), forecastWriteLoad.average());
        assertEquals(forecastWriteLoadStats.get("min"), forecastWriteLoad.min());
        assertEquals(forecastWriteLoadStats.get("max"), forecastWriteLoad.max());
        assertEquals(forecastWriteLoadStats.get("std_dev"), forecastWriteLoad.stdDev());

        Map<String, Object> forecastDiskUsageStats = (Map<String, Object>) map.get("forecast_disk_usage");
        assertThat(forecastDiskUsageStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
        assertEquals(forecastDiskUsageStats.get("total"), forecastDiskUsage.total());
        assertEquals(forecastDiskUsageStats.get("average"), forecastDiskUsage.average());
        assertEquals(forecastDiskUsageStats.get("min"), forecastDiskUsage.min());
        assertEquals(forecastDiskUsageStats.get("max"), forecastDiskUsage.max());
        assertEquals(forecastDiskUsageStats.get("std_dev"), forecastDiskUsage.stdDev());

        Map<String, Object> actualDiskUsageStats = (Map<String, Object>) map.get("actual_disk_usage");
        assertThat(actualDiskUsageStats.keySet(), containsInAnyOrder("total", "average", "min", "max", "std_dev"));
        assertEquals(actualDiskUsageStats.get("total"), actualDiskUsage.total());
        assertEquals(actualDiskUsageStats.get("average"), actualDiskUsage.average());
        assertEquals(actualDiskUsageStats.get("min"), actualDiskUsage.min());
        assertEquals(actualDiskUsageStats.get("max"), actualDiskUsage.max());
        assertEquals(actualDiskUsageStats.get("std_dev"), actualDiskUsage.stdDev());
    }
}
