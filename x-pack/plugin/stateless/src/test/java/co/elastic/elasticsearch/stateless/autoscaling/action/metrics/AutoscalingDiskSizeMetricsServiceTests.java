/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.autoscaling.action.metrics;

import co.elastic.elasticsearch.stateless.autoscaling.action.metrics.AutoscalingDiskSizeMetricsService.DiskSizeMetrics;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import static co.elastic.elasticsearch.stateless.autoscaling.action.metrics.AutoscalingDiskSizeMetricsService.NO_TIMESTAMP;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.elasticsearch.core.TimeValue.timeValueDays;
import static org.hamcrest.Matchers.equalTo;

public class AutoscalingDiskSizeMetricsServiceTests extends ESTestCase {

    public void testShouldComputeMetricsForAnIndexWithNoTimestamp() {

        var service = new AutoscalingDiskSizeMetricsService(createBuiltInClusterSettings(), () -> 1L);

        service.onSegmentCreated(new ShardId("index-1", "_na_", 0), "segment_1", 100L, NO_TIMESTAMP);
        service.onSegmentCreated(new ShardId("index-1", "_na_", 0), "segment_2", 200L, NO_TIMESTAMP);

        var metrics = service.getDiskSizeMetrics();

        assertThat(metrics, equalTo(new DiskSizeMetrics(true, 200L, 300L, 0L)));
    }

    public void testShouldComputeMetricsForAnIndexWithTimestamp() {

        var currentTime = System.currentTimeMillis();
        var service = new AutoscalingDiskSizeMetricsService(createBuiltInClusterSettings(), () -> currentTime);

        // for now age is hardcoded to 7 days. ES-6224 will make it configurable
        service.onSegmentCreated(new ShardId("index-1", "_na_", 0), "segment_1", 100L, currentTime - timeValueDays(10).millis());
        service.onSegmentCreated(new ShardId("index-1", "_na_", 0), "segment_2", 200L, currentTime - timeValueDays(1).millis());

        var metrics = service.getDiskSizeMetrics();

        assertThat(metrics, equalTo(new DiskSizeMetrics(true, 200L, 200L, 100L)));
    }
}
