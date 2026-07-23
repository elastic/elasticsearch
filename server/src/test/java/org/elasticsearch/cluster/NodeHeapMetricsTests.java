/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class NodeHeapMetricsTests extends ESTestCase {

    public void testEstimatedUsageAsPercentage() {
        final long totalBytes = randomNonNegativeLong();
        final long estimatedUsageBytes = randomLongBetween(0, totalBytes);
        final NodeHeapMetrics nodeHeapMetrics = new NodeHeapMetrics(
            randomUUID(),
            totalBytes,
            new NodeHeapEstimates(estimatedUsageBytes, randomLongBetween(0, estimatedUsageBytes))
        );
        assertThat(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), greaterThanOrEqualTo(0.0));
        assertThat(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), lessThanOrEqualTo(100.0));
        assertEquals(nodeHeapMetrics.estimatedUsageAsPercentage(), 100.0 * estimatedUsageBytes / totalBytes, 0.0001);
    }

    public void testUpdateEstimatedUsageClampsToZero() {
        final long totalBytes = 1000L;
        final long initialTotal = 100L;
        final long initialHosted = 50L;
        final NodeHeapMetrics metrics = new NodeHeapMetrics(randomUUID(), totalBytes, new NodeHeapEstimates(initialTotal, initialHosted));

        // Shard removal that would drive hostedShardsHeapUsage below zero
        final NodeHeapMetrics afterShardRemoval = metrics.updateEstimatedUsage(0, -200L);
        assertThat(afterShardRemoval.nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(0L));
        assertThat(afterShardRemoval.nodeHeapEstimates().totalHeapUsage(), equalTo(0L));

        // Index removal that would drive totalHeapUsage below zero without affecting hosted
        final NodeHeapMetrics afterIndexRemoval = metrics.updateEstimatedUsage(-200L, 0);
        assertThat(afterIndexRemoval.nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(initialHosted));
        assertThat(afterIndexRemoval.nodeHeapEstimates().totalHeapUsage(), equalTo(0L));
    }

    public void testEstimatedFreeBytesAsPercentage() {
        final long totalBytes = randomNonNegativeLong();
        final long estimatedUsageBytes = randomLongBetween(0, totalBytes);
        final long estimatedFreeBytes = totalBytes - estimatedUsageBytes;
        final NodeHeapMetrics nodeHeapMetrics = new NodeHeapMetrics(
            randomUUID(),
            totalBytes,
            new NodeHeapEstimates(estimatedUsageBytes, randomLongBetween(0, estimatedUsageBytes))
        );
        assertThat(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), greaterThanOrEqualTo(0.0));
        assertThat(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), lessThanOrEqualTo(100.0));
        assertEquals(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), 100.0 * estimatedFreeBytes / totalBytes, 0.0001);
    }
}
