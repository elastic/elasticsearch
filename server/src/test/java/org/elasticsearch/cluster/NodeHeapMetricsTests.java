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

    public void testUpdateEstimatedUsagePositiveDeltas() {
        final String nodeId = randomUUID();
        final long totalBytes = randomLongBetween(1000, Long.MAX_VALUE / 2);
        final long initialTotal = randomLongBetween(0, totalBytes / 4);
        final long initialShards = randomLongBetween(0, initialTotal);
        final NodeHeapMetrics original = new NodeHeapMetrics(nodeId, totalBytes, new NodeHeapEstimates(initialTotal, initialShards));

        final long indexDelta = randomLongBetween(0, totalBytes / 4);
        final long shardsDelta = randomLongBetween(0, totalBytes / 4);
        final long expectedUsageDelta = indexDelta + shardsDelta;

        final NodeHeapMetrics updated = original.updateEstimatedUsage(indexDelta, shardsDelta);

        assertEquals(nodeId, updated.nodeId());
        assertEquals(totalBytes, updated.totalBytes());
        assertEquals(initialTotal + expectedUsageDelta, updated.nodeHeapEstimates().totalHeapUsage());
        assertEquals(initialShards + expectedUsageDelta, updated.nodeHeapEstimates().hostedShardsHeapUsage());
    }

    public void testUpdateEstimatedUsageZeroDeltas() {
        final long totalBytes = randomNonNegativeLong();
        final long initialTotal = randomLongBetween(0, totalBytes);
        final long initialShards = randomLongBetween(0, initialTotal);
        final NodeHeapMetrics original = new NodeHeapMetrics(randomUUID(), totalBytes, new NodeHeapEstimates(initialTotal, initialShards));

        final NodeHeapMetrics updated = original.updateEstimatedUsage(0, 0);

        assertEquals(original.nodeId(), updated.nodeId());
        assertEquals(original.totalBytes(), updated.totalBytes());
        assertEquals(initialTotal, updated.nodeHeapEstimates().totalHeapUsage());
        assertEquals(initialShards, updated.nodeHeapEstimates().hostedShardsHeapUsage());
    }

    public void testUpdateEstimatedUsageNegativeDeltas() {
        final long totalBytes = randomLongBetween(1000, Long.MAX_VALUE / 2);
        final long initialTotal = randomLongBetween(totalBytes / 2, totalBytes);
        final long initialShards = randomLongBetween(initialTotal / 2, initialTotal);
        final NodeHeapMetrics original = new NodeHeapMetrics(randomUUID(), totalBytes, new NodeHeapEstimates(initialTotal, initialShards));

        final long indexDelta = -randomLongBetween(0, initialTotal / 4);
        final long shardsDelta = -randomLongBetween(0, initialTotal / 4);
        final long expectedUsageDelta = indexDelta + shardsDelta;

        final NodeHeapMetrics updated = original.updateEstimatedUsage(indexDelta, shardsDelta);

        assertEquals(initialTotal + expectedUsageDelta, updated.nodeHeapEstimates().totalHeapUsage());
        assertEquals(initialShards + expectedUsageDelta, updated.nodeHeapEstimates().hostedShardsHeapUsage());
    }

    public void testUpdateEstimatedUsageDeltaSumOverflowThrows() {
        final NodeHeapMetrics original = new NodeHeapMetrics(randomUUID(), Long.MAX_VALUE, new NodeHeapEstimates(0, 0));
        expectThrows(ArithmeticException.class, () -> original.updateEstimatedUsage(Long.MAX_VALUE, 1));
    }

    public void testUpdateEstimatedUsageTotalOverflowThrows() {
        final NodeHeapMetrics original = new NodeHeapMetrics(randomUUID(), Long.MAX_VALUE, new NodeHeapEstimates(2, 0));
        // usageDelta = Long.MAX_VALUE; totalHeapUsage = 2 + Long.MAX_VALUE overflows
        expectThrows(ArithmeticException.class, () -> original.updateEstimatedUsage(Long.MAX_VALUE, 0));
    }
}
