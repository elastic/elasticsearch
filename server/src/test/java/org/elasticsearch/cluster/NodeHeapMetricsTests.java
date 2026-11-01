/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class NodeHeapMetricsTests extends ESTestCase {

    public void testShardHeapUsageIsTransportVersionGated() throws IOException {
        final long totalBytes = randomNonNegativeLong();
        final long totalHeapUsage = randomNonNegativeLong();
        final long hostedShardsHeapUsage = randomLongBetween(1, 10_000_000);
        final long nonShardHeapUsage = randomLongBetween(1, 10_000_000);
        final var metrics = new NodeHeapMetrics(
            randomUUID(),
            totalBytes,
            new NodeHeapEstimates(totalHeapUsage, hostedShardsHeapUsage, nonShardHeapUsage)
        );

        final var currentVersionCopy = copyWriteable(metrics, writableRegistry(), NodeHeapMetrics::readFrom, TransportVersion.current());
        assertThat(currentVersionCopy, equalTo(metrics));

        final var legacyVersion = TransportVersionUtils.getPreviousVersion(NodeHeapMetrics.SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE);
        final var legacyCopy = copyWriteable(metrics, writableRegistry(), NodeHeapMetrics::readFrom, legacyVersion);
        assertThat(legacyCopy, equalTo(new NodeHeapMetrics(metrics.nodeId(), totalBytes, new NodeHeapEstimates(totalHeapUsage, 0L, 0L))));
    }

    public void testEstimatedUsageAsPercentage() {
        final long totalBytes = randomNonNegativeLong();
        final long estimatedUsageBytes = randomLongBetween(0, totalBytes);
        final NodeHeapMetrics nodeHeapMetrics = new NodeHeapMetrics(
            randomUUID(),
            totalBytes,
            new NodeHeapEstimates(estimatedUsageBytes, randomLongBetween(0, estimatedUsageBytes), 0L)
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
            new NodeHeapEstimates(estimatedUsageBytes, randomLongBetween(0, estimatedUsageBytes), 0L)
        );
        assertThat(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), greaterThanOrEqualTo(0.0));
        assertThat(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), lessThanOrEqualTo(100.0));
        assertEquals(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), 100.0 * estimatedFreeBytes / totalBytes, 0.0001);
    }
}
