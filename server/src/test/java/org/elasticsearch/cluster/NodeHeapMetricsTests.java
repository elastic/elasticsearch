/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

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
            new NodeHeapEstimates(estimatedUsageBytes, randomLongBetween(0, estimatedUsageBytes), randomLongBetween(0, estimatedUsageBytes))
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
            new NodeHeapEstimates(estimatedUsageBytes, randomLongBetween(0, estimatedUsageBytes), randomLongBetween(0, estimatedUsageBytes))
        );
        assertThat(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), greaterThanOrEqualTo(0.0));
        assertThat(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), lessThanOrEqualTo(100.0));
        assertEquals(nodeHeapMetrics.estimatedFreeBytesAsPercentage(), 100.0 * estimatedFreeBytes / totalBytes, 0.0001);
    }

    public void testUpdateEstimatedUsagePreservesNonShardHeapUsage() {
        final var nodeHeapMetrics = new NodeHeapMetrics("node", 1_000L, new NodeHeapEstimates(300L, 120L, 80L));

        final NodeHeapMetrics updated = nodeHeapMetrics.updateEstimatedUsage(40L, 60L);

        assertThat(updated.nodeHeapEstimates().totalHeapUsage(), equalTo(400L));
        assertThat(updated.nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(220L));
        assertThat(updated.nodeHeapEstimates().nonShardHeapUsage(), equalTo(80L));
    }

    public void testLegacyNodeHeapEstimatesSerializationUsesZeroNonShardHeapUsage() throws IOException {
        final var legacyVersion = TransportVersionUtils.getPreviousVersion(NodeHeapEstimates.EXPLICIT_HEAP_ESTIMATE_COMPONENTS);
        final var estimates = new NodeHeapEstimates(300L, 120L, 80L);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(legacyVersion);
            estimates.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(legacyVersion);
                final var readEstimates = new NodeHeapEstimates(in);
                assertThat(readEstimates.totalHeapUsage(), equalTo(300L));
                assertThat(readEstimates.hostedShardsHeapUsage(), equalTo(120L));
                assertThat(readEstimates.nonShardHeapUsage(), equalTo(0L));
            }
        }
    }
}
