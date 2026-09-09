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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
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

    public void testNodeHeapEstimatesSerializationScenarios() throws IOException {
        final var legacyVersion = TransportVersionUtils.getPreviousVersion(NodeHeapEstimates.EXPLICIT_HEAP_ESTIMATE_COMPONENTS);
        final var newData = new NodeHeapEstimates(300L, 120L, 80L);
        final var oldData = new NodeHeapEstimates(300L, 120L, 0L);

        // New code + new data: all explicit components are preserved on the current wire format.
        assertThat(copy(newData, NodeHeapEstimates::new), equalTo(newData));

        // Old code + new data: the legacy wire format has no non-shard field, so new data loses that component.
        assertThat(copy(newData, legacyVersion).nonShardHeapUsage(), equalTo(0L));

        // New code + old data: old-shaped data remains explicit when written on the current wire format.
        assertThat(copy(oldData, NodeHeapEstimates::new), equalTo(oldData));

        // Old code + old data: old-shaped data stays old-shaped over the legacy wire format.
        assertThat(copy(oldData, legacyVersion), equalTo(oldData));
    }

    public void testNodeHeapMetricsSerializationScenarios() throws IOException {
        final var legacyNodeHeapEstimatesVersion = TransportVersionUtils.getPreviousVersion(
            NodeHeapEstimates.EXPLICIT_HEAP_ESTIMATE_COMPONENTS
        );
        final var legacyTotalOnlyVersion = TransportVersionUtils.getPreviousVersion(
            NodeHeapMetrics.SHARD_HEAP_USAGE_IN_ESTIMATED_HEAP_USAGE
        );
        final var newData = new NodeHeapMetrics("node", 1_000L, new NodeHeapEstimates(300L, 120L, 80L));
        final var oldData = new NodeHeapMetrics("node", 1_000L, new NodeHeapEstimates(300L, 120L, 0L));

        // New code + new data: all nested estimate components are preserved.
        assertThat(copy(newData), equalTo(newData));

        // Old code + new data: node metrics still contain total/hosted heap, but explicit non-shard heap is not on the legacy wire.
        final var newDataOverLegacyWire = copy(newData, legacyNodeHeapEstimatesVersion);
        assertThat(newDataOverLegacyWire.nodeHeapEstimates().totalHeapUsage(), equalTo(300L));
        assertThat(newDataOverLegacyWire.nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(120L));
        assertThat(newDataOverLegacyWire.nodeHeapEstimates().nonShardHeapUsage(), equalTo(0L));

        // New code + old data: an old-shaped estimate remains explicit when written on the current wire format.
        assertThat(copy(oldData), equalTo(oldData));

        // Old code + old data: old-shaped data keeps total/hosted heap and the placeholder non-shard component.
        assertThat(copy(oldData, legacyNodeHeapEstimatesVersion), equalTo(oldData));

        // Very old code predates hosted-shards heap in NodeHeapMetrics, so only total heap can be recovered.
        final var totalOnlyCopy = copy(newData, legacyTotalOnlyVersion);
        assertThat(totalOnlyCopy.totalBytes(), equalTo(1_000L));
        assertThat(totalOnlyCopy.nodeHeapEstimates().totalHeapUsage(), equalTo(300L));
        assertThat(totalOnlyCopy.nodeHeapEstimates().hostedShardsHeapUsage(), equalTo(0L));
        assertThat(totalOnlyCopy.nodeHeapEstimates().nonShardHeapUsage(), equalTo(0L));
    }

    private static NodeHeapEstimates copy(NodeHeapEstimates estimates, Writeable.Reader<NodeHeapEstimates> reader) throws IOException {
        return copy(estimates, reader, TransportVersion.current());
    }

    private static NodeHeapEstimates copy(NodeHeapEstimates estimates, TransportVersion transportVersion) throws IOException {
        return copy(estimates, NodeHeapEstimates::new, transportVersion);
    }

    private static NodeHeapEstimates copy(
        NodeHeapEstimates estimates,
        Writeable.Reader<NodeHeapEstimates> reader,
        TransportVersion transportVersion
    ) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(transportVersion);
            estimates.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(transportVersion);
                return reader.read(in);
            }
        }
    }

    private static NodeHeapMetrics copy(NodeHeapMetrics metrics) throws IOException {
        return copy(metrics, TransportVersion.current());
    }

    private static NodeHeapMetrics copy(NodeHeapMetrics metrics, TransportVersion transportVersion) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(transportVersion);
            metrics.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(transportVersion);
                return NodeHeapMetrics.readFrom(in);
            }
        }
    }
}
