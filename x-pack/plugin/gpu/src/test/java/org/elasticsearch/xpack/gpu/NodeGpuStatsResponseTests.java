/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class NodeGpuStatsResponseTests extends ESTestCase {

    // GPUSupport returns 0L for totalGpuMemoryInBytes when no GPU is present.
    public void testSerializationWithNoGpu() throws IOException {
        var node = DiscoveryNodeUtils.create("node1");
        var response = new NodeGpuStatsResponse(node, false, false, 0, 0L, null);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);

            var in = out.bytes().streamInput();
            var deserialized = new NodeGpuStatsResponse(in);
            assertFalse(deserialized.isGpuSupported());
            assertFalse(deserialized.isGpuSettingEnabled());
            assertEquals(0, deserialized.getGpuUsageCount());
            assertEquals(0L, deserialized.getTotalGpuMemoryInBytes());
            assertNull(deserialized.getGpuName());
        }
    }

    // Verifies that a response from a node with a working GPU can be
    // round-tripped through stream serialization with all fields preserved.
    public void testSerializationWithGpu() throws IOException {
        var node = DiscoveryNodeUtils.create("node1");
        var response = new NodeGpuStatsResponse(node, true, true, 42, 24_000_000_000L, "NVIDIA L4");

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);

            var in = out.bytes().streamInput();
            var deserialized = new NodeGpuStatsResponse(in);
            assertTrue(deserialized.isGpuSupported());
            assertTrue(deserialized.isGpuSettingEnabled());
            assertEquals(42, deserialized.getGpuUsageCount());
            assertEquals(24_000_000_000L, deserialized.getTotalGpuMemoryInBytes());
            assertEquals("NVIDIA L4", deserialized.getGpuName());
        }
    }

    // Verifies that negative gpuUsageCount is rejected at construction time.
    public void testNegativeGpuUsageCountRejected() {
        var node = DiscoveryNodeUtils.create("node1");
        var e = expectThrows(IllegalArgumentException.class, () -> new NodeGpuStatsResponse(node, true, true, -1, 24_000_000_000L, "GPU"));
        assertThat(e.getMessage(), containsString("gpuUsageCount must be non-negative"));
    }

    // Verifies that negative totalGpuMemoryInBytes is rejected at construction time.
    public void testNegativeTotalGpuMemoryRejected() {
        var node = DiscoveryNodeUtils.create("node1");
        var e = expectThrows(IllegalArgumentException.class, () -> new NodeGpuStatsResponse(node, true, true, 0, -1L, "GPU"));
        assertThat(e.getMessage(), containsString("totalGpuMemoryInBytes must be non-negative"));
    }
}
