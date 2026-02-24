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

public class NodeGpuStatsResponseTests extends ESTestCase {

    // Reproduces #142936: GPUSupport previously returned -1 for totalGpuMemoryInBytes when no GPU
    // is present, and writeVLong does not support negative values, causing an IllegalStateException.
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
}
