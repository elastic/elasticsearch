/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;

public class IndexModeStatsActionTypeTests extends ESTestCase {

    public void testNodeResponseStats() throws IOException {
        final Map<IndexMode, IndexStats> serializedStats = serializeAndReadStats();
        for (var indexStats : IndexMode.availableModes()) {
            assertTrue(serializedStats.containsKey(indexStats));
        }
    }

    private static Map<IndexMode, IndexStats> serializeAndReadStats() throws IOException {
        final Map<IndexMode, IndexStats> stats = new EnumMap<>(IndexMode.class);
        for (IndexMode indexMode : IndexMode.availableModes()) {
            stats.put(indexMode, new IndexStats());
        }
        final DiscoveryNode node = DiscoveryNodeUtils.create("node");
        final var nodeResponse = new IndexModeStatsActionType.NodeResponse(node, stats);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeResponse.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                new DiscoveryNode(in);
                return in.readMap(IndexMode::readFrom, IndexStats::new);
            }
        }
    }
}
