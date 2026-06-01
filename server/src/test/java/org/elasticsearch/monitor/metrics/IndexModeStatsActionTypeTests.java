/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class IndexModeStatsActionTypeTests extends ESTestCase {

    public void testNodeResponseStats() throws IOException {
        final Map<IndexMode, IndexStats> serializedStats = serializeAndReadStats(IndexMode.availableModes(), TransportVersion.current());
        for (var indexMode : IndexMode.availableModes()) {
            assertThat(serializedStats, hasKey(indexMode));
        }
    }

    /**
     * Verifies that modes introduced after a given transport version are silently filtered when
     * serializing to an older node. Tests each version boundary independently: before
     * {@link IndexMode#COLUMNAR_INDEX_MODES_ADDED} and before {@link IndexMode#VECTORDB_DOCUMENT_INDEX_MODE}.
     */
    public void testNodeResponseStatsFilteredByTransportVersion() throws IOException {
        final List<TransportVersion> boundaries = List.of(
            TransportVersionUtils.getPreviousVersion(IndexMode.COLUMNAR_INDEX_MODES_ADDED, true),
            TransportVersionUtils.getPreviousVersion(IndexMode.VECTORDB_DOCUMENT_INDEX_MODE, true)
        );
        for (TransportVersion boundary : boundaries) {
            final Map<IndexMode, IndexStats> stats = serializeAndReadStats(IndexMode.values(), boundary);
            for (IndexMode mode : IndexMode.values()) {
                if (mode.supportsVersion(boundary)) {
                    assertThat("version " + boundary + ": " + mode + " should be present", stats, hasKey(mode));
                } else {
                    assertThat("version " + boundary + ": " + mode + " should be absent", stats, not(hasKey(mode)));
                }
            }
        }
    }

    private static Map<IndexMode, IndexStats> serializeAndReadStats(IndexMode[] modes, TransportVersion version) throws IOException {
        final Map<IndexMode, IndexStats> stats = new EnumMap<>(IndexMode.class);
        for (IndexMode indexMode : modes) {
            stats.put(indexMode, new IndexStats());
        }
        final DiscoveryNode node = DiscoveryNodeUtils.create("node");
        final var nodeResponse = new IndexModeStatsActionType.NodeResponse(node, stats);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            nodeResponse.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                new DiscoveryNode(in);
                return in.readMap(IndexMode::readFrom, IndexStats::new);
            }
        }
    }
}
