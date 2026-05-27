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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.monitor.metrics.IndexModeStatsActionType.NodeResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class IndexModeStatsActionTypeTests extends ESTestCase {

    /**
     * A within-minor rolling upgrade can serialize a node's index-mode stats to a node on an older transport version
     * that does not understand a feature-flagged mode (e.g. {@code columnar}). The stats response must drop the
     * unsupported mode instead of letting {@link IndexMode#writeTo} fail the whole response. See #149399.
     */
    public void testNodeResponseDropsUnsupportedModesOnOlderTransportVersion() throws IOException {
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());

        DiscoveryNode node = DiscoveryNodeUtils.create("node-1");
        Map<IndexMode, IndexStats> stats = new EnumMap<>(IndexMode.class);
        stats.put(IndexMode.STANDARD, new IndexStats());
        stats.put(IndexMode.COLUMNAR, new IndexStats());
        NodeResponse response = new NodeResponse(node, stats);

        TransportVersion older = TransportVersionUtils.getPreviousVersion(IndexMode.COLUMNAR_INDEX_MODES_ADDED);
        NodeResponse deserialized = copyWriteable(response, new NamedWriteableRegistry(List.of()), in -> new NodeResponse(in, node), older);
        assertNotNull(deserialized);
    }

    public void testIsWriteableTo() {
        TransportVersion beforeColumnar = TransportVersionUtils.getPreviousVersion(IndexMode.COLUMNAR_INDEX_MODES_ADDED);
        // Feature-flagged modes that predate the target version must be reported as not writeable so callers drop them.
        assertFalse(IndexMode.isWriteableTo(IndexMode.COLUMNAR, beforeColumnar));
        assertFalse(IndexMode.isWriteableTo(IndexMode.LOGSDB_COLUMNAR, beforeColumnar));
        // Modes that exist on every compatible version are always writeable.
        assertTrue(IndexMode.isWriteableTo(IndexMode.STANDARD, beforeColumnar));
        assertTrue(IndexMode.isWriteableTo(IndexMode.TIME_SERIES, beforeColumnar));
        assertTrue(IndexMode.isWriteableTo(IndexMode.LOGSDB, beforeColumnar));
        assertTrue(IndexMode.isWriteableTo(IndexMode.LOOKUP, beforeColumnar));
        // Once the target version supports the mode it becomes writeable.
        assertTrue(IndexMode.isWriteableTo(IndexMode.COLUMNAR, IndexMode.COLUMNAR_INDEX_MODES_ADDED));

        TransportVersion beforeVectordb = TransportVersionUtils.getPreviousVersion(IndexMode.VECTORDB_DOCUMENT_INDEX_MODE);
        assertFalse(IndexMode.isWriteableTo(IndexMode.VECTORDB_DOCUMENT, beforeVectordb));
        assertTrue(IndexMode.isWriteableTo(IndexMode.VECTORDB_DOCUMENT, IndexMode.VECTORDB_DOCUMENT_INDEX_MODE));
    }
}
