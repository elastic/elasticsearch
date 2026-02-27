/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;

public class TransportGpuStatsActionTests extends ESTestCase {

    public void testSupportsGpuStatsTransportActionFiltersOldNodes() {
        var oldNode = DiscoveryNodeUtils.builder("old")
            .version(new VersionInformation(Version.V_9_1_9, IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current()))
            .build();
        var compatibleNode = DiscoveryNodeUtils.builder("compatible")
            .version(new VersionInformation(Version.V_9_3_0, IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current()))
            .build();
        var newNode = DiscoveryNodeUtils.builder("new").version(Version.CURRENT).build();

        assertFalse(TransportGpuStatsAction.supportsGpuStatsTransportAction(oldNode));
        assertTrue(TransportGpuStatsAction.supportsGpuStatsTransportAction(compatibleNode));
        assertTrue(TransportGpuStatsAction.supportsGpuStatsTransportAction(newNode));
    }

    public void testGpuStatsActionNameRemainsStable() {
        assertEquals("cluster:monitor/xpack/gpu_vector_indexing/stats/dist", GpuStatsAction.NAME);
    }
}
