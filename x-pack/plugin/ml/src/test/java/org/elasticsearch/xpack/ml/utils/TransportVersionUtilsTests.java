/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.TransportVersionUtils;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TransportVersionUtilsTests extends ESTestCase {

    private static final Map<String, CompatibilityVersions> transportVersions = Map.of(
        "Alfredo",
        new CompatibilityVersions(TransportVersions.V_7_0_0, Map.of()),
        "Bertram",
        new CompatibilityVersions(TransportVersions.V_7_0_1, Map.of()),
        "Charles",
        new CompatibilityVersions(TransportVersions.V_8_500_020, Map.of()),
        "Dominic",
        new CompatibilityVersions(TransportVersions.V_8_0_0, Map.of())
    );

    private static final ClusterState state = new ClusterState(
        new ClusterName("fred"),
        0L,
        "EC7C0637-1644-43AB-AEAB-D8B7970CAECA",
        null,
        null,
        null,
        transportVersions,
        null,
        ClusterBlocks.EMPTY_CLUSTER_BLOCK,
        null,
        false,
        null
    );

    public void testGetMinTransportVersion() {
        assertThat(TransportVersionUtils.getMinTransportVersion(state), equalTo(TransportVersions.V_7_0_0));
    }

    public void testIsMinTransformVersionSameAsCurrent() {
        assertThat(TransportVersionUtils.isMinTransportVersionSameAsCurrent(state), equalTo(false));

        Map<String, CompatibilityVersions> transportVersions1 = Map.of(
            "Eugene",
            new CompatibilityVersions(TransportVersion.current(), Map.of())
        );

        ClusterState state1 = new ClusterState(
            new ClusterName("harry"),
            0L,
            "20F833F2-7C48-4522-BA78-6821C9DCD5D8",
            null,
            null,
            null,
            transportVersions1,
            null,
            ClusterBlocks.EMPTY_CLUSTER_BLOCK,
            null,
            false,
            null
        );

        assertThat(TransportVersionUtils.isMinTransportVersionSameAsCurrent(state1), equalTo(true));
    }

    public void testIsMinTransportVersionOnOrAfter() {
        assertThat(TransportVersionUtils.isMinTransportVersionOnOrAfter(state, TransportVersions.V_7_0_0), equalTo(true));
        assertThat(TransportVersionUtils.isMinTransportVersionOnOrAfter(state, TransportVersions.V_8_500_020), equalTo(false));
    }
}
