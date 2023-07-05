/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.utils;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.utils.TransportVersionUtils;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class TransportVersionUtilsTests extends ESTestCase {

    private static final Map<String, TransportVersion> transportVersions = Map.of(
        "Alfredo",
        TransportVersion.V_7_0_0,
        "Bertram",
        TransportVersion.V_7_0_1,
        "Charles",
        TransportVersion.V_8_500_003,
        "Dominic",
        TransportVersion.V_8_0_0
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
        null,
        false,
        null
    );

    public void testGetMinTransportVersion() {
        assertThat(TransportVersionUtils.getMinTransportVersion(state), equalTo(TransportVersion.V_7_0_0));
    }

    public void testIsMinTransformVersionSameAsCurrent() {
        assertThat(TransportVersionUtils.isMinTransformVersionSameAsCurrent(state), equalTo(false));

        Map<String, TransportVersion> transportVersions1 = Map.of("Eugene", TransportVersion.current());

        ClusterState state1 = new ClusterState(
            new ClusterName("harry"),
            0L,
            "20F833F2-7C48-4522-BA78-6821C9DCD5D8",
            null,
            null,
            null,
            transportVersions1,
            null,
            null,
            false,
            null
        );

        assertThat(TransportVersionUtils.isMinTransformVersionSameAsCurrent(state1), equalTo(true));
    }
}
