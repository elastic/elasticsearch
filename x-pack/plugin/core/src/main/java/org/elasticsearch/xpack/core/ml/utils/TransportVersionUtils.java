/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState;

public class TransportVersionUtils {
    private TransportVersionUtils() {}

    public static TransportVersion getMinTransportVersion(ClusterState state) {
        return state.getMinTransportVersion();
    }

    public static TransportVersion getCurrentTransportVersion() {
        return TransportVersion.current();
    }

    public static boolean isMinTransportVersionSameAsCurrent(ClusterState state) {
        TransportVersion minTransportVersion = getMinTransportVersion(state);
        TransportVersion currentTransformVersion = TransportVersion.current();
        return minTransportVersion.compareTo(currentTransformVersion) == 0;
    }

    public static boolean isMinTransportVersionOnOrAfter(ClusterState state, TransportVersion version) {
        return getMinTransportVersion(state).onOrAfter(version);
    }
}
