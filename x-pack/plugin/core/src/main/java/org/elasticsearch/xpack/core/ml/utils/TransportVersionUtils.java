/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterState;

import java.util.Comparator;

public class TransportVersionUtils {
    private TransportVersionUtils() {}

    public static TransportVersion getMaxTransportVersion(ClusterState state) {
        return state.transportVersions().values().stream().max(Comparator.naturalOrder()).orElse(TransportVersion.current());
    }

    public static TransportVersion getMinTransportVersion(ClusterState state) {
        return state.getMinTransportVersion();
    }

    public static boolean areAllTransformVersionsTheSame(ClusterState state) {
        TransportVersion minTransportVersion = getMinTransportVersion(state);
        TransportVersion maxTransportVersion = getMaxTransportVersion(state);
        return minTransportVersion.compareTo(maxTransportVersion) == 0;
    }
}
