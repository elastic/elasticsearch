/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Utility to access {@link ClusterState} only when it is "ready", with a fallback if it's not. The definition of "ready" is left to the
 * class implementations.
 */
public interface ClusterStateSupplier extends Supplier<Optional<ClusterState>> {
    default <T> T withCurrentClusterState(Function<ClusterState, T> clusterStateFunction, T fallbackIfNotReady) {
        var x = get();
        return x.map(clusterStateFunction).orElse(fallbackIfNotReady);
    }
}
