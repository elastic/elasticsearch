/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.version;

import org.elasticsearch.TransportVersion;

import java.util.Comparator;
import java.util.Map;

/**
 * Wraps component version numbers for cluster state
 *
 * <p>Cluster state will need to carry version information for different independently versioned components.
 * This wrapper lets us wrap these versions one level below {@link org.elasticsearch.cluster.ClusterState}.
 * It's similar to {@link org.elasticsearch.cluster.node.VersionInformation}, but this class is meant to
 * be constructed during node startup and hold values from plugins as well.
 *
 * @param transportVersion
 */
public record VersionsWrapper(TransportVersion transportVersion) {

    /**
     * Constructs a VersionWrapper collecting all the minimum versions from the values of the map.
     *
     * @param versionsWrappers A map of strings (typically node identifiers) and versions wrappers
     * @return Minimum versions for the cluster
     */
    public static VersionsWrapper minimumVersions(Map<String, VersionsWrapper> versionsWrappers) {
        return new VersionsWrapper(
            versionsWrappers.values()
                .stream()
                .map(VersionsWrapper::transportVersion)
                .min(Comparator.naturalOrder())
                // In practice transportVersions is always nonempty (except in tests) but use a conservative default anyway:
                .orElse(TransportVersion.MINIMUM_COMPATIBLE)
        );
    }
}
