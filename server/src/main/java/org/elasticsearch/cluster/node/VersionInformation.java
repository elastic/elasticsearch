/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.index.IndexVersion;

import java.util.Objects;

/**
 * Represents the versions of various aspects of an Elasticsearch node.
 * @param nodeVersion       The node {@link Version}
 * @param minIndexVersion   The minimum {@link IndexVersion} supported by this node
 * @param maxIndexVersion   The maximum {@link IndexVersion} supported by this node
 */
public record VersionInformation(Version nodeVersion, IndexVersion minIndexVersion, IndexVersion maxIndexVersion) {

    public static final VersionInformation CURRENT = new VersionInformation(
        Version.CURRENT,
        IndexVersion.MINIMUM_COMPATIBLE,
        IndexVersion.CURRENT
    );

    public static VersionInformation inferVersions(Version nodeVersion) {
        if (nodeVersion == null) {
            return null;
        } else if (nodeVersion.equals(Version.CURRENT)) {
            return CURRENT;
        } else if (nodeVersion.before(Version.V_8_10_0)) {
            return new VersionInformation(
                nodeVersion,
                IndexVersion.fromId(nodeVersion.minimumIndexCompatibilityVersion().id),
                IndexVersion.fromId(nodeVersion.id)
            );
        } else {
            throw new IllegalArgumentException("Node versions can only be inferred before release version 8.10.0");
        }
    }

    public VersionInformation {
        Objects.requireNonNull(nodeVersion);
        Objects.requireNonNull(minIndexVersion);
        Objects.requireNonNull(maxIndexVersion);
    }
}
