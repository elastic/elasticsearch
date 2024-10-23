/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.util.Objects;

/**
 * Represents the versions of various aspects of an Elasticsearch node.
 * @param buildVersion      The node {@link BuildVersion}
 * @param minIndexVersion   The minimum {@link IndexVersion} supported by this node
 * @param maxIndexVersion   The maximum {@link IndexVersion} supported by this node
 */
public record VersionInformation(BuildVersion buildVersion, IndexVersion minIndexVersion, IndexVersion maxIndexVersion) {

    public static final VersionInformation CURRENT = new VersionInformation(
        BuildVersion.current(),
        IndexVersions.MINIMUM_COMPATIBLE,
        IndexVersion.current()
    );

    public VersionInformation {
        Objects.requireNonNull(buildVersion);
        Objects.requireNonNull(minIndexVersion);
        Objects.requireNonNull(maxIndexVersion);
    }

    @Deprecated
    public VersionInformation(Version version, IndexVersion minIndexVersion, IndexVersion maxIndexVersion) {
        this(BuildVersion.fromVersionId(version.id()), minIndexVersion, maxIndexVersion);
    }

    @Deprecated
    public Version nodeVersion() {
        return Version.fromId(buildVersion.id());
    }

    public static VersionInformation inferVersions(Version nodeVersion) {
        if (nodeVersion == null) {
            return null;
        } else if (nodeVersion.equals(Version.CURRENT)) {
            return CURRENT;
        } else if (nodeVersion.before(Version.V_8_11_0)) {
            return new VersionInformation(
                BuildVersion.fromVersionId(nodeVersion.id()),
                IndexVersion.getMinimumCompatibleIndexVersion(nodeVersion.id),
                IndexVersion.fromId(nodeVersion.id)
            );
        } else {
            throw new IllegalArgumentException("Node versions can only be inferred before release version 8.10.0");
        }
    }
}
