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
 * @param buildVersion              The node {@link BuildVersion}
 * @param minIndexVersion           The minimum {@link IndexVersion} supported by this node
 * @param minReadOnlyIndexVersion   The minimum {@link IndexVersion} for read-only indices supported by this node
 * @param maxIndexVersion           The maximum {@link IndexVersion} supported by this node
 */
public record VersionInformation(
    BuildVersion buildVersion,
    Version nodeVersion,
    IndexVersion minIndexVersion,
    IndexVersion minReadOnlyIndexVersion,
    IndexVersion maxIndexVersion
) {

    public static final VersionInformation CURRENT = new VersionInformation(
        BuildVersion.current(),
        IndexVersions.MINIMUM_COMPATIBLE,
        IndexVersions.MINIMUM_READONLY_COMPATIBLE,
        IndexVersion.current()
    );

    public VersionInformation {
        Objects.requireNonNull(buildVersion);
        Objects.requireNonNull(nodeVersion);
        Objects.requireNonNull(minIndexVersion);
        Objects.requireNonNull(minReadOnlyIndexVersion);
        Objects.requireNonNull(maxIndexVersion);
        assert minReadOnlyIndexVersion.onOrBefore(minIndexVersion) : minReadOnlyIndexVersion + " > " + minIndexVersion;
    }

    public VersionInformation(
        BuildVersion version,
        IndexVersion minIndexVersion,
        IndexVersion minReadOnlyIndexVersion,
        IndexVersion maxIndexVersion
    ) {
        this(version, Version.CURRENT, minIndexVersion, minReadOnlyIndexVersion, maxIndexVersion);
        /*
         * Whilst DiscoveryNode.getVersion exists, we need to be able to get a Version from VersionInfo
         * This needs to be consistent - on serverless, BuildVersion has an id of -1, which translates
         * to a nonsensical Version. So all consumers of Version need to be moved to BuildVersion
         * before we can remove Version from here.
         */
        // for the moment, check this is only called with current() so the implied Version is correct
        // TODO: work out what needs to happen for other versions. Maybe we can only remove this once the nodeVersion field is gone
        assert version.equals(BuildVersion.current()) : version + " is not " + BuildVersion.current();
    }

    @Deprecated
    public VersionInformation(Version version, IndexVersion minIndexVersion, IndexVersion maxIndexVersion) {
        this(version, minIndexVersion, minIndexVersion, maxIndexVersion);
    }

    @Deprecated
    public VersionInformation(
        Version version,
        IndexVersion minIndexVersion,
        IndexVersion minReadOnlyIndexVersion,
        IndexVersion maxIndexVersion
    ) {
        this(BuildVersion.fromVersionId(version.id()), version, minIndexVersion, minReadOnlyIndexVersion, maxIndexVersion);
    }

    @Deprecated
    public static VersionInformation inferVersions(Version nodeVersion) {
        if (nodeVersion == null) {
            return null;
        } else if (nodeVersion.equals(Version.CURRENT)) {
            return CURRENT;
        } else if (nodeVersion.before(Version.V_8_11_0)) {
            return new VersionInformation(
                nodeVersion,
                IndexVersion.getMinimumCompatibleIndexVersion(nodeVersion.id),
                IndexVersion.fromId(nodeVersion.id)
            );
        } else {
            throw new IllegalArgumentException("Node versions can only be inferred before release version 8.10.0");
        }
    }
}
