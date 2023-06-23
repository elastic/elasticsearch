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

public record NodeVersions(Version nodeVersion, IndexVersion minIndexVersion, IndexVersion maxIndexVersion) {

    public static final NodeVersions CURRENT = new NodeVersions(Version.CURRENT, IndexVersion.MINIMUM_COMPATIBLE, IndexVersion.CURRENT);

    public NodeVersions(Version nodeVersion) {
        this(nodeVersion, IndexVersion.MINIMUM_COMPATIBLE, IndexVersion.CURRENT);
    }

    public NodeVersions {
        Objects.requireNonNull(nodeVersion);
        Objects.requireNonNull(minIndexVersion);
        Objects.requireNonNull(maxIndexVersion);
    }
}
