/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.internal;

import org.elasticsearch.Build;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.env.DefaultBuildVersion;

/**
 * Allows plugging in current build info.
 */
public interface BuildExtension {

    /**
     * Returns the {@link Build} that represents the running Elasticsearch code.
     */
    Build getCurrentBuild();

    /**
     * {@code true} if this build uses release versions.
     */
    default boolean hasReleaseVersioning() {
        return true;
    }

    // TODO[wrb]: Remove default implementation once downstream BuildExtensions are updated
    default BuildVersion currentBuildVersion() {
        return DefaultBuildVersion.CURRENT;
    }

    // TODO[wrb]: Remove default implementation once downstream BuildExtensions are updated
    default BuildVersion fromVersionId(int versionId) {
        return new DefaultBuildVersion(versionId);
    }
}
