/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.internal;

import org.elasticsearch.Build;
import org.elasticsearch.env.BuildVersion;

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

    /**
     * Returns the {@link BuildVersion} for the running Elasticsearch code.
     */
    BuildVersion currentBuildVersion();

    /**
     * Returns the {@link BuildVersion} for a given version identifier.
     */
    BuildVersion fromVersionId(int versionId);

    /**
     * Returns the {@link BuildVersion} for a given version string.
     */
    BuildVersion fromString(String version);
}
