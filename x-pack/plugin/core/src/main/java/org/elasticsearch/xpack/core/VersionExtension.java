/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;

import java.util.ServiceLoader;

/**
 * Allows plugging in xpack config versions.
 */
public interface VersionExtension {

    /**
     * Returns the {@link MlConfigVersion} that Elasticsearch should use.
     * <p>
     * This must be at least equal to the latest version found in {@link MlConfigVersion} V_* constants.
     */
    MlConfigVersion getCurrentMlConfigVersion();

    /**
     * Returns the {@link TransformConfigVersion} that Elasticsearch should use.
     * <p>
     * This must be at least equal to the latest version found in {@link TransformConfigVersion} V_* constants.
     */
    TransformConfigVersion getCurrentTransformConfigVersion();

    /**
     * Loads a single VersionExtension, or returns {@code null} if none are found.
     */
    static VersionExtension load() {
        var loader = ServiceLoader.load(VersionExtension.class);
        var extensions = loader.stream().toList();
        if (extensions.size() > 1) {
            throw new IllegalStateException("More than one version extension found");
        } else if (extensions.isEmpty()) {
            return null;
        }
        return extensions.get(0).get();
    }
}
