/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml;

import java.util.ServiceLoader;

/**
 * Allows plugging in ML config versions.
 */
public interface MlVersionExtension {

    /**
     * Returns the {@link MlVersionExtension} that Elasticsearch should use.
     * <p>
     * This must be at least equal to the latest version found in {@link MlVersionExtension} V_* constants.
     */
    MlConfigVersion getCurrentMlConfigVersion();

    /**
     * Loads a single VersionExtension, or returns {@code null} if none are found.
     */
    static MlVersionExtension load() {
        var loader = ServiceLoader.load(MlVersionExtension.class);
        var extensions = loader.stream().toList();
        if (extensions.size() > 1) {
            throw new IllegalStateException("More than one version extension found");
        } else if (extensions.isEmpty()) {
            return null;
        }
        return extensions.get(0).get();
    }
}
