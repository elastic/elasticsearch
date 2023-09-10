/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.internal;

import org.elasticsearch.Build;

import java.util.ServiceLoader;

/**
 * Allows plugging in current build info.
 */
public interface BuildExtension {

    /**
     * Returns the {@link Build} that represents the running Elasticsearch code.
     */
    Build getCurrentBuild();

    /**
     * Loads a single BuildExtension, or returns {@code null} if none are found.
     */
    static BuildExtension load() {
        var loader = ServiceLoader.load(BuildExtension.class);
        var extensions = loader.stream().toList();
        if (extensions.size() > 1) {
            throw new IllegalStateException("More than one build extension found");
        } else if (extensions.size() == 0) {
            return null;
        }
        return extensions.get(0).get();
    }
}
