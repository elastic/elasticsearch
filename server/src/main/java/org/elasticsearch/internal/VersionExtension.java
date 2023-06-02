/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.internal;

import java.util.ServiceLoader;

/**
 * Allows plugging in Version.CURRENT.
 */
public interface VersionExtension {
    /**
     * Returns the version id of the current version.
     * Note this cannot return a Version object because it needs to be
     * called during static initialization of Version.
     */
    int getCurrentVersionId();

    /**
     * Loads a single VersionExtension, or returns {@code null} if none are found.
     */
    static VersionExtension load() {
        var loader = ServiceLoader.load(VersionExtension.class);
        var extensions = loader.stream().toList();
        if (extensions.size() > 1) {
            throw new IllegalStateException("More than one version extension found");
        } else if (extensions.size() == 0) {
            return null;
        }
        return extensions.get(0).get();
    }
}
