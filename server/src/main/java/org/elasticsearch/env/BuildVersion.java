/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.env;

import org.elasticsearch.Version;
import org.elasticsearch.internal.BuildExtension;
import org.elasticsearch.plugins.ExtensionLoader;

import java.util.Objects;
import java.util.ServiceLoader;

public abstract class BuildVersion {

    private static class ExtensionHolder {
        private static final BuildExtension BUILD_EXTENSION = findExtension();

        private static boolean hasExtension() {
            return Objects.nonNull(BUILD_EXTENSION);
        }

        private static BuildExtension findExtension() {
            return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class)).orElse(null);
        }
    }

    public abstract boolean onOrAfterMinimumCompatible();

    public abstract boolean isFutureVersion();

    // temporary
    // TODO[wrb]: remove from PersistedClusterStateService
    // TODO[wrb]: remove from security bootstrap checks
    @Deprecated
    public Version toVersion() {
        return null;
    }

    public static BuildVersion fromVersionId(int versionId) {
        return ExtensionHolder.hasExtension()
            ? ExtensionHolder.BUILD_EXTENSION.fromVersionId(versionId)
            : new DefaultBuildVersion(versionId);
    }

    public static BuildVersion current() {
        return ExtensionHolder.hasExtension() ? ExtensionHolder.BUILD_EXTENSION.currentBuildVersion() : DefaultBuildVersion.CURRENT;
    }

    public static BuildVersion empty() {
        return ExtensionHolder.hasExtension() ? ExtensionHolder.BUILD_EXTENSION.fromVersionId(0) : DefaultBuildVersion.EMPTY;
    }

    // only exists for NodeMetadata#toXContent
    // TODO[wrb]: make this abstract once all downstream classes override it
    protected int id() {
        return -1;
    }
}
