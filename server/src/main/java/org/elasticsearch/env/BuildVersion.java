/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.env;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.internal.BuildExtension;
import org.elasticsearch.plugins.ExtensionLoader;

import java.util.ServiceLoader;

public abstract class BuildVersion {

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
        return CurrentExtensionHolder.BUILD_EXTENSION.fromVersionId(versionId);
    }

    public static BuildVersion current() {
        return CurrentExtensionHolder.BUILD_EXTENSION.currentBuildVersion();
    }

    // only exists for NodeMetadata#toXContent
    // TODO[wrb]: make this abstract once all downstream classes override it
    protected int id() {
        return -1;
    }

    private static class CurrentExtensionHolder {
        private static final BuildExtension BUILD_EXTENSION = findExtension();

        private static BuildExtension findExtension() {
            return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class)).orElse(new DefaultBuildExtension());
        }
    }

    private static class DefaultBuildExtension implements BuildExtension {
        @Override
        public Build getCurrentBuild() {
            return Build.current();
        }

        @Override
        public BuildVersion currentBuildVersion() {
            return DefaultBuildVersion.CURRENT;
        }

        @Override
        public BuildVersion fromVersionId(int versionId) {
            return new DefaultBuildVersion(versionId);
        }
    }

}
