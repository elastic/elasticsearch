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

import java.util.Optional;
import java.util.ServiceLoader;
import java.util.function.IntFunction;

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
        return IdToBuildVersionHolder.ID_TO_BUILD_VERSION_FUNCTION.apply(versionId);
    }

    public static BuildVersion current() {
        return CurrentHolder.CURRENT;
    }

    public static BuildVersion empty() {
        return EmptyHolder.EMPTY;
    }

    // only exists for NodeMetadata#toXContent
    // TODO[wrb]: make this abstract once all downstream classes override it
    protected int id() {
        return -1;
    }

    private static class ExtensionHolder {
        private static final Optional<BuildExtension> BUILD_EXTENSION = findExtension();

        private static Optional<BuildExtension> findExtension() {
            return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class));
        }
    }

    private static class IdToBuildVersionHolder {
        private static final IntFunction<BuildVersion> ID_TO_BUILD_VERSION_FUNCTION = findIdToBuildVersionFunction();

        private static IntFunction<BuildVersion> findIdToBuildVersionFunction() {
            return ExtensionHolder.BUILD_EXTENSION.map(be -> (IntFunction<BuildVersion>) be::fromVersionId)
                .orElse(DefaultBuildVersion::new);
        }
    }

    private static class CurrentHolder {
        private static final BuildVersion CURRENT = findCurrent();

        private static BuildVersion findCurrent() {
            return ExtensionHolder.BUILD_EXTENSION.map(BuildExtension::currentBuildVersion).orElse(DefaultBuildVersion.CURRENT);
        }
    }

    private static class EmptyHolder {
        private static final BuildVersion EMPTY = findEmpty();

        private static BuildVersion findEmpty() {
            return ExtensionHolder.BUILD_EXTENSION.map(be -> be.fromVersionId(0)).orElse(DefaultBuildVersion.EMPTY);
        }
    }

}
