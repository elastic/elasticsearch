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
        private static final BuildExtension BUILD_EXTENSION = findExtension();

        private static boolean hasExtension() {
            return Objects.nonNull(BUILD_EXTENSION);
        }

        private static BuildExtension findExtension() {
            return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class)).orElse(null);
        }
    }

    private static class CurrentHolder {
        private static final BuildVersion CURRENT = findCurrent();

        private static BuildVersion findCurrent() {
            return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class))
                .map(BuildExtension::currentBuildVersion)
                .orElse(DefaultBuildVersion.CURRENT);
        }
    }

    private static class EmptyHolder {
        private static final BuildVersion EMPTY = findEmpty();

        private static BuildVersion findEmpty() {
            return ExtensionLoader.loadSingleton(ServiceLoader.load(BuildExtension.class))
                .map(be -> be.fromVersionId(0))
                .orElse(DefaultBuildVersion.EMPTY);
        }
    }

}
