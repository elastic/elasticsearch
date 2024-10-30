/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.env;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.internal.BuildExtension;
import org.elasticsearch.plugins.ExtensionLoader;

import java.util.ServiceLoader;

/**
 * A version representing the code of Elasticsearch
 *
 * <p>This class allows us to check whether an Elasticsearch release
 * is "too old" or "too new," using an intentionally minimal API for
 * comparisons. The static {@link #current()} method returns the current
 * release version, and {@link #fromVersionId(int)} returns a version
 * based on some identifier. By default, this identifier matches what the
 * {@link Version} class uses, but the implementation is pluggable.
 * If a module provides a {@link BuildExtension} service via Java SPI, this
 * class's static methods will return a different implementation of {@link BuildVersion},
 * potentially with different behavior. This allows downstream projects to
 * provide versions that accommodate different release models or versioning
 * schemes.</p>
 */
public abstract class BuildVersion {

    /**
     * Check whether this version is on or after a minimum threshold.
     *
     * <p>In some cases, the only thing we need to know about a version is whether
     * it's compatible with the currently-running Elasticsearch. This method checks
     * the lower bound, and returns false if the version is "too old."</p>
     *
     * <p>By default, the minimum compatible version is derived from {@code Version.CURRENT.minimumCompatibilityVersion()},
     * but this behavior is pluggable.</p>
     * @return True if this version is on or after the minimum compatible version
     * for the currently running Elasticsearch, false otherwise.
     */
    public abstract boolean onOrAfterMinimumCompatible();

    /**
     * Check whether this version comes from a release later than the
     * currently running Elasticsearch.
     *
     * <p>This is useful for checking whether a node would be downgraded.</p>
     *
     * @return True if this version represents a release of Elasticsearch later
     * than the one that's running.
     */
    public abstract boolean isFutureVersion();

    /**
     * Create a {@link BuildVersion} from a version ID number.
     *
     * <p>By default, this identifier should match the integer ID of a {@link Version};
     * see that class for details on the default semantic versioning scheme. This behavior
     * is, of course, pluggable.</p>
     *
     * @param versionId An integer identifier for a version
     * @return a version representing a build or release of Elasticsearch
     */
    public static BuildVersion fromVersionId(int versionId) {
        return CurrentExtensionHolder.BUILD_EXTENSION.fromVersionId(versionId);
    }

    /**
     * Create a {@link BuildVersion} from a version string.
     *
     * @param version A string representation of a version
     * @return a version representing a build or release of Elasticsearch
     */
    public static BuildVersion fromString(String version) {
        return CurrentExtensionHolder.BUILD_EXTENSION.fromString(version);
    }

    /**
     * Get the current build version.
     *
     * <p>By default, this value will be different for every public release of Elasticsearch,
     * but downstream implementations aren't restricted by this condition.</p>
     *
     * @return The BuildVersion for Elasticsearch
     */
    public static BuildVersion current() {
        return CurrentExtensionHolder.BUILD_EXTENSION.currentBuildVersion();
    }

    // only exists for NodeMetadata#toXContent
    public abstract int id();

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

        @Override
        public BuildVersion fromString(String version) {
            return new DefaultBuildVersion(version);
        }
    }

}
