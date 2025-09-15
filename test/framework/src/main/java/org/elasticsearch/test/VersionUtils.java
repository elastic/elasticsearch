/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.Random;

/** Utilities for selecting versions in tests */
public class VersionUtils {

    private static final List<Version> ALL_VERSIONS = Version.getDeclaredVersions(Version.class);

    /**
     * Returns an immutable, sorted list containing all versions, both released and unreleased.
     */
    public static List<Version> allVersions() {
        return ALL_VERSIONS;
    }

    /**
     * Get the version before {@code version}.
     */
    public static Version getPreviousVersion(Version version) {
        for (int i = ALL_VERSIONS.size() - 1; i >= 0; i--) {
            Version v = ALL_VERSIONS.get(i);
            if (v.before(version)) {
                return v;
            }
        }
        throw new IllegalArgumentException("couldn't find any versions before [" + version + "]");
    }

    /**
     * Get the released version before {@link Version#CURRENT}.
     */
    public static Version getPreviousVersion() {
        Version version = getPreviousVersion(Version.CURRENT);
        assert version.before(Version.CURRENT);
        return version;
    }

    /**
     * Returns the {@link Version} before the {@link Version#CURRENT}
     * where the minor version is less than the currents minor version.
     */
    public static Version getPreviousMinorVersion() {
        for (int i = ALL_VERSIONS.size() - 1; i >= 0; i--) {
            Version v = ALL_VERSIONS.get(i);
            if (v.minor < Version.CURRENT.minor || v.major < Version.CURRENT.major) {
                return v;
            }
        }
        throw new IllegalArgumentException("couldn't find any versions of the minor before [" + Build.current().version() + "]");
    }

    /** Returns the oldest {@link Version} */
    public static Version getFirstVersion() {
        return ALL_VERSIONS.get(0);
    }

    /** Returns a random {@link Version} from all available versions. */
    public static Version randomVersion(Random random) {
        return ALL_VERSIONS.get(random.nextInt(ALL_VERSIONS.size()));
    }

    /** Returns a random {@link Version} from all available versions, that is compatible with the given version. */
    public static Version randomCompatibleVersion(Random random, Version version) {
        final List<Version> compatible = ALL_VERSIONS.stream().filter(version::isCompatible).toList();
        return compatible.get(random.nextInt(compatible.size()));
    }

    /** Returns a random {@link Version} between <code>minVersion</code> and <code>maxVersion</code> (inclusive). */
    public static Version randomVersionBetween(Random random, @Nullable Version minVersion, @Nullable Version maxVersion) {
        int minVersionIndex = 0;
        if (minVersion != null) {
            minVersionIndex = ALL_VERSIONS.indexOf(minVersion);
        }
        int maxVersionIndex = ALL_VERSIONS.size() - 1;
        if (maxVersion != null) {
            maxVersionIndex = ALL_VERSIONS.indexOf(maxVersion);
        }
        if (minVersionIndex == -1) {
            throw new IllegalArgumentException("minVersion [" + minVersion + "] does not exist.");
        } else if (maxVersionIndex == -1) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] does not exist.");
        } else if (minVersionIndex > maxVersionIndex) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] cannot be less than minVersion [" + minVersion + "]");
        } else {
            // minVersionIndex is inclusive so need to add 1 to this index
            int range = maxVersionIndex + 1 - minVersionIndex;
            return ALL_VERSIONS.get(minVersionIndex + random.nextInt(range));
        }
    }

    /** returns the first future compatible version */
    public static Version compatibleFutureVersion(Version version) {
        final Optional<Version> opt = ALL_VERSIONS.stream().filter(version::before).filter(v -> v.isCompatible(version)).findAny();
        assert opt.isPresent() : "no future compatible version for " + version;
        return opt.get();
    }

    /** Returns the maximum {@link Version} that is compatible with the given version. */
    public static Version maxCompatibleVersion(Version version) {
        final List<Version> compatible = ALL_VERSIONS.stream().filter(version::isCompatible).filter(version::onOrBefore).toList();
        assert compatible.size() > 0;
        return compatible.get(compatible.size() - 1);
    }
}
