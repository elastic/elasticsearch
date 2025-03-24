/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.core.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.IntFunction;

/** Utilities for selecting versions in tests */
public class VersionUtils {

    private static final NavigableSet<Version> ALL_VERSIONS = Collections.unmodifiableNavigableSet(
        new TreeSet<>(Version.getDeclaredVersions(Version.class))
    );

    /**
     * Returns an immutable, sorted list containing all versions, both released and unreleased.
     */
    public static NavigableSet<Version> allVersions() {
        return ALL_VERSIONS;
    }

    /**
     * Get the version before {@code version}.
     */
    public static Version getPreviousVersion(Version version) {
        var versions = ALL_VERSIONS.headSet(version, false);
        if (versions.isEmpty()) {
            throw new IllegalArgumentException("couldn't find any versions before [" + version + "]");
        }
        return versions.getLast();
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
        for (Version v : ALL_VERSIONS.descendingSet()) {
            if (v.minor < Version.CURRENT.minor || v.major < Version.CURRENT.major) {
                return v;
            }
        }
        throw new IllegalArgumentException("couldn't find any versions of the minor before [" + Build.current().version() + "]");
    }

    /** Returns the oldest {@link Version} */
    public static Version getFirstVersion() {
        return ALL_VERSIONS.getFirst();
    }

    /** Returns a random {@link Version} from all available versions. */
    public static Version randomVersion(Random random) {
        return randomFrom(random, ALL_VERSIONS, Version::fromId);
    }

    /** Returns a random {@link Version} from all available versions, that is compatible with the given version. */
    public static Version randomCompatibleVersion(Random random, Version version) {
        final List<Version> compatible = ALL_VERSIONS.stream().filter(version::isCompatible).toList();
        return compatible.get(random.nextInt(compatible.size()));
    }

    /** Returns a random {@link Version} between <code>minVersion</code> and <code>maxVersion</code> (inclusive). */
    public static Version randomVersionBetween(Random random, @Nullable Version minVersion, @Nullable Version maxVersion) {
        if (minVersion != null && maxVersion != null && maxVersion.before(minVersion)) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] cannot be less than minVersion [" + minVersion + "]");
        }

        NavigableSet<Version> versions = ALL_VERSIONS;
        if (minVersion != null) {
            if (versions.contains(minVersion) == false) {
                throw new IllegalArgumentException("minVersion [" + minVersion + "] does not exist.");
            }
            versions = versions.tailSet(minVersion, true);
        }
        if (maxVersion != null) {
            if (versions.contains(maxVersion) == false) {
                throw new IllegalArgumentException("maxVersion [" + maxVersion + "] does not exist.");
            }
            versions = versions.headSet(maxVersion, true);
        }

        return randomFrom(random, versions, Version::fromId);
    }

    /** Returns the maximum {@link Version} that is compatible with the given version. */
    public static Version maxCompatibleVersion(Version version) {
        return ALL_VERSIONS.tailSet(version, true).descendingSet().stream().filter(version::isCompatible).findFirst().orElseThrow();
    }

    public static <T extends VersionId<T>> T randomFrom(Random random, NavigableSet<T> set, IntFunction<T> ctor) {
        // get the first and last id, pick a random id in the middle, then find that id in the set in O(nlogn) time
        // this assumes the id numbers are reasonably evenly distributed in the set
        assert set.isEmpty() == false;
        int lowest = set.getFirst().id();
        int highest = set.getLast().id();

        T randomId = ctor.apply(RandomNumbers.randomIntBetween(random, lowest, highest));
        // try to find the id below, then the id above. We're just looking for *some* item in the set that is close to randomId
        T found = set.floor(randomId);
        return found != null ? found : set.ceiling(randomId);
    }
}
