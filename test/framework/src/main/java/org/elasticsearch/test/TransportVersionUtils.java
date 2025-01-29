/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.core.Nullable;

import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportVersionUtils {
    /** Returns all released versions */
    public static NavigableSet<TransportVersion> allReleasedVersions() {
        return TransportVersion.getAllVersions();
    }

    /** Returns the oldest known {@link TransportVersion} */
    public static TransportVersion getFirstVersion() {
        return allReleasedVersions().getFirst();
    }

    /** Returns a random {@link TransportVersion} from all available versions. */
    public static TransportVersion randomVersion() {
        return ESTestCase.randomFrom(allReleasedVersions());
    }

    /** Returns a random {@link TransportVersion} from all available versions without the ignore set */
    public static TransportVersion randomVersion(Set<TransportVersion> ignore) {
        return ESTestCase.randomFrom(allReleasedVersions().stream().filter(v -> ignore.contains(v) == false).collect(Collectors.toList()));
    }

    /** Returns a random {@link TransportVersion} from all available versions. */
    public static TransportVersion randomVersion(Random random) {
        return ESTestCase.randomFrom(random, allReleasedVersions());
    }

    /** Returns a random {@link TransportVersion} between <code>minVersion</code> and <code>maxVersion</code> (inclusive). */
    public static TransportVersion randomVersionBetween(
        Random random,
        @Nullable TransportVersion minVersion,
        @Nullable TransportVersion maxVersion
    ) {
        if (minVersion != null && maxVersion != null && maxVersion.before(minVersion)) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] cannot be less than minVersion [" + minVersion + "]");
        }

        NavigableSet<TransportVersion> versions = allReleasedVersions();
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

        return ESTestCase.randomFrom(random, versions);
    }

    public static TransportVersion getPreviousVersion() {
        TransportVersion version = getPreviousVersion(TransportVersion.current());
        assert version.before(TransportVersion.current());
        return version;
    }

    public static TransportVersion getPreviousVersion(TransportVersion version) {
        var versions = allReleasedVersions().headSet(version, false);
        if (versions.isEmpty()) {
            throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
        }
        return versions.getLast();
    }

    public static TransportVersion getNextVersion(TransportVersion version) {
        return getNextVersion(version, false);
    }

    public static TransportVersion getNextVersion(TransportVersion version, boolean createIfNecessary) {
        var versions = allReleasedVersions().tailSet(version, false);
        if (versions.isEmpty()) {
            if (createIfNecessary) {
                // create a new transport version one greater than specified
                return new TransportVersion(version.id() + 1);
            } else {
                throw new IllegalArgumentException("couldn't find any released versions after [" + version + "]");
            }
        }
        return versions.getFirst();
    }

    /** Returns a random {@code TransportVersion} that is compatible with {@link TransportVersion#current()} */
    public static TransportVersion randomCompatibleVersion(Random random) {
        return randomVersionBetween(random, TransportVersions.MINIMUM_COMPATIBLE, TransportVersion.current());
    }
}
