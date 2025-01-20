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

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportVersionUtils {
    /** Returns all released versions */
    public static List<TransportVersion> allReleasedVersions() {
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
        return allReleasedVersions().get(random.nextInt(allReleasedVersions().size()));
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

        int minVersionIndex = 0;
        List<TransportVersion> allReleasedVersions = allReleasedVersions();
        if (minVersion != null) {
            minVersionIndex = Collections.binarySearch(allReleasedVersions, minVersion);
        }
        int maxVersionIndex = allReleasedVersions.size() - 1;
        if (maxVersion != null) {
            maxVersionIndex = Collections.binarySearch(allReleasedVersions, maxVersion);
        }
        if (minVersionIndex < 0) {
            throw new IllegalArgumentException("minVersion [" + minVersion + "] does not exist.");
        } else if (maxVersionIndex < 0) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] does not exist.");
        } else {
            // minVersionIndex is inclusive so need to add 1 to this index
            int range = maxVersionIndex + 1 - minVersionIndex;
            return allReleasedVersions.get(minVersionIndex + random.nextInt(range));
        }
    }

    public static TransportVersion getPreviousVersion() {
        TransportVersion version = getPreviousVersion(TransportVersion.current());
        assert version.before(TransportVersion.current());
        return version;
    }

    public static TransportVersion getPreviousVersion(TransportVersion version) {
        int place = Collections.binarySearch(allReleasedVersions(), version);
        if (place < 0) {
            // version does not exist - need the item before the index this version should be inserted
            place = -(place + 1);
        }

        if (place < 1) {
            throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
        }
        return allReleasedVersions().get(place - 1);
    }

    public static TransportVersion getNextVersion(TransportVersion version) {
        return getNextVersion(version, false);
    }

    public static TransportVersion getNextVersion(TransportVersion version, boolean createIfNecessary) {
        List<TransportVersion> allReleasedVersions = allReleasedVersions();
        int place = Collections.binarySearch(allReleasedVersions, version);
        if (place < 0) {
            // version does not exist - need the item at the index this version should be inserted
            place = -(place + 1);
        } else {
            // need the *next* version
            place++;
        }

        if (place < 0 || place >= allReleasedVersions.size()) {
            if (createIfNecessary) {
                // create a new transport version one greater than specified
                return new TransportVersion(version.id() + 1);
            } else {
                throw new IllegalArgumentException("couldn't find any released versions after [" + version + "]");
            }
        }
        return allReleasedVersions.get(place);
    }

    /** Returns a random {@code TransportVersion} that is compatible with {@link TransportVersion#current()} */
    public static TransportVersion randomCompatibleVersion(Random random) {
        return randomVersionBetween(random, TransportVersions.MINIMUM_COMPATIBLE, TransportVersion.current());
    }
}
