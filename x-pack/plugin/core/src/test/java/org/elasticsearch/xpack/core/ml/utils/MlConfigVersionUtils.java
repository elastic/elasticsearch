/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.utils;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.KnownMlConfigVersions;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class MlConfigVersionUtils {
    private static final List<MlConfigVersion> ALL_VERSIONS = KnownMlConfigVersions.ALL_VERSIONS;

    /** Returns all released versions */
    public static List<MlConfigVersion> allReleasedVersions() {
        return ALL_VERSIONS;
    }

    /** Returns the oldest known {@link MlConfigVersion} */
    public static MlConfigVersion getFirstVersion() {
        return ALL_VERSIONS.get(0);
    }

    /** Returns a random {@link MlConfigVersion} from all available versions. */
    public static MlConfigVersion randomVersion() {
        return ESTestCase.randomFrom(ALL_VERSIONS);
    }

    /** Returns a random {@link MlConfigVersion} from all available versions without the ignore set */
    public static MlConfigVersion randomVersion(Set<MlConfigVersion> ignore) {
        return ESTestCase.randomFrom(ALL_VERSIONS.stream().filter(v -> ignore.contains(v) == false).collect(Collectors.toList()));
    }

    /** Returns a random {@link MlConfigVersion} from all available versions. */
    public static MlConfigVersion randomVersion(Random random) {
        return ALL_VERSIONS.get(random.nextInt(ALL_VERSIONS.size()));
    }

    /** Returns a random {@link MlConfigVersion} between <code>minVersion</code> and <code>maxVersion</code> (inclusive). */
    public static MlConfigVersion randomVersionBetween(
        Random random,
        @Nullable MlConfigVersion minVersion,
        @Nullable MlConfigVersion maxVersion
    ) {
        if (minVersion != null && maxVersion != null && maxVersion.before(minVersion)) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] cannot be less than minVersion [" + minVersion + "]");
        }

        int minVersionIndex = 0;
        if (minVersion != null) {
            minVersionIndex = Collections.binarySearch(ALL_VERSIONS, minVersion);
        }
        int maxVersionIndex = ALL_VERSIONS.size() - 1;
        if (maxVersion != null) {
            maxVersionIndex = Collections.binarySearch(ALL_VERSIONS, maxVersion);
        }
        if (minVersionIndex < 0) {
            throw new IllegalArgumentException("minVersion [" + minVersion + "] does not exist.");
        } else if (maxVersionIndex < 0) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] does not exist.");
        } else {
            // minVersionIndex is inclusive so need to add 1 to this index
            int range = maxVersionIndex + 1 - minVersionIndex;
            return ALL_VERSIONS.get(minVersionIndex + random.nextInt(range));
        }
    }

    public static MlConfigVersion getPreviousVersion() {
        MlConfigVersion version = getPreviousVersion(MlConfigVersion.CURRENT);
        assert version.before(MlConfigVersion.CURRENT);
        return version;
    }

    public static MlConfigVersion getPreviousVersion(MlConfigVersion version) {
        int place = Collections.binarySearch(ALL_VERSIONS, version);
        if (place < 0) {
            // version does not exist - need the item before the index this version should be inserted
            place = -(place + 1);
        }

        if (place < 1) {
            throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
        }
        return ALL_VERSIONS.get(place - 1);
    }

    public static MlConfigVersion getNextVersion(MlConfigVersion version) {
        int place = Collections.binarySearch(ALL_VERSIONS, version);
        if (place < 0) {
            // version does not exist - need the item at the index this version should be inserted
            place = -(place + 1);
        } else {
            // need the *next* version
            place++;
        }

        if (place < 0 || place >= ALL_VERSIONS.size()) {
            throw new IllegalArgumentException("couldn't find any released versions after [" + version + "]");
        }
        return ALL_VERSIONS.get(place);
    }

    /** Returns a random {@code MlConfigVersion} that is compatible with {@link MlConfigVersion#CURRENT} */
    public static MlConfigVersion randomCompatibleVersion(Random random) {
        return randomVersionBetween(random, MlConfigVersion.MINIMUM_COMPATIBLE, MlConfigVersion.CURRENT);
    }

    /** Returns a random {@code MlConfigVersion} that is compatible with the previous version to {@code version} */
    public static MlConfigVersion randomPreviousCompatibleVersion(Random random, MlConfigVersion version) {
        return randomVersionBetween(random, MlConfigVersion.MINIMUM_COMPATIBLE, getPreviousVersion(version));
    }
}
