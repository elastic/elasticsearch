/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.index;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.KnownIndexVersions;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class IndexVersionUtils {

    private static final List<IndexVersion> ALL_VERSIONS = KnownIndexVersions.ALL_VERSIONS;

    /** Returns all released versions */
    public static List<IndexVersion> allReleasedVersions() {
        return ALL_VERSIONS;
    }

    /** Returns the oldest known {@link IndexVersion} */
    public static IndexVersion getFirstVersion() {
        return ALL_VERSIONS.get(0);
    }

    /** Returns a random {@link IndexVersion} from all available versions. */
    public static IndexVersion randomVersion() {
        return ESTestCase.randomFrom(ALL_VERSIONS);
    }

    /** Returns a random {@link IndexVersion} from all available versions without the ignore set */
    public static IndexVersion randomVersion(Set<IndexVersion> ignore) {
        return ESTestCase.randomFrom(ALL_VERSIONS.stream().filter(v -> ignore.contains(v) == false).collect(Collectors.toList()));
    }

    /** Returns a random {@link IndexVersion} from all available versions. */
    public static IndexVersion randomVersion(Random random) {
        return ALL_VERSIONS.get(random.nextInt(ALL_VERSIONS.size()));
    }

    /** Returns a random {@link IndexVersion} between <code>minVersion</code> and <code>maxVersion</code> (inclusive). */
    public static IndexVersion randomVersionBetween(Random random, @Nullable IndexVersion minVersion, @Nullable IndexVersion maxVersion) {
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

    public static IndexVersion getPreviousVersion() {
        IndexVersion version = getPreviousVersion(IndexVersion.CURRENT);
        assert version.before(IndexVersion.CURRENT);
        return version;
    }

    public static IndexVersion getPreviousVersion(IndexVersion version) {
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

    public static IndexVersion getNextVersion(IndexVersion version) {
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

    /** Returns a random {@code IndexVersion} that is compatible with {@link IndexVersion#CURRENT} */
    public static IndexVersion randomCompatibleVersion(Random random) {
        return randomVersionBetween(random, IndexVersion.MINIMUM_COMPATIBLE, IndexVersion.CURRENT);
    }
}
