/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.index;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.KnownIndexVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.lucene.tests.util.LuceneTestCase.random;

public class IndexVersionUtils {

    private static final NavigableSet<IndexVersion> ALL_VERSIONS = KnownIndexVersions.ALL_VERSIONS;
    private static final NavigableSet<IndexVersion> ALL_WRITE_VERSIONS = KnownIndexVersions.ALL_WRITE_VERSIONS;

    /** Returns all released versions */
    public static NavigableSet<IndexVersion> allReleasedVersions() {
        return ALL_VERSIONS;
    }

    /** Returns the oldest known {@link IndexVersion}. This version can only be read from and not written to */
    public static IndexVersion getLowestReadCompatibleVersion() {
        return ALL_VERSIONS.getFirst();
    }

    /** Returns the oldest known {@link IndexVersion} that can be written to */
    public static IndexVersion getLowestWriteCompatibleVersion() {
        return ALL_WRITE_VERSIONS.getFirst();
    }

    /** Returns a random {@link IndexVersion} from all available versions. */
    public static IndexVersion randomVersion() {
        return VersionUtils.randomFrom(random(), ALL_VERSIONS, IndexVersion::fromId);
    }

    /** Returns a random {@link IndexVersion} from all versions that can be written to. */
    public static IndexVersion randomWriteVersion() {
        return VersionUtils.randomFrom(random(), ALL_WRITE_VERSIONS, IndexVersion::fromId);
    }

    /** Returns a random {@link IndexVersion} from all available versions without the ignore set */
    public static IndexVersion randomVersion(Set<IndexVersion> ignore) {
        return ESTestCase.randomFrom(ALL_VERSIONS.stream().filter(v -> ignore.contains(v) == false).collect(Collectors.toList()));
    }

    /** Returns a random {@link IndexVersion} between <code>minVersion</code> and <code>maxVersion</code> (inclusive). */
    public static IndexVersion randomVersionBetween(Random random, @Nullable IndexVersion minVersion, @Nullable IndexVersion maxVersion) {
        if (minVersion != null && maxVersion != null && maxVersion.before(minVersion)) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] cannot be less than minVersion [" + minVersion + "]");
        }

        NavigableSet<IndexVersion> versions = allReleasedVersions();
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

        return VersionUtils.randomFrom(random, versions, IndexVersion::fromId);
    }

    public static IndexVersion getPreviousVersion() {
        IndexVersion version = getPreviousVersion(IndexVersion.current());
        assert version.before(IndexVersion.current());
        return version;
    }

    public static IndexVersion getPreviousVersion(IndexVersion version) {
        IndexVersion lower = allReleasedVersions().lower(version);
        if (lower == null) {
            throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
        }
        return lower;
    }

    public static IndexVersion getPreviousMajorVersion(IndexVersion version) {
        return IndexVersion.getMinimumCompatibleIndexVersion(version.id());
    }

    public static IndexVersion getNextVersion(IndexVersion version) {
        IndexVersion higher = allReleasedVersions().higher(version);
        if (higher == null) {
            throw new IllegalArgumentException("couldn't find any released versions after [" + version + "]");
        }
        return higher;
    }

    /** Returns a random {@code IndexVersion} that is compatible with {@link IndexVersion#current()} */
    public static IndexVersion randomCompatibleVersion(Random random) {
        return randomVersionBetween(random, IndexVersions.MINIMUM_READONLY_COMPATIBLE, IndexVersion.current());
    }

    /** Returns a random {@code IndexVersion} that is compatible with {@link IndexVersion#current()} and can be written to */
    public static IndexVersion randomCompatibleWriteVersion(Random random) {
        return randomVersionBetween(random, IndexVersions.MINIMUM_COMPATIBLE, IndexVersion.current());
    }

    /** Returns a random {@code IndexVersion} that is compatible with the previous version to {@code version} */
    public static IndexVersion randomPreviousCompatibleVersion(Random random, IndexVersion version) {
        return randomVersionBetween(random, IndexVersions.MINIMUM_READONLY_COMPATIBLE, getPreviousVersion(version));
    }

    /** Returns a random {@code IndexVersion} that is compatible with the previous version to {@code version} and can be written to */
    public static IndexVersion randomPreviousCompatibleWriteVersion(Random random, IndexVersion version) {
        return randomVersionBetween(random, IndexVersions.MINIMUM_COMPATIBLE, getPreviousVersion(version));
    }
}
