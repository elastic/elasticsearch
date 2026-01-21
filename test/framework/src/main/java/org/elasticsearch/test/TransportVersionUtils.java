/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.elasticsearch.TransportVersion;

import java.util.Collections;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static org.apache.lucene.tests.util.LuceneTestCase.random;

public class TransportVersionUtils {

    private static final NavigableSet<TransportVersion> RELEASED_VERSIONS = Collections.unmodifiableNavigableSet(
        new TreeSet<>(TransportVersion.getAllVersions())
    );
    private static final NavigableSet<TransportVersion> NON_PATCH_VERSIONS = Collections.unmodifiableNavigableSet(
        // Exclude patch versions since they break the semantics of methods like `randomVersionBetween`
        TransportVersion.getAllVersions()
            .stream()
            .filter(not(TransportVersionUtils::isPatchVersion))
            .collect(Collectors.toCollection(TreeSet::new))
    );

    /** Returns all released versions */
    public static NavigableSet<TransportVersion> allReleasedVersions() {
        return RELEASED_VERSIONS;
    }

    /** Returns a random {@link TransportVersion} from all available versions. */
    public static TransportVersion randomVersion() {
        return RandomPicks.randomFrom(random(), allReleasedVersions());
    }

    /** Returns a random {@link TransportVersion} from all available versions without the ignore set */
    public static TransportVersion randomVersion(Set<TransportVersion> ignore) {
        return ESTestCase.randomFrom(allReleasedVersions().stream().filter(v -> ignore.contains(v) == false).collect(Collectors.toList()));
    }

    /**
     * Returns a random {@link TransportVersion} which supports the given version. Effectively, this returns a version equal to, or "later"
     * than the given version.
     */
    public static TransportVersion randomVersionSupporting(TransportVersion minVersion) {
        return RandomPicks.randomFrom(random(), RELEASED_VERSIONS.stream().filter(v -> v.supports(minVersion)).toList());
    }

    /**
     * Returns a random {@link TransportVersion} which does not supports the given version. Effectively, this returns a version "before"
     * the given version.
     */
    public static TransportVersion randomVersionNotSupporting(TransportVersion version) {
        return RandomPicks.randomFrom(random(), RELEASED_VERSIONS.stream().filter(v -> v.supports(version) == false).toList());
    }

    public static TransportVersion getPreviousVersion(TransportVersion version) {
        return getPreviousVersion(version, false);
    }

    public static TransportVersion getPreviousVersion(TransportVersion version, boolean createIfNecessary) {
        TransportVersion lower = (isPatchVersion(version) ? RELEASED_VERSIONS : NON_PATCH_VERSIONS).lower(version);
        if (lower == null) {
            if (createIfNecessary) {
                // create a new transport version one less than specified
                return new TransportVersion(version.id() - 1);
            } else {
                throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
            }
        }
        return lower;
    }

    public static TransportVersion getNextVersion(TransportVersion version) {
        return getNextVersion(version, false);
    }

    public static TransportVersion getNextVersion(TransportVersion version, boolean createIfNecessary) {
        TransportVersion higher = (isPatchVersion(version) ? RELEASED_VERSIONS : NON_PATCH_VERSIONS).higher(version);
        if (higher != null && isPatchVersion(version) && isPatchVersion(higher) == false) {
            throw new IllegalStateException(
                "couldn't determine next version as version [" + version + "] is  patch, and is the newest patch"
            );
        }

        if (higher == null) {
            if (createIfNecessary) {
                // create a new transport version one greater than specified
                return new TransportVersion(version.id() + 1);
            } else {
                throw new IllegalArgumentException("couldn't find any released versions after [" + version + "]");
            }
        }
        return higher;
    }

    /** Returns a random {@code TransportVersion} that is compatible with {@link TransportVersion#current()} */
    public static TransportVersion randomCompatibleVersion() {
        return randomCompatibleVersion(true);
    }

    /** Returns a random {@code TransportVersion} that is compatible with {@link TransportVersion#current()} */
    public static TransportVersion randomCompatibleVersion(boolean includePatches) {
        return RandomPicks.randomFrom(
            random(),
            (includePatches ? RELEASED_VERSIONS : NON_PATCH_VERSIONS).stream().filter(TransportVersion::isCompatible).toList()
        );
    }

    /**
     * Returns {@code true} if the given version is a patch version. Transport versions are generally monotoic, that is, when comparing
     * transport versions via {@link TransportVersion#compareTo(TransportVersion)} a later version is also temporally "newer". This,
     * however, is not always true for patch versions, as they can be introduced at any time. There may be instances where this distinction
     * is important, in which case this method can be used to determine if a version is a patch, and therefore, may actually be temporally
     * newer than "later" versions.
     *
     * @return whether this version is a patch version.
     */
    private static boolean isPatchVersion(TransportVersion version) {
        return version.id() % 100 != 0;
    }
}
