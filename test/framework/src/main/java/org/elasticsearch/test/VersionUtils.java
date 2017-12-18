/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test;

import org.elasticsearch.Version;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

/** Utilities for selecting versions in tests */
public class VersionUtils {
    /**
     * Sort versions that have backwards compatibility guarantees from
     * those that don't. Doesn't actually check whether or not the versions
     * are released, instead it relies on gradle to have already checked
     * this which it does in {@code :core:verifyVersions}. So long as the
     * rules here match up with the rules in gradle then this should
     * produce sensible results.
     * @return a tuple containing versions with backwards compatibility
     * guarantees in v1 and versions without the guranteees in v2
     */
    static Tuple<List<Version>, List<Version>> resolveReleasedVersions(Version current, Class<?> versionClass) {
        List<Version> versions = Version.getDeclaredVersions(versionClass);

        if (!Booleans.parseBoolean(System.getProperty("build.snapshot", "true"))) {
            return Tuple.tuple(versions, Collections.emptyList());
        }

        Version last = versions.remove(versions.size() - 1);
        assert last.equals(current) : "The highest version must be the current one "
            + "but was [" + last + "] and current was [" + current + "]";

        /* In the 5.x series prior to 5.6, unreleased version constants had an
         * `_UNRELEASED` suffix, and when making the first release on a minor release
         * branch the last, unreleased, version constant from the previous minor branch
         * was dropped. After 5.6, there is no `_UNRELEASED` suffix on version constants'
         * names and, additionally, they are not dropped when a new minor release branch
         * starts.
         *
         * This means that in 6.x and later series the last release _in each
         * minor branch_ is unreleased, whereas in 5.x it's more complicated: There were
         * (sometimes, and sometimes multiple) minor branches containing no releases, each
         * of which contains a single version constant of the form 5.n.0, and these
         * branches always followed a branch that _did_ contain a version of the
         * form 5.m.p (p>0). All versions strictly before the last 5.m version are released,
         * and all other 5.* versions are unreleased.
         */

        if (current.major == 5 && current.revision != 0) {
            /* The current (i.e. latest) version is 5.a.b, b nonzero, which
             * means that all other versions are released. */
            return new Tuple<>(unmodifiableList(versions), singletonList(current));
        }

        final List<Version> unreleased = new ArrayList<>();
        unreleased.add(current);
        Version prevConsideredVersion = current;

        for (int i = versions.size() - 1; i >= 0; i--) {
            Version currConsideredVersion = versions.get(i);
            if (currConsideredVersion.major == 5) {
                unreleased.add(currConsideredVersion);
                versions.remove(i);
                if (currConsideredVersion.revision != 0) {
                    /* Currently considering the latest version in the 5.x series,
                     * which is (a) unreleased and (b) the only such. So we're done. */
                    break;
                }
                /* ... else we're on a version of the form 5.n.0, and have not yet
                 * considered a version of the form 5.n.m (m>0), so this entire branch
                 * is unreleased, so carry on looking for a branch containing releases.
                 */
            } else if (currConsideredVersion.major != prevConsideredVersion.major
                || currConsideredVersion.minor != prevConsideredVersion.minor) {
                /* Have moved to the end of a new minor branch, so this is
                 * an unreleased version. */
                unreleased.add(currConsideredVersion);
                versions.remove(i);
            }
            prevConsideredVersion = currConsideredVersion;

        }

        Collections.reverse(unreleased);
        return new Tuple<>(unmodifiableList(versions), unmodifiableList(unreleased));
    }

    private static final List<Version> RELEASED_VERSIONS;
    private static final List<Version> UNRELEASED_VERSIONS;
    private static final List<Version> ALL_VERSIONS;

    static {
        Tuple<List<Version>, List<Version>> versions = resolveReleasedVersions(Version.CURRENT, Version.class);
        RELEASED_VERSIONS = versions.v1();
        UNRELEASED_VERSIONS = versions.v2();
        List<Version> allVersions = new ArrayList<>(RELEASED_VERSIONS.size() + UNRELEASED_VERSIONS.size());
        allVersions.addAll(RELEASED_VERSIONS);
        allVersions.addAll(UNRELEASED_VERSIONS);
        Collections.sort(allVersions);
        ALL_VERSIONS = unmodifiableList(allVersions);
    }

    /**
     * Returns an immutable, sorted list containing all released versions.
     */
    public static List<Version> allReleasedVersions() {
        return RELEASED_VERSIONS;
    }

    /**
     * Returns an immutable, sorted list containing all unreleased versions.
     */
    public static List<Version> allUnreleasedVersions() {
        return UNRELEASED_VERSIONS;
    }

    /**
     * Returns an immutable, sorted list containing all versions, both released and unreleased.
     */
    public static List<Version> allVersions() {
        return ALL_VERSIONS;
    }

    /**
     * Get the released version before {@code version}.
     */
    public static Version getPreviousVersion(Version version) {
        for (int i = RELEASED_VERSIONS.size() - 1; i >= 0; i--) {
            Version v = RELEASED_VERSIONS.get(i);
            if (v.before(version)) {
                return v;
            }
        }
        throw new IllegalArgumentException("couldn't find any released versions before [" + version + "]");
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
     * Returns the released {@link Version} before the {@link Version#CURRENT}
     * where the minor version is less than the currents minor version.
     */
    public static Version getPreviousMinorVersion() {
        for (int i = RELEASED_VERSIONS.size() - 1; i >= 0; i--) {
            Version v = RELEASED_VERSIONS.get(i);
            if (v.minor < Version.CURRENT.minor || v.major < Version.CURRENT.major) {
                return v;
            }
        }
        throw new IllegalArgumentException("couldn't find any released versions of the minor before [" + Version.CURRENT + "]");
    }

    /** Returns the oldest released {@link Version} */
    public static Version getFirstVersion() {
        return RELEASED_VERSIONS.get(0);
    }

    /** Returns a random {@link Version} from all available versions. */
    public static Version randomVersion(Random random) {
        return ALL_VERSIONS.get(random.nextInt(ALL_VERSIONS.size()));
    }

    /** Returns a random {@link Version} from all available versions, that is compatible with the given version. */
    public static Version randomCompatibleVersion(Random random, Version version) {
        final List<Version> compatible = ALL_VERSIONS.stream().filter(version::isCompatible).collect(Collectors.toList());
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

    /** returns the first future incompatible version */
    public static Version incompatibleFutureVersion(Version version) {
        final Optional<Version> opt = ALL_VERSIONS.stream().filter(version::before).filter(v -> v.isCompatible(version) == false).findAny();
        assert opt.isPresent() : "no future incompatible version for " + version;
        return opt.get();
    }

    /** Returns the maximum {@link Version} that is compatible with the given version. */
    public static Version maxCompatibleVersion(Version version) {
        final List<Version> compatible = ALL_VERSIONS.stream().filter(version::isCompatible).filter(version::onOrBefore)
            .collect(Collectors.toList());
        assert compatible.size() > 0;
        return compatible.get(compatible.size() - 1);
    }

}
