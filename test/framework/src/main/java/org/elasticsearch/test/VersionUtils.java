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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

/** Utilities for selecting versions in tests */
public class VersionUtils {
    // this will need to be set to false once the branch has no BWC candidates (ie, its 2 major releases behind a released version)
    private static final boolean isReleasableBranch = true;

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
        TreeSet<Version> releasedVersions = new TreeSet<>(Version.getDeclaredVersions(versionClass));
        List<Version> unreleasedVersions = new ArrayList<>();

        assert releasedVersions.last().equals(current) : "The highest version must be the current one "
            + "but was [" + releasedVersions.last() + "] and current was [" + current + "]";

        List<Version> temporarilyRemovedReleases = new ArrayList<>();

        for (Version version: releasedVersions) {
            if (version.isRelease() == false && isMajorReleased(version, releasedVersions)) {
                // remove the Alpha/Beta/RC temporarily for already released versions
                temporarilyRemovedReleases.add(version);
            } else if (version.isRelease() == false
                && version.major == current.major
                && version.minor == current.minor
                && version.revision == current.revision
                && version.build != current.build) {
                // remove the Alpha/Beta/RC temporarily for the current version
                temporarilyRemovedReleases.add(version);
            }
        }
        releasedVersions.removeAll(temporarilyRemovedReleases);
        releasedVersions.remove(current);

        if (isReleasableBranch) {
            if (isReleased(current)) {
                // if the minor has been released then it only has a maintenance version
                // go back 1 version to get the last supported snapshot version of the line, which is a maint bugfix
                Version highestMinor = getHighestPreviousMinor(current.major, releasedVersions);
                releasedVersions.remove(highestMinor);
                unreleasedVersions.add(highestMinor);
            } else {
                // if our currentVersion is a X.0.0, we need to check X-1 minors to see if they are released
                if (current.minor == 0) {
                    boolean hasFoundNextMinor = false;
                    boolean hasFoundStagedMinor = false;
                    for (Version version: getMinorTips(current.major - 1, releasedVersions)) {
                        if (isReleased(version) == false) {
                            // This should only ever contain 2 non released branches in flight. An example is 6.x is frozen,
                            // and 6.2 is cut but not yet released there is some simple logic to make sure that in the case of more than 2,
                            // it will bail. The order is that the minor snapshot is fufilled first, and then the staged minor snapshot
                            if (hasFoundNextMinor == false) {
                                hasFoundNextMinor = true;
                                releasedVersions.remove(version);
                                unreleasedVersions.add(version);
                            } else if (hasFoundStagedMinor == false) {
                                hasFoundStagedMinor = true;
                                releasedVersions.remove(version);
                                unreleasedVersions.add(version);
                            } else {
                                throw new IllegalArgumentException(
                                    "More than 2 snapshot version existed for the next minor and staged (frozen) minors.");
                            }
                        } else {
                            // this is the last minor snap for this major, so replace the highest (last) one of these and break
                            releasedVersions.remove(version);
                            unreleasedVersions.add(version);
                            // we only care about the largest minor here, so in the case of 6.1 and 6.0, it will only get 6.1
                            break;
                        }
                    }
                    // now dip back 2 versions to get the last supported snapshot version of the line
                    Version highestMinor = getHighestPreviousMinor(current.major - 1, releasedVersions);
                    releasedVersions.remove(highestMinor);
                    unreleasedVersions.add(highestMinor);
                } else {
                    // version is not a X.0.0, so we are somewhere on a X.Y line only check till minor == 0 of the major
                    boolean hasFoundStagedMinor = false;
                    for (Version version: getMinorTips(current.major, releasedVersions)) {
                        if (isReleased(version) == false) {
                            // This should only ever contain 0 or 1 branch in flight. An example is 6.x is frozen, and 6.2 is cut
                            // but not yet released there is some simple logic to make sure that in the case of more than 1, it will bail
                            if (hasFoundStagedMinor == false) {
                                hasFoundStagedMinor = true;
                                releasedVersions.remove(version);
                                unreleasedVersions.add(version);
                            } else {
                                throw new IllegalArgumentException("More than 1 snapshot version existed for the staged (frozen) minors.");
                            }
                        } else {
                            // this is the last minor snap for this major, so replace the highest (last) one of these and break
                            releasedVersions.remove(version);
                            unreleasedVersions.add(version);
                            // we only care about the largest minor here, so in the case of 6.1 and 6.0, it will only get 6.1
                            break;
                        }
                    }
                    // now dip back 1 version to get the last supported snapshot version of the line
                    Version highestMinor = getHighestPreviousMinor(current.major, releasedVersions);
                    releasedVersions.remove(highestMinor);
                    unreleasedVersions.add(highestMinor);
                }
            }
        }

        // re-add the Alpha/Beta/RC
        releasedVersions.addAll(temporarilyRemovedReleases);
        unreleasedVersions.add(current);

        Collections.sort(unreleasedVersions);
        return new Tuple<>(new ArrayList<>(releasedVersions), unreleasedVersions);
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

    private static Version generateVersion(int major, int minor, int revision) {
        return Version.fromString(String.format(Locale.ROOT, "%s.%s.%s", major, minor, revision));
    }

    /**
     * Uses basic logic about our releases to determine if this version has been previously released
     */
    private static boolean isReleased(Version version) {
        return version.revision > 0;
    }

    /**
     * Validates that the count of non suffixed (alpha/beta/rc) versions in a given major to major+1 is greater than 1.
     * This means that there is more than just a major.0.0 or major.0.0-alpha in a branch to signify it has been prevously released.
     */
    private static boolean isMajorReleased(Version version, TreeSet<Version> items) {
        return getMajorSet(version.major, items)
            .stream()
            .map(v -> v.isRelease())
            .count() > 1;
    }

    /**
     * Gets the largest version previous major version based on the nextMajorVersion passed in.
     * If you have a list [5.0.2, 5.1.2, 6.0.1, 6.1.1] and pass in 6 for the nextMajorVersion, it will return you 5.1.2
     */
    private static Version getHighestPreviousMinor(int majorVersion, TreeSet<Version> items){
        return items.headSet(generateVersion(majorVersion, 0, 0)).last();
    }

    /**
     * Gets the entire set of major.minor.* given those parameters.
     */
    private static SortedSet<Version> getMinorSetForMajor(int major, int minor, TreeSet<Version> items) {
        return items
            .tailSet(generateVersion(major, minor, 0))
            .headSet(generateVersion(major, minor + 1, 0));
    }

    /**
     * Gets the entire set of major.* to the currentVersion
     */
    private static SortedSet<Version> getMajorSet(int major, TreeSet<Version> items) {
        return items
            .tailSet(generateVersion(major, 0, 0))
            .headSet(generateVersion(major + 1, 0, 0));
    }

    /**
     * Gets the tip of each minor set and puts it in a list.
     *
     * examples:
     *  [1.0.0, 1.1.0, 1.1.1, 1.2.0, 1.3.1] will return [1.0.0, 1.1.1, 1.2.0, 1.3.1]
     *  [1.0.0, 1.0.1, 1.0.2, 1.0.3, 1.0.4] will return [1.0.4]
     */
    private static List<Version> getMinorTips(int major, TreeSet<Version> items) {
        SortedSet<Version> majorSet = getMajorSet(major, items);
        List<Version> minorList = new ArrayList<>();
        for (int minor = majorSet.last().minor; minor >= 0; minor--) {
            SortedSet<Version> minorSetInMajor = getMinorSetForMajor(major, minor, items);
            if (minorSetInMajor.isEmpty() == false) {
                minorList.add(minorSetInMajor.last());
            }
        }
        return minorList;
    }

}
