/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        // group versions into major version
        Map<Integer, List<Version>> majorVersions = Version.getDeclaredVersions(versionClass).stream()
            .collect(Collectors.groupingBy(v -> (int)v.major));
        // this breaks b/c 5.x is still in version list but master doesn't care about it!
        //assert majorVersions.size() == 2;
        // TODO: remove oldVersions, we should only ever have 2 majors in Version
        List<List<Version>> oldVersions = splitByMinor(majorVersions.getOrDefault((int)current.major - 2, Collections.emptyList()));
        List<List<Version>> previousMajor = splitByMinor(majorVersions.get((int)current.major - 1));
        List<List<Version>> currentMajor = splitByMinor(majorVersions.get((int)current.major));

        List<Version> unreleasedVersions = new ArrayList<>();
        final List<List<Version>> stableVersions;
        if (currentMajor.size() == 1) {
            // on master branch
            stableVersions = previousMajor;
            // remove current
            moveLastToUnreleased(currentMajor, unreleasedVersions);
        } else {
            // on a stable or release branch, ie N.x
            stableVersions = currentMajor;
            // remove the next maintenance bugfix
            moveLastToUnreleased(previousMajor, unreleasedVersions);
        }

        // remove next minor
        Version lastMinor = moveLastToUnreleased(stableVersions, unreleasedVersions);
        if (lastMinor.revision == 0) {
            if (stableVersions.get(stableVersions.size() - 1).size() == 1) {
                // a minor is being staged, which is also unreleased
                moveLastToUnreleased(stableVersions, unreleasedVersions);
            }
            // remove the next bugfix
            if (stableVersions.isEmpty() == false) {
                moveLastToUnreleased(stableVersions, unreleasedVersions);
            }
        }

        // If none of the previous major was released, then the last minor and bugfix of the old version was not released either.
        if (previousMajor.isEmpty()) {
            assert currentMajor.isEmpty() : currentMajor;
            // minor of the old version is being staged
            moveLastToUnreleased(oldVersions, unreleasedVersions);
            // bugix of the old version is also being staged
            moveLastToUnreleased(oldVersions, unreleasedVersions);
        }
        List<Version> releasedVersions = Stream.of(oldVersions, previousMajor, currentMajor)
            .flatMap(List::stream).flatMap(List::stream).collect(Collectors.toList());
        Collections.sort(unreleasedVersions); // we add unreleased out of order, so need to sort here
        return new Tuple<>(Collections.unmodifiableList(releasedVersions), Collections.unmodifiableList(unreleasedVersions));
    }

    // split the given versions into sub lists grouped by minor version
    private static List<List<Version>> splitByMinor(List<Version> versions) {
        Map<Integer, List<Version>> byMinor = versions.stream().collect(Collectors.groupingBy(v -> (int)v.minor));
        return byMinor.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(Collectors.toList());
    }

    // move the last version of the last minor in versions to the unreleased versions
    private static Version moveLastToUnreleased(List<List<Version>> versions, List<Version> unreleasedVersions) {
        List<Version> lastMinor = new ArrayList<>(versions.get(versions.size() - 1));
        Version lastVersion = lastMinor.remove(lastMinor.size() - 1);
        if (lastMinor.isEmpty()) {
            versions.remove(versions.size() - 1);
        } else {
            versions.set(versions.size() - 1, lastMinor);
        }
        unreleasedVersions.add(lastVersion);
        return lastVersion;
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
        ALL_VERSIONS = Collections.unmodifiableList(allVersions);
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

    /** returns the first future compatible version */
    public static Version compatibleFutureVersion(Version version) {
        final Optional<Version> opt = ALL_VERSIONS.stream().filter(version::before).filter(v -> v.isCompatible(version)).findAny();
        assert opt.isPresent() : "no future compatible version for " + version;
        return opt.get();
    }

    /** Returns the maximum {@link Version} that is compatible with the given version. */
    public static Version maxCompatibleVersion(Version version) {
        final List<Version> compatible = ALL_VERSIONS.stream().filter(version::isCompatible).filter(version::onOrBefore)
            .collect(Collectors.toList());
        assert compatible.size() > 0;
        return compatible.get(compatible.size() - 1);
    }

    /**
     * Returns a random version index compatible with the current version.
     */
    public static Version randomIndexCompatibleVersion(Random random) {
        return randomVersionBetween(random, Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT);
    }

    /**
     * Returns a random version index compatible with the given version, but not the given version.
     */
    public static Version randomPreviousCompatibleVersion(Random random, Version version) {
        // TODO: change this to minimumCompatibilityVersion(), but first need to remove released/unreleased
        // versions so getPreviousVerison returns the *actual* previous version. Otherwise eg 8.0.0 returns say 7.0.2 for previous,
        // but 7.2.0 for minimum compat
        return randomVersionBetween(random, version.minimumIndexCompatibilityVersion(), getPreviousVersion(version));
    }
}
