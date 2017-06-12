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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static java.util.Collections.emptyList;
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
        Field[] fields = versionClass.getFields();
        List<Version> versions = new ArrayList<>(fields.length);
        for (final Field field : fields) {
            final int mod = field.getModifiers();
            if (false == Modifier.isStatic(mod) && Modifier.isFinal(mod) && Modifier.isPublic(mod)) {
                continue;
            }
            if (field.getType() != Version.class) {
                continue;
            }
            assert field.getName().matches("(V(_\\d+)+(_(alpha|beta|rc)\\d+)?|CURRENT)") : field.getName();
            if ("CURRENT".equals(field.getName())) {
                continue;
            }
            try {
                versions.add(((Version) field.get(null)));
            } catch (final IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        Collections.sort(versions);
        assert versions.get(versions.size() - 1).equals(current) : "The highest version must be the current one "
            + "but was [" + versions.get(versions.size() - 1) + "] and current was [" + current + "]";

        if (current.revision != 0) {
            /* If we are in a stable branch there should be no unreleased version constants
             * because we don't expect to release any new versions in older branches. If there
             * are extra constants then gradle will yell about it. */
            return new Tuple<>(unmodifiableList(versions), emptyList());
        }

        /* If we are on a patch release then we know that at least the version before the
         * current one is unreleased. If it is released then gradle would be complaining. */
        int unreleasedIndex = versions.size() - 2;
        while (true) {
            if (unreleasedIndex < 0) {
                throw new IllegalArgumentException("Couldn't find first non-alpha release");
            }
            /* Technically we don't support backwards compatiblity for alphas, betas,
             * and rcs. But the testing infrastructure requires that we act as though we
             * do. This is a difference between the gradle and Java logic but should be
             * fairly safe as it is errs on us being more compatible rather than less....
             * Anyway, the upshot is that we never declare alphas as unreleased, no
             * matter where they are in the list. */
            if (versions.get(unreleasedIndex).isRelease()) {
                break;
            }
            unreleasedIndex--;
        }

        Version unreleased = versions.remove(unreleasedIndex);
        if (unreleased.revision == 0) {
            /* If the last unreleased version is itself a patch release then gradle enforces
             * that there is yet another unreleased version before that. */
            unreleasedIndex--;
            Version earlierUnreleased = versions.remove(unreleasedIndex);
            return new Tuple<>(unmodifiableList(versions), unmodifiableList(Arrays.asList(earlierUnreleased, unreleased)));
        }
        return new Tuple<>(unmodifiableList(versions), singletonList(unreleased));
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
        int index = RELEASED_VERSIONS.indexOf(version);
        assert index > 0;
        return RELEASED_VERSIONS.get(index - 1);
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
        Version version = Version.CURRENT;
        do {
            version = getPreviousVersion(version);
            assert version.before(Version.CURRENT);
        } while (version.minor == Version.CURRENT.minor);
        return version;
    }

    /** Returns the oldest released {@link Version} */
    public static Version getFirstVersion() {
        return RELEASED_VERSIONS.get(0);
    }

    /** Returns a random {@link Version} from all available versions. */
    public static Version randomVersion(Random random) {
        return ALL_VERSIONS.get(random.nextInt(ALL_VERSIONS.size()));
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
}
