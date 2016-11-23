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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/** Utilities for selecting versions in tests */
public class VersionUtils {

    private static final List<Version> RELEASED_VERSIONS;
    private static final List<Version> UNRELEASED_VERSIONS;

    static {
        final Field[] declaredFields = Version.class.getFields();
        final Set<Integer> releasedIdsSet = new HashSet<>();
        final Set<Integer> unreleasedIdsSet = new HashSet<>();
        for (final Field field : declaredFields) {
            final int mod = field.getModifiers();
            if (Modifier.isStatic(mod) && Modifier.isFinal(mod) && Modifier.isPublic(mod)) {
                if (field.getType() == Version.class) {
                    final int id;
                    try {
                        id = ((Version) field.get(null)).id;
                    } catch (final IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                    assert field.getName().matches("(V(_\\d+)+(_(alpha|beta|rc)\\d+)?(_UNRELEASED)?|CURRENT)") : field.getName();
                    // note that below we remove CURRENT and add it to released; we do it this way because there are two constants that
                    // correspond to CURRENT, CURRENT itself and the actual version that CURRENT points to
                    if (field.getName().equals("CURRENT") || field.getName().endsWith("UNRELEASED")) {
                        unreleasedIdsSet.add(id);
                    } else {
                        releasedIdsSet.add(id);
                    }
                }
            }
        }

        // treat CURRENT as released for BWC testing
        unreleasedIdsSet.remove(Version.CURRENT.id);
        releasedIdsSet.add(Version.CURRENT.id);

        // unreleasedIdsSet and releasedIdsSet should be disjoint
        assert unreleasedIdsSet.stream().filter(releasedIdsSet::contains).collect(Collectors.toSet()).isEmpty();

        RELEASED_VERSIONS =
            Collections.unmodifiableList(releasedIdsSet.stream().sorted().map(Version::fromId).collect(Collectors.toList()));
        UNRELEASED_VERSIONS =
            Collections.unmodifiableList(unreleasedIdsSet.stream().sorted().map(Version::fromId).collect(Collectors.toList()));
    }

    /**
     * Returns an immutable, sorted list containing all released versions.
     *
     * @return all released versions
     */
    public static List<Version> allReleasedVersions() {
        return RELEASED_VERSIONS;
    }

    /**
     * Returns an immutable, sorted list containing all unreleased versions.
     *
     * @return all unreleased versions
     */
    public static List<Version> allUnreleasedVersions() {
        return UNRELEASED_VERSIONS;
    }

    public static Version getPreviousVersion(Version version) {
        int index = RELEASED_VERSIONS.indexOf(version);
        assert index > 0;
        return RELEASED_VERSIONS.get(index - 1);
    }

    /** Returns the {@link Version} before the {@link Version#CURRENT} */
    public static Version getPreviousVersion() {
        Version version = getPreviousVersion(Version.CURRENT);
        assert version.before(Version.CURRENT);
        return version;
    }

    /** Returns the oldest {@link Version} */
    public static Version getFirstVersion() {
        return RELEASED_VERSIONS.get(0);
    }

    /** Returns a random {@link Version} from all available versions. */
    public static Version randomVersion(Random random) {
        return RELEASED_VERSIONS.get(random.nextInt(RELEASED_VERSIONS.size()));
    }

    /** Returns a random {@link Version} between <code>minVersion</code> and <code>maxVersion</code> (inclusive). */
    public static Version randomVersionBetween(Random random, Version minVersion, Version maxVersion) {
        int minVersionIndex = 0;
        if (minVersion != null) {
            minVersionIndex = RELEASED_VERSIONS.indexOf(minVersion);
        }
        int maxVersionIndex = RELEASED_VERSIONS.size() - 1;
        if (maxVersion != null) {
            maxVersionIndex = RELEASED_VERSIONS.indexOf(maxVersion);
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
            return RELEASED_VERSIONS.get(minVersionIndex + random.nextInt(range));
        }
    }

    public static boolean isSnapshot(Version version) {
        if (Version.CURRENT.equals(version)) {
            return true;
        }
        return false;
    }
}
