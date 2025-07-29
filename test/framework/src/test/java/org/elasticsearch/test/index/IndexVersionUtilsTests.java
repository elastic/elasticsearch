/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.index;

import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class IndexVersionUtilsTests extends ESTestCase {
    /**
     * Tests that {@link IndexVersions#MINIMUM_COMPATIBLE} and {@link IndexVersionUtils#allReleasedVersions()}
     * agree on the minimum version that should be tested.
     */
    public void testIndexCompatibleVersionMatches() {
        VersionsFromProperty indexCompatible = new VersionsFromProperty("tests.gradle_index_compat_versions");

        String minIndexVersion = IndexVersions.MINIMUM_COMPATIBLE.toReleaseVersion();
        String lowestCompatibleVersion = indexCompatible.released.get(0);

        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");

        if (arch.equals("aarch64") && osName.startsWith("Mac") && minIndexVersion.startsWith("7.0")) {
            // AArch64 is supported on Mac since 7.16.0
            assertThat(lowestCompatibleVersion, equalTo("7.16.0"));
        } else if (arch.equals("aarch64") && osName.startsWith("Linux") && minIndexVersion.startsWith("7.0")) {
            // AArch64 is supported on Linux since 7.12.0
            assertThat(lowestCompatibleVersion, equalTo("7.12.0"));
        } else {
            assertThat(lowestCompatibleVersion, equalTo(minIndexVersion));
        }
    }

    /**
     * Read a versions system property as set by gradle into a tuple of {@code (releasedVersion, unreleasedVersion)}.
     */
    private class VersionsFromProperty {
        private final List<String> released = new ArrayList<>();
        private final List<String> unreleased = new ArrayList<>();

        private VersionsFromProperty(String property) {
            Set<String> allUnreleased = new HashSet<>(Arrays.asList(System.getProperty("tests.gradle_unreleased_versions", "").split(",")));
            if (allUnreleased.isEmpty()) {
                fail("[tests.gradle_unreleased_versions] not set or empty. Gradle should set this before running.");
            }
            String versions = System.getProperty(property);
            assertNotNull("Couldn't find [" + property + "]. Gradle should set this before running the tests.", versions);
            logger.info("Looked up versions [{}={}]", property, versions);

            for (String version : versions.split(",")) {
                if (allUnreleased.contains(version)) {
                    unreleased.add(version);
                } else {
                    released.add(version);
                }
            }
        }
    }
}
