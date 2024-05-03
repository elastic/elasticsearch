/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.index;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IndexVersionUtilsTests extends ESTestCase {
    /**
     * Tests that {@link IndexVersions#MINIMUM_COMPATIBLE} and {@link IndexVersionUtils#allReleasedVersions()}
     * agree with the list of index compatible versions we build in gradle.
     */
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/98054")
    public void testGradleVersionsMatchVersionUtils() {
        VersionsFromProperty indexCompatible = new VersionsFromProperty("tests.gradle_index_compat_versions");
        List<IndexVersion> released = IndexVersionUtils.allReleasedVersions()
            .stream()
            /* Java lists all versions from the 5.x series onwards, but we only want to consider
             * ones that we're supposed to be compatible with. */
            .filter(v -> v.onOrAfter(IndexVersions.MINIMUM_COMPATIBLE))
            .toList();

        List<String> releasedIndexCompatible = released.stream()
            .filter(v -> IndexVersion.current().equals(v) == false)
            .map(Object::toString)
            .toList();
        assertEquals(releasedIndexCompatible, indexCompatible.released);
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
