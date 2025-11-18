/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test;

import org.elasticsearch.Version;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.Version.fromId;

/**
 * Tests VersionUtils. Note: this test should remain unchanged across major versions
 * it uses the hardcoded versions on purpose.
 */
public class VersionUtilsTests extends ESTestCase {

    public void testRandomVersionBetween() {
        // TODO: rework this test to use a dummy Version class so these don't need to change with each release
        // full range
        Version got = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), Version.CURRENT);
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(Version.CURRENT));
        got = VersionUtils.randomVersionBetween(random(), null, Version.CURRENT);
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(Version.CURRENT));
        got = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), null);
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(Version.CURRENT));

        // sub range
        got = VersionUtils.randomVersionBetween(random(), fromId(7000099), fromId(7010099));
        assertTrue(got.onOrAfter(fromId(7000099)));
        assertTrue(got.onOrBefore(fromId(7010099)));

        // unbounded lower
        got = VersionUtils.randomVersionBetween(random(), null, fromId(7000099));
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(fromId(7000099)));
        got = VersionUtils.randomVersionBetween(random(), null, VersionUtils.allVersions().getFirst());
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(VersionUtils.allVersions().getFirst()));

        // unbounded upper
        got = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), null);
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(Version.CURRENT));
        got = VersionUtils.randomVersionBetween(random(), VersionUtils.getPreviousVersion(), null);
        assertTrue(got.onOrAfter(VersionUtils.getPreviousVersion()));
        assertTrue(got.onOrBefore(Version.CURRENT));

        // range of one
        got = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), VersionUtils.getFirstVersion());
        assertEquals(got, VersionUtils.getFirstVersion());
        got = VersionUtils.randomVersionBetween(random(), Version.CURRENT, Version.CURRENT);
        assertEquals(got, Version.CURRENT);
        got = VersionUtils.randomVersionBetween(random(), fromId(7000099), fromId(7000099));
        assertEquals(got, fromId(7000099));

        // implicit range of one
        got = VersionUtils.randomVersionBetween(random(), null, VersionUtils.getFirstVersion());
        assertEquals(got, VersionUtils.getFirstVersion());
        got = VersionUtils.randomVersionBetween(random(), Version.CURRENT, null);
        assertEquals(got, Version.CURRENT);
    }

    /**
     * Tests that {@link Version#minimumCompatibilityVersion()} and {@link VersionUtils#allVersions()}
     * agree with the list of wire compatible versions we build in gradle.
     */
    public void testGradleVersionsMatchVersionUtils() {
        // First check the index compatible versions
        List<String> versions = VersionUtils.allVersions()
            .stream()
            /* Java lists all versions from the 5.x series onwards, but we only want to consider
             * ones that we're supposed to be compatible with. */
            .filter(v -> v.onOrAfter(Version.CURRENT.minimumCompatibilityVersion()))
            .map(Version::toString)
            .toList();
        List<String> gradleVersions = versionFromProperty("tests.gradle_wire_compat_versions");
        assertEquals(versions, gradleVersions);
    }

    private List<String> versionFromProperty(String property) {
        List<String> versions = new ArrayList<>();
        String versionsString = System.getProperty(property);
        assertNotNull("Couldn't find [" + property + "]. Gradle should set this before running the tests.", versionsString);
        logger.info("Looked up versions [{}={}]", property, versionsString);
        for (String version : versionsString.split(",")) {
            versions.add(version);
        }

        return versions;
    }
}
