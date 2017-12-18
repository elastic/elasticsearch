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
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class VersionUtilsTests extends ESTestCase {

    public void testAllVersionsSorted() {
        List<Version> allVersions = VersionUtils.allReleasedVersions();
        for (int i = 0, j = 1; j < allVersions.size(); ++i, ++j) {
            assertTrue(allVersions.get(i).before(allVersions.get(j)));
        }
    }

    public void testRandomVersionBetween() {
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
        got = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0,
                Version.V_6_0_0_beta1);
        assertTrue(got.onOrAfter(Version.V_5_0_0));
        assertTrue(got.onOrBefore(Version.V_6_0_0_beta1));

        // unbounded lower
        got = VersionUtils.randomVersionBetween(random(), null, Version.V_6_0_0_beta1);
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(Version.V_6_0_0_beta1));
        got = VersionUtils.randomVersionBetween(random(), null, VersionUtils.allReleasedVersions().get(0));
        assertTrue(got.onOrAfter(VersionUtils.getFirstVersion()));
        assertTrue(got.onOrBefore(VersionUtils.allReleasedVersions().get(0)));

        // unbounded upper
        got = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, null);
        assertTrue(got.onOrAfter(Version.V_5_0_0));
        assertTrue(got.onOrBefore(Version.CURRENT));
        got = VersionUtils.randomVersionBetween(random(), VersionUtils.getPreviousVersion(), null);
        assertTrue(got.onOrAfter(VersionUtils.getPreviousVersion()));
        assertTrue(got.onOrBefore(Version.CURRENT));

        // range of one
        got = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(), VersionUtils.getFirstVersion());
        assertEquals(got, VersionUtils.getFirstVersion());
        got = VersionUtils.randomVersionBetween(random(), Version.CURRENT, Version.CURRENT);
        assertEquals(got, Version.CURRENT);
        got = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0_beta1,
                Version.V_6_0_0_beta1);
        assertEquals(got, Version.V_6_0_0_beta1);

        // implicit range of one
        got = VersionUtils.randomVersionBetween(random(), null, VersionUtils.getFirstVersion());
        assertEquals(got, VersionUtils.getFirstVersion());
        got = VersionUtils.randomVersionBetween(random(), Version.CURRENT, null);
        assertEquals(got, Version.CURRENT);

        if (Booleans.parseBoolean(System.getProperty("build.snapshot", "true"))) {
            // max or min can be an unreleased version
            final Version unreleased = randomFrom(VersionUtils.allUnreleasedVersions());
            assertThat(VersionUtils.randomVersionBetween(random(), null, unreleased), lessThanOrEqualTo(unreleased));
            assertThat(VersionUtils.randomVersionBetween(random(), unreleased, null), greaterThanOrEqualTo(unreleased));
            assertEquals(unreleased, VersionUtils.randomVersionBetween(random(), unreleased, unreleased));
        }
    }

    public static class TestReleaseBranch {
        public static final Version V_5_3_0 = Version.fromString("5.3.0");
        public static final Version V_5_3_1 = Version.fromString("5.3.1");
        public static final Version V_5_3_2 = Version.fromString("5.3.2");
        public static final Version V_5_4_0 = Version.fromString("5.4.0");
        public static final Version V_5_4_1 = Version.fromString("5.4.1");
        public static final Version CURRENT = V_5_4_1;
    }
    public void testResolveReleasedVersionsForReleaseBranch() {
        Tuple<List<Version>, List<Version>> t = VersionUtils.resolveReleasedVersions(TestReleaseBranch.CURRENT, TestReleaseBranch.class);
        List<Version> released = t.v1();
        List<Version> unreleased = t.v2();

        final List<Version> expectedReleased;
        final List<Version> expectedUnreleased;
        if (Booleans.parseBoolean(System.getProperty("build.snapshot", "true"))) {
            expectedReleased = Arrays.asList(
                    TestReleaseBranch.V_5_3_0,
                    TestReleaseBranch.V_5_3_1,
                    TestReleaseBranch.V_5_3_2,
                    TestReleaseBranch.V_5_4_0);
            expectedUnreleased = Collections.singletonList(TestReleaseBranch.V_5_4_1);
        } else {
            expectedReleased = Arrays.asList(
                    TestReleaseBranch.V_5_3_0,
                    TestReleaseBranch.V_5_3_1,
                    TestReleaseBranch.V_5_3_2,
                    TestReleaseBranch.V_5_4_0,
                    TestReleaseBranch.V_5_4_1);
            expectedUnreleased = Collections.emptyList();
        }

        assertThat(released, equalTo(expectedReleased));
        assertThat(unreleased, equalTo(expectedUnreleased));
    }

    public static class TestStableBranch {
        public static final Version V_5_3_0 = Version.fromString("5.3.0");
        public static final Version V_5_3_1 = Version.fromString("5.3.1");
        public static final Version V_5_3_2 = Version.fromString("5.3.2");
        public static final Version V_5_4_0 = Version.fromString("5.4.0");
        public static final Version CURRENT = V_5_4_0;
    }
    public void testResolveReleasedVersionsForUnreleasedStableBranch() {
        Tuple<List<Version>, List<Version>> t = VersionUtils.resolveReleasedVersions(TestStableBranch.CURRENT,
                TestStableBranch.class);
        List<Version> released = t.v1();
        List<Version> unreleased = t.v2();

        final List<Version> expectedReleased;
        final List<Version> expectedUnreleased;
        if (Booleans.parseBoolean(System.getProperty("build.snapshot", "true"))) {
            expectedReleased = Arrays.asList(TestStableBranch.V_5_3_0, TestStableBranch.V_5_3_1);
            expectedUnreleased = Arrays.asList(TestStableBranch.V_5_3_2, TestStableBranch.V_5_4_0);
        } else {
            expectedReleased =
                    Arrays.asList(TestStableBranch.V_5_3_0, TestStableBranch.V_5_3_1, TestStableBranch.V_5_3_2, TestStableBranch.V_5_4_0);
            expectedUnreleased = Collections.emptyList();
        }

        assertThat(released, equalTo(expectedReleased));
        assertThat(unreleased, equalTo(expectedUnreleased));
    }

    public static class TestStableBranchBehindStableBranch {
        public static final Version V_5_3_0 = Version.fromString("5.3.0");
        public static final Version V_5_3_1 = Version.fromString("5.3.1");
        public static final Version V_5_3_2 = Version.fromString("5.3.2");
        public static final Version V_5_4_0 = Version.fromString("5.4.0");
        public static final Version V_5_5_0 = Version.fromString("5.5.0");
        public static final Version CURRENT = V_5_5_0;
    }
    public void testResolveReleasedVersionsForStableBranchBehindStableBranch() {
        Tuple<List<Version>, List<Version>> t = VersionUtils.resolveReleasedVersions(TestStableBranchBehindStableBranch.CURRENT,
                TestStableBranchBehindStableBranch.class);
        List<Version> released = t.v1();
        List<Version> unreleased = t.v2();

        final List<Version> expectedReleased;
        final List<Version> expectedUnreleased;
        if (Booleans.parseBoolean(System.getProperty("build.snapshot", "true"))) {
            expectedReleased = Arrays.asList(TestStableBranchBehindStableBranch.V_5_3_0, TestStableBranchBehindStableBranch.V_5_3_1);
            expectedUnreleased = Arrays.asList(
                    TestStableBranchBehindStableBranch.V_5_3_2,
                    TestStableBranchBehindStableBranch.V_5_4_0,
                    TestStableBranchBehindStableBranch.V_5_5_0);
        } else {
            expectedReleased = Arrays.asList(
                    TestStableBranchBehindStableBranch.V_5_3_0,
                    TestStableBranchBehindStableBranch.V_5_3_1,
                    TestStableBranchBehindStableBranch.V_5_3_2,
                    TestStableBranchBehindStableBranch.V_5_4_0,
                    TestStableBranchBehindStableBranch.V_5_5_0);
            expectedUnreleased = Collections.emptyList();
        }
        assertThat(released, equalTo(expectedReleased));
        assertThat(unreleased, equalTo(expectedUnreleased));
    }

    public static class TestUnstableBranch {
        public static final Version V_5_3_0 = Version.fromString("5.3.0");
        public static final Version V_5_3_1 = Version.fromString("5.3.1");
        public static final Version V_5_3_2 = Version.fromString("5.3.2");
        public static final Version V_5_4_0 = Version.fromString("5.4.0");
        public static final Version V_6_0_0_alpha1 = Version.fromString("6.0.0-alpha1");
        public static final Version V_6_0_0_alpha2 = Version.fromString("6.0.0-alpha2");
        public static final Version V_6_0_0_beta1 = Version.fromString("6.0.0-beta1");
        public static final Version CURRENT = V_6_0_0_beta1;
    }

    public void testResolveReleasedVersionsForUnstableBranch() {
        Tuple<List<Version>, List<Version>> t = VersionUtils.resolveReleasedVersions(TestUnstableBranch.CURRENT,
                TestUnstableBranch.class);
        List<Version> released = t.v1();
        List<Version> unreleased = t.v2();

        final List<Version> expectedReleased;
        final List<Version> expectedUnreleased;
        if (Booleans.parseBoolean(System.getProperty("build.snapshot", "true"))) {
            expectedReleased = Arrays.asList(
                    TestUnstableBranch.V_5_3_0,
                    TestUnstableBranch.V_5_3_1,
                    TestUnstableBranch.V_6_0_0_alpha1,
                    TestUnstableBranch.V_6_0_0_alpha2);
            expectedUnreleased = Arrays.asList(TestUnstableBranch.V_5_3_2, TestUnstableBranch.V_5_4_0, TestUnstableBranch.V_6_0_0_beta1);
        } else {
            expectedReleased = Arrays.asList(
                    TestUnstableBranch.V_5_3_0,
                    TestUnstableBranch.V_5_3_1,
                    TestUnstableBranch.V_5_3_2,
                    TestUnstableBranch.V_5_4_0,
                    TestUnstableBranch.V_6_0_0_alpha1,
                    TestUnstableBranch.V_6_0_0_alpha2,
                    TestUnstableBranch.V_6_0_0_beta1);
            expectedUnreleased = Collections.emptyList();
        }
        assertThat(released, equalTo(expectedReleased));
        assertThat(unreleased, equalTo(expectedUnreleased));
    }

    public static class TestNewMajorRelease {
        public static final Version V_5_6_0 = Version.fromString("5.6.0");
        public static final Version V_5_6_1 = Version.fromString("5.6.1");
        public static final Version V_5_6_2 = Version.fromString("5.6.2");
        public static final Version V_6_0_0_alpha1 = Version.fromString("6.0.0-alpha1");
        public static final Version V_6_0_0_alpha2 = Version.fromString("6.0.0-alpha2");
        public static final Version V_6_0_0_beta1 = Version.fromString("6.0.0-beta1");
        public static final Version V_6_0_0_beta2 = Version.fromString("6.0.0-beta2");
        public static final Version V_6_0_0 = Version.fromString("6.0.0");
        public static final Version V_6_0_1 = Version.fromString("6.0.1");
        public static final Version CURRENT = V_6_0_1;
    }

    public void testResolveReleasedVersionsAtNewMajorRelease() {
        Tuple<List<Version>, List<Version>> t = VersionUtils.resolveReleasedVersions(TestNewMajorRelease.CURRENT,
            TestNewMajorRelease.class);
        List<Version> released = t.v1();
        List<Version> unreleased = t.v2();

        final List<Version> expectedReleased;
        final List<Version> expectedUnreleased;
        if (Booleans.parseBoolean(System.getProperty("build.snapshot", "true"))) {
            expectedReleased = Arrays.asList(
                    TestNewMajorRelease.V_5_6_0,
                    TestNewMajorRelease.V_5_6_1,
                    TestNewMajorRelease.V_6_0_0_alpha1,
                    TestNewMajorRelease.V_6_0_0_alpha2,
                    TestNewMajorRelease.V_6_0_0_beta1,
                    TestNewMajorRelease.V_6_0_0_beta2,
                    TestNewMajorRelease.V_6_0_0);
            expectedUnreleased = Arrays.asList(TestNewMajorRelease.V_5_6_2, TestNewMajorRelease.V_6_0_1);
        } else {
            expectedReleased = Arrays.asList(
                    TestNewMajorRelease.V_5_6_0,
                    TestNewMajorRelease.V_5_6_1,
                    TestNewMajorRelease.V_5_6_2,
                    TestNewMajorRelease.V_6_0_0_alpha1,
                    TestNewMajorRelease.V_6_0_0_alpha2,
                    TestNewMajorRelease.V_6_0_0_beta1,
                    TestNewMajorRelease.V_6_0_0_beta2,
                    TestNewMajorRelease.V_6_0_0,
                    TestNewMajorRelease.V_6_0_1);
            expectedUnreleased = Collections.emptyList();
        }

        assertThat(released, equalTo(expectedReleased));
        assertThat(unreleased, equalTo(expectedUnreleased));
    }

    public static class TestVersionBumpIn6x {
        public static final Version V_5_6_0 = Version.fromString("5.6.0");
        public static final Version V_5_6_1 = Version.fromString("5.6.1");
        public static final Version V_5_6_2 = Version.fromString("5.6.2");
        public static final Version V_6_0_0_alpha1 = Version.fromString("6.0.0-alpha1");
        public static final Version V_6_0_0_alpha2 = Version.fromString("6.0.0-alpha2");
        public static final Version V_6_0_0_beta1 = Version.fromString("6.0.0-beta1");
        public static final Version V_6_0_0_beta2 = Version.fromString("6.0.0-beta2");
        public static final Version V_6_0_0 = Version.fromString("6.0.0");
        public static final Version V_6_0_1 = Version.fromString("6.0.1");
        public static final Version V_6_1_0 = Version.fromString("6.1.0");
        public static final Version CURRENT = V_6_1_0;
    }

    public void testResolveReleasedVersionsAtVersionBumpIn6x() {
        Tuple<List<Version>, List<Version>> t = VersionUtils.resolveReleasedVersions(TestVersionBumpIn6x.CURRENT,
            TestVersionBumpIn6x.class);
        List<Version> released = t.v1();
        List<Version> unreleased = t.v2();

        final List<Version> expectedReleased;
        final List<Version> expectedUnreleased;

        if (Booleans.parseBoolean(System.getProperty("build.snapshot", "true"))) {
            expectedReleased = Arrays.asList(
                    TestVersionBumpIn6x.V_5_6_0,
                    TestVersionBumpIn6x.V_5_6_1,
                    TestVersionBumpIn6x.V_6_0_0_alpha1,
                    TestVersionBumpIn6x.V_6_0_0_alpha2,
                    TestVersionBumpIn6x.V_6_0_0_beta1,
                    TestVersionBumpIn6x.V_6_0_0_beta2,
                    TestVersionBumpIn6x.V_6_0_0);
            expectedUnreleased = Arrays.asList(TestVersionBumpIn6x.V_5_6_2, TestVersionBumpIn6x.V_6_0_1, TestVersionBumpIn6x.V_6_1_0);
        } else {
            expectedReleased = Arrays.asList(
                    TestVersionBumpIn6x.V_5_6_0,
                    TestVersionBumpIn6x.V_5_6_1,
                    TestVersionBumpIn6x.V_5_6_2,
                    TestVersionBumpIn6x.V_6_0_0_alpha1,
                    TestVersionBumpIn6x.V_6_0_0_alpha2,
                    TestVersionBumpIn6x.V_6_0_0_beta1,
                    TestVersionBumpIn6x.V_6_0_0_beta2,
                    TestVersionBumpIn6x.V_6_0_0,
                    TestVersionBumpIn6x.V_6_0_1,
                    TestVersionBumpIn6x.V_6_1_0);
            expectedUnreleased = Collections.emptyList();
        }

        assertThat(released, equalTo(expectedReleased));
        assertThat(unreleased, equalTo(expectedUnreleased));
    }

    public static class TestNewMinorBranchIn6x {
        public static final Version V_5_6_0 = Version.fromString("5.6.0");
        public static final Version V_5_6_1 = Version.fromString("5.6.1");
        public static final Version V_5_6_2 = Version.fromString("5.6.2");
        public static final Version V_6_0_0_alpha1 = Version.fromString("6.0.0-alpha1");
        public static final Version V_6_0_0_alpha2 = Version.fromString("6.0.0-alpha2");
        public static final Version V_6_0_0_beta1 = Version.fromString("6.0.0-beta1");
        public static final Version V_6_0_0_beta2 = Version.fromString("6.0.0-beta2");
        public static final Version V_6_0_0 = Version.fromString("6.0.0");
        public static final Version V_6_0_1 = Version.fromString("6.0.1");
        public static final Version V_6_1_0 = Version.fromString("6.1.0");
        public static final Version V_6_1_1 = Version.fromString("6.1.1");
        public static final Version V_6_1_2 = Version.fromString("6.1.2");
        public static final Version V_6_2_0 = Version.fromString("6.2.0");
        public static final Version CURRENT = V_6_2_0;
    }

    public void testResolveReleasedVersionsAtNewMinorBranchIn6x() {
        Tuple<List<Version>, List<Version>> t = VersionUtils.resolveReleasedVersions(TestNewMinorBranchIn6x.CURRENT,
            TestNewMinorBranchIn6x.class);
        List<Version> released = t.v1();
        List<Version> unreleased = t.v2();

        final List<Version> expectedReleased;
        final List<Version> expectedUnreleased;
        if (Booleans.parseBoolean(System.getProperty("build.snapshot", "true"))) {
            expectedReleased = Arrays.asList(
                    TestNewMinorBranchIn6x.V_5_6_0,
                    TestNewMinorBranchIn6x.V_5_6_1,
                    TestNewMinorBranchIn6x.V_6_0_0_alpha1,
                    TestNewMinorBranchIn6x.V_6_0_0_alpha2,
                    TestNewMinorBranchIn6x.V_6_0_0_beta1,
                    TestNewMinorBranchIn6x.V_6_0_0_beta2,
                    TestNewMinorBranchIn6x.V_6_0_0,
                    TestNewMinorBranchIn6x.V_6_1_0,
                    TestNewMinorBranchIn6x.V_6_1_1);
            expectedUnreleased = Arrays.asList(
                    TestNewMinorBranchIn6x.V_5_6_2,
                    TestNewMinorBranchIn6x.V_6_0_1,
                    TestNewMinorBranchIn6x.V_6_1_2,
                    TestNewMinorBranchIn6x.V_6_2_0);
        } else {
            expectedReleased = Arrays.asList(
                    TestNewMinorBranchIn6x.V_5_6_0,
                    TestNewMinorBranchIn6x.V_5_6_1,
                    TestNewMinorBranchIn6x.V_5_6_2,
                    TestNewMinorBranchIn6x.V_6_0_0_alpha1,
                    TestNewMinorBranchIn6x.V_6_0_0_alpha2,
                    TestNewMinorBranchIn6x.V_6_0_0_beta1,
                    TestNewMinorBranchIn6x.V_6_0_0_beta2,
                    TestNewMinorBranchIn6x.V_6_0_0,
                    TestNewMinorBranchIn6x.V_6_0_1,
                    TestNewMinorBranchIn6x.V_6_1_0,
                    TestNewMinorBranchIn6x.V_6_1_1,
                    TestNewMinorBranchIn6x.V_6_1_2,
                    TestNewMinorBranchIn6x.V_6_2_0);
            expectedUnreleased = Collections.emptyList();
        }

        assertThat(released, equalTo(expectedReleased));
        assertThat(unreleased, equalTo(expectedUnreleased));
    }

    /**
     * Tests that {@link Version#minimumCompatibilityVersion()} and {@link VersionUtils#allReleasedVersions()}
     * agree with the list of wire and index compatible versions we build in gradle.
     */
    public void testGradleVersionsMatchVersionUtils() {
        // First check the index compatible versions
        VersionsFromProperty indexCompatible = new VersionsFromProperty("tests.gradle_index_compat_versions");

        List<Version> released = VersionUtils.allReleasedVersions().stream()
                /* We skip alphas, betas, and the like in gradle because they don't have
                 * backwards compatibility guarantees even though they are technically
                 * released. */
                .filter(Version::isRelease)
                .collect(toList());
        List<String> releasedIndexCompatible = released.stream()
                .filter(v -> !Version.CURRENT.equals(v))
                .map(Object::toString)
                .collect(toList());
        assertEquals(releasedIndexCompatible, indexCompatible.released);

        List<String> unreleasedIndexCompatible = VersionUtils.allUnreleasedVersions().stream()
                /* Gradle skips the current version because being backwards compatible
                 * with yourself is implied. Java lists the version because it is useful. */
                .filter(v -> !Version.CURRENT.equals(v))
                .map(Object::toString)
                .collect(toList());
        assertEquals(unreleasedIndexCompatible, indexCompatible.unreleased);

        // Now the wire compatible versions
        VersionsFromProperty wireCompatible = new VersionsFromProperty("tests.gradle_wire_compat_versions");
        Version minimumCompatibleVersion = Version.CURRENT.minimumCompatibilityVersion();
        List<String> releasedWireCompatible = released.stream()
                .filter(v -> !Version.CURRENT.equals(v))
                .filter(v -> v.onOrAfter(minimumCompatibleVersion))
                .map(Object::toString)
                .collect(toList());
        assertEquals(releasedWireCompatible, wireCompatible.released);

        List<String> unreleasedWireCompatible = VersionUtils.allUnreleasedVersions().stream()
                /* Gradle skips the current version because being backwards compatible
                 * with yourself is implied. Java lists the version because it is useful. */
                .filter(v -> !Version.CURRENT.equals(v))
                .filter(v -> v.onOrAfter(minimumCompatibleVersion))
                .map(Object::toString)
                .collect(toList());
        assertEquals(unreleasedWireCompatible, wireCompatible.unreleased);
    }

    /**
     * Read a versions system property as set by gradle into a tuple of {@code (releasedVersion, unreleasedVersion)}.
     */
    private class VersionsFromProperty {
        private final List<String> released = new ArrayList<>();
        private final List<String> unreleased = new ArrayList<>();

        private VersionsFromProperty(String property) {
            String versions = System.getProperty(property);
            assertNotNull("Couldn't find [" + property + "]. Gradle should set these before running the tests.", versions);
            logger.info("Looked up versions [{}={}]", property, versions);

            for (String version : versions.split(",")) {
                if (version.endsWith("-SNAPSHOT")) {
                    unreleased.add(version.replace("-SNAPSHOT", ""));
                } else {
                    released.add(version);
                }
            }
        }
    }
}
