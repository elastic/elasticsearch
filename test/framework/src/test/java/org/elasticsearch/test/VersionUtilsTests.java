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
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests VersionUtils. Note: this test should remain unchanged across major versions
 * it uses the hardcoded versions on purpose.
 */
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

        // max or min can be an unreleased version
        Version unreleased = randomFrom(VersionUtils.allUnreleasedVersions());
        assertThat(VersionUtils.randomVersionBetween(random(), null, unreleased), lessThanOrEqualTo(unreleased));
        assertThat(VersionUtils.randomVersionBetween(random(), unreleased, null), greaterThanOrEqualTo(unreleased));
        assertEquals(unreleased, VersionUtils.randomVersionBetween(random(), unreleased, unreleased));
    }

    static class TestReleaseBranch {
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
        assertEquals(Arrays.asList(TestReleaseBranch.V_5_3_0, TestReleaseBranch.V_5_3_1, TestReleaseBranch.V_5_3_2,
                TestReleaseBranch.V_5_4_0), released);
        assertEquals(singletonList(TestReleaseBranch.V_5_4_1), unreleased);
    }

    static class TestStableBranch {
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
        assertEquals(Arrays.asList(TestStableBranch.V_5_3_0, TestStableBranch.V_5_3_1), released);
        assertEquals(Arrays.asList(TestStableBranch.V_5_3_2, TestStableBranch.V_5_4_0), unreleased);
    }

    static class TestStableBranchBehindStableBranch {
        public static final Version V_5_3_0 = Version.fromString("5.3.0");
        public static final Version V_5_3_1 = Version.fromString("5.3.1");
        public static final Version V_5_3_2 = Version.fromString("5.3.2");
        public static final Version V_5_4_0 = Version.fromString("5.4.0");
        public static final Version V_5_5_0 = Version.fromString("5.5.0");
        public static final Version CURRENT = V_5_5_0;
    }
    public void testResolveReleasedVersionsForStableBtranchBehindStableBranch() {
        Tuple<List<Version>, List<Version>> t = VersionUtils.resolveReleasedVersions(TestStableBranchBehindStableBranch.CURRENT,
                TestStableBranchBehindStableBranch.class);
        List<Version> released = t.v1();
        List<Version> unreleased = t.v2();
        assertEquals(Arrays.asList(TestStableBranchBehindStableBranch.V_5_3_0, TestStableBranchBehindStableBranch.V_5_3_1), released);
        assertEquals(Arrays.asList(TestStableBranchBehindStableBranch.V_5_3_2, TestStableBranchBehindStableBranch.V_5_4_0,
                TestStableBranchBehindStableBranch.V_5_5_0), unreleased);
    }

    static class TestUnstableBranch {
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
        assertEquals(Arrays.asList(TestUnstableBranch.V_5_3_0, TestUnstableBranch.V_5_3_1,
                TestUnstableBranch.V_6_0_0_alpha1, TestUnstableBranch.V_6_0_0_alpha2), released);
        assertEquals(Arrays.asList(TestUnstableBranch.V_5_3_2, TestUnstableBranch.V_5_4_0, TestUnstableBranch.V_6_0_0_beta1), unreleased);
    }

    /**
     * Tests that {@link Version#minimumCompatibilityVersion()} and {@link VersionUtils#allReleasedVersions()}
     * agree with the list of wire and index compatible versions we build in gradle.
     */
    public void testGradleVersionsMatchVersionUtils() {
        // First check the index compatible versions
        VersionsFromProperty indexCompatible = new VersionsFromProperty("tests.gradle_index_compat_versions");
        List<Version> released = VersionUtils.allReleasedVersions().stream()
                // Java lists some non-index compatible versions but gradle does not include them.
                .filter(v -> v.major == Version.CURRENT.major || v.major == Version.CURRENT.major - 1)
                /* Gradle will never include *released* alphas or betas because it will prefer
                 * the unreleased branch head. Gradle is willing to use branch heads that are
                 * beta or rc so that we have *something* to test against even though we
                 * do not offer backwards compatibility for alphas, betas, or rcs. */
                .filter(Version::isRelease)
                .collect(toList());

        List<String> releasedIndexCompatible = released.stream()
                .map(Object::toString)
                .collect(toList());
        assertEquals(releasedIndexCompatible, indexCompatible.released);

        List<String> unreleasedIndexCompatible = new ArrayList<>(VersionUtils.allUnreleasedVersions().stream()
                /* Gradle skips the current version because being backwards compatible
                 * with yourself is implied. Java lists the version because it is useful. */
                .filter(v -> v != Version.CURRENT)
                /* Note that gradle skips alphas because they don't have any backwards
                 * compatibility guarantees but keeps the last beta and rc in a branch
                 * on when there are only betas an RCs in that branch so that we have
                 * *something* to test that branch against. There is no need to recreate
                 * that logic here because allUnreleasedVersions already only contains
                 * the heads of branches so it should be good enough to just keep all
                 * the non-alphas.*/
                .filter(v -> false == v.isAlpha())
                .map(Object::toString)
                .collect(toCollection(LinkedHashSet::new)));
        assertEquals(unreleasedIndexCompatible, indexCompatible.unreleased);

        // Now the wire compatible versions
        VersionsFromProperty wireCompatible = new VersionsFromProperty("tests.gradle_wire_compat_versions");

        Version minimumCompatibleVersion = Version.CURRENT.minimumCompatibilityVersion();
        List<String> releasedWireCompatible = released.stream()
                .filter(v -> v.onOrAfter(minimumCompatibleVersion))
                .map(Object::toString)
                .collect(toList());
        assertEquals(releasedWireCompatible, wireCompatible.released);

        List<String> unreleasedWireCompatible = VersionUtils.allUnreleasedVersions().stream()
                /* Gradle skips the current version because being backwards compatible
                 * with yourself is implied. Java lists the version because it is useful. */
                .filter(v -> v != Version.CURRENT)
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
