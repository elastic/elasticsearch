package org.elasticsearch.gradle;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

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
public class BwcVersionsTests extends GradleUnitTestCase {

    private static final Map<String, List<String>> sampleVersions = new HashMap<>();

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    static {
        // unreleased major and two unreleased minors ( minor in feature freeze )
        sampleVersions.put("8.0.0", asList(
            "7_0_0", "7_0_1", "7_1_0", "7_1_1", "7_2_0", "7_3_0", "8.0.0"
        ));
        sampleVersions.put("7.0.0-alpha1", asList(
            "6_0_0_alpha1", "6_0_0_alpha2", "6_0_0_beta1", "6_0_0_beta2", "6_0_0_rc1", "6_0_0_rc2",
            "6_0_0", "6_0_1", "6_1_0", "6_1_1", "6_1_2", "6_1_3", "6_1_4",
            "6_2_0", "6_2_1", "6_2_2", "6_2_3", "6_2_4",
            "6_3_0", "6_3_1", "6_3_2",
            "6_4_0", "6_4_1", "6_4_2",
            "6_5_0", "7_0_0_alpha1"
        ));
        sampleVersions.put("6.5.0", asList(
            "5_0_0_alpha1", "5_0_0_alpha2", "5_0_0_alpha3", "5_0_0_alpha4", "5_0_0_alpha5", "5_0_0_beta1", "5_0_0_rc1",
            "5_0_0", "5_0_1", "5_0_2", "5_1_1", "5_1_2", "5_2_0", "5_2_1", "5_2_2", "5_3_0", "5_3_1", "5_3_2", "5_3_3",
            "5_4_0", "5_4_1", "5_4_2", "5_4_3", "5_5_0", "5_5_1", "5_5_2", "5_5_3", "5_6_0", "5_6_1", "5_6_2", "5_6_3",
            "5_6_4", "5_6_5", "5_6_6", "5_6_7", "5_6_8", "5_6_9", "5_6_10", "5_6_11", "5_6_12", "5_6_13",
            "6_0_0_alpha1", "6_0_0_alpha2", "6_0_0_beta1", "6_0_0_beta2", "6_0_0_rc1", "6_0_0_rc2", "6_0_0", "6_0_1",
            "6_1_0", "6_1_1", "6_1_2", "6_1_3", "6_1_4", "6_2_0", "6_2_1", "6_2_2", "6_2_3", "6_2_4", "6_3_0", "6_3_1",
            "6_3_2", "6_4_0", "6_4_1", "6_4_2", "6_5_0"
        ));
        sampleVersions.put("6.6.0", asList(
            "5_0_0_alpha1", "5_0_0_alpha2", "5_0_0_alpha3", "5_0_0_alpha4", "5_0_0_alpha5", "5_0_0_beta1", "5_0_0_rc1",
            "5_0_0", "5_0_1", "5_0_2", "5_1_1", "5_1_2", "5_2_0", "5_2_1", "5_2_2", "5_3_0", "5_3_1", "5_3_2", "5_3_3",
            "5_4_0", "5_4_1", "5_4_2", "5_4_3", "5_5_0", "5_5_1", "5_5_2", "5_5_3", "5_6_0", "5_6_1", "5_6_2", "5_6_3",
            "5_6_4", "5_6_5", "5_6_6", "5_6_7", "5_6_8", "5_6_9", "5_6_10", "5_6_11", "5_6_12", "5_6_13",
            "6_0_0_alpha1", "6_0_0_alpha2", "6_0_0_beta1", "6_0_0_beta2", "6_0_0_rc1", "6_0_0_rc2", "6_0_0", "6_0_1",
            "6_1_0", "6_1_1", "6_1_2", "6_1_3", "6_1_4", "6_2_0", "6_2_1", "6_2_2", "6_2_3", "6_2_4", "6_3_0", "6_3_1",
            "6_3_2", "6_4_0", "6_4_1", "6_4_2", "6_5_0", "6_6_0"
        ));
        sampleVersions.put("6.4.2", asList(
            "5_0_0_alpha1", "5_0_0_alpha2", "5_0_0_alpha3", "5_0_0_alpha4", "5_0_0_alpha5", "5_0_0_beta1", "5_0_0_rc1",
            "5_0_0", "5_0_1", "5_0_2", "5_1_1", "5_1_2", "5_2_0", "5_2_1", "5_2_2", "5_3_0",
            "5_3_1", "5_3_2", "5_3_3", "5_4_0", "5_4_1", "5_4_2", "5_4_3", "5_5_0", "5_5_1", "5_5_2", "5_5_3",
            "5_6_0", "5_6_1", "5_6_2", "5_6_3", "5_6_4", "5_6_5", "5_6_6", "5_6_7", "5_6_8", "5_6_9", "5_6_10",
            "5_6_11", "5_6_12", "5_6_13",
            "6_0_0_alpha1", "6_0_0_alpha2", "6_0_0_beta1", "6_0_0_beta2", "6_0_0_rc1", "6_0_0_rc2",
            "6_0_0", "6_0_1", "6_1_0", "6_1_1", "6_1_2", "6_1_3", "6_1_4", "6_2_0", "6_2_1", "6_2_2", "6_2_3",
            "6_2_4", "6_3_0", "6_3_1", "6_3_2", "6_4_0", "6_4_1", "6_4_2"
        ));
        sampleVersions.put("7.1.0", asList(
            "7_1_0", "7_0_0", "6_7_0", "6_6_1", "6_6_0"
        ));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testExceptionOnEmpty() {
        new BwcVersions(asList("foo", "bar"), Version.fromString("7.0.0"));
    }

    @Test(expected = IllegalStateException.class)
    public void testExceptionOnNonCurrent() {
        new BwcVersions(singletonList(formatVersionToLine("6.5.0")), Version.fromString("7.0.0"));
    }

    @Test(expected = IllegalStateException.class)
    public void testExceptionOnTooManyMajors() {
        new BwcVersions(
            asList(
                formatVersionToLine("5.6.12"),
                formatVersionToLine("6.5.0"),
                formatVersionToLine("7.0.0")
            ),
            Version.fromString("6.5.0")
        );
    }

    public void testWireCompatible() {
        assertVersionsEquals(
            asList("6.5.0", "7.0.0"),
            getVersionCollection("7.0.0-alpha1").getWireCompatible()
        );
        assertVersionsEquals(
            asList(
                "5.6.0", "5.6.1", "5.6.2", "5.6.3", "5.6.4", "5.6.5", "5.6.6", "5.6.7", "5.6.8", "5.6.9", "5.6.10",
                "5.6.11", "5.6.12", "5.6.13",
                "6.0.0", "6.0.1", "6.1.0", "6.1.1", "6.1.2", "6.1.3", "6.1.4",
                "6.2.0", "6.2.1", "6.2.2", "6.2.3", "6.2.4",
                "6.3.0", "6.3.1", "6.3.2", "6.4.0", "6.4.1", "6.4.2", "6.5.0"
            ),
            getVersionCollection("6.5.0").getWireCompatible()
        );

        assertVersionsEquals(
            asList(
                "5.6.0", "5.6.1", "5.6.2", "5.6.3", "5.6.4", "5.6.5", "5.6.6", "5.6.7", "5.6.8", "5.6.9", "5.6.10",
                "5.6.11", "5.6.12", "5.6.13", "6.0.0", "6.0.1", "6.1.0", "6.1.1", "6.1.2", "6.1.3", "6.1.4",
                "6.2.0", "6.2.1", "6.2.2", "6.2.3", "6.2.4", "6.3.0", "6.3.1", "6.3.2", "6.4.0", "6.4.1", "6.4.2"
            ),
            getVersionCollection("6.4.2").getWireCompatible()
        );

        assertVersionsEquals(
            asList(
                "5.6.0", "5.6.1", "5.6.2", "5.6.3", "5.6.4", "5.6.5", "5.6.6", "5.6.7", "5.6.8", "5.6.9", "5.6.10",
                "5.6.11", "5.6.12", "5.6.13",
                "6.0.0", "6.0.1", "6.1.0", "6.1.1", "6.1.2", "6.1.3", "6.1.4",
                "6.2.0", "6.2.1", "6.2.2", "6.2.3", "6.2.4",
                "6.3.0", "6.3.1", "6.3.2", "6.4.0", "6.4.1", "6.4.2", "6.5.0", "6.6.0"
            ),
            getVersionCollection("6.6.0").getWireCompatible()
        );

        assertVersionsEquals(
            asList("7.3.0", "8.0.0"),
            getVersionCollection("8.0.0").getWireCompatible()
        );
        assertVersionsEquals(
            asList("6.7.0", "7.0.0", "7.1.0"),
            getVersionCollection("7.1.0").getWireCompatible()
        );

    }

    public void testWireCompatibleUnreleased() {
        assertVersionsEquals(
            asList("6.5.0", "7.0.0"),
            getVersionCollection("7.0.0-alpha1").getUnreleasedWireCompatible()
        );
        assertVersionsEquals(
            asList("5.6.13", "6.4.2", "6.5.0"),
            getVersionCollection("6.5.0").getUnreleasedWireCompatible()
        );

        assertVersionsEquals(
            asList("5.6.13", "6.4.2"),
            getVersionCollection("6.4.2").getUnreleasedWireCompatible()
        );

        assertVersionsEquals(
            asList("5.6.13", "6.4.2", "6.5.0", "6.6.0"),
            getVersionCollection("6.6.0").getUnreleasedWireCompatible()
        );

        assertVersionsEquals(
            asList("7.3.0", "8.0.0"),
            getVersionCollection("8.0.0").getUnreleasedWireCompatible()
        );
        assertVersionsEquals(
            asList("6.7.0", "7.0.0", "7.1.0"),
            getVersionCollection("7.1.0").getWireCompatible()
        );
    }

    public void testIndexCompatible() {
        assertVersionsEquals(
            asList(
                "6.0.0", "6.0.1", "6.1.0", "6.1.1", "6.1.2", "6.1.3", "6.1.4",
                "6.2.0", "6.2.1", "6.2.2", "6.2.3", "6.2.4", "6.3.0", "6.3.1",
                "6.3.2", "6.4.0", "6.4.1", "6.4.2", "6.5.0", "7.0.0"
            ),
            getVersionCollection("7.0.0-alpha1").getIndexCompatible()
        );

        assertVersionsEquals(
            asList(
                "5.0.0", "5.0.1", "5.0.2", "5.1.1", "5.1.2", "5.2.0", "5.2.1", "5.2.2", "5.3.0", "5.3.1", "5.3.2", "5.3.3",
                "5.4.0", "5.4.1", "5.4.2", "5.4.3", "5.5.0", "5.5.1", "5.5.2", "5.5.3", "5.6.0", "5.6.1", "5.6.2", "5.6.3",
                "5.6.4", "5.6.5", "5.6.6", "5.6.7", "5.6.8", "5.6.9", "5.6.10", "5.6.11", "5.6.12", "5.6.13",
                "6.0.0", "6.0.1", "6.1.0", "6.1.1", "6.1.2", "6.1.3", "6.1.4", "6.2.0", "6.2.1", "6.2.2", "6.2.3", "6.2.4",
                "6.3.0", "6.3.1", "6.3.2", "6.4.0", "6.4.1", "6.4.2", "6.5.0"
            ),
            getVersionCollection("6.5.0").getIndexCompatible()
        );

        assertVersionsEquals(
            asList(
                "5.0.0", "5.0.1", "5.0.2", "5.1.1", "5.1.2", "5.2.0", "5.2.1", "5.2.2", "5.3.0", "5.3.1", "5.3.2", "5.3.3",
                "5.4.0", "5.4.1", "5.4.2", "5.4.3", "5.5.0", "5.5.1", "5.5.2", "5.5.3", "5.6.0", "5.6.1", "5.6.2", "5.6.3",
                "5.6.4", "5.6.5", "5.6.6", "5.6.7", "5.6.8", "5.6.9", "5.6.10", "5.6.11", "5.6.12", "5.6.13",
                "6.0.0", "6.0.1", "6.1.0", "6.1.1", "6.1.2", "6.1.3", "6.1.4", "6.2.0", "6.2.1", "6.2.2", "6.2.3", "6.2.4",
                "6.3.0", "6.3.1", "6.3.2", "6.4.0", "6.4.1", "6.4.2"
            ),
            getVersionCollection("6.4.2").getIndexCompatible()
        );

        assertVersionsEquals(
            asList(
                "5.0.0", "5.0.1", "5.0.2", "5.1.1", "5.1.2", "5.2.0", "5.2.1", "5.2.2", "5.3.0", "5.3.1", "5.3.2", "5.3.3",
                "5.4.0", "5.4.1", "5.4.2", "5.4.3", "5.5.0", "5.5.1", "5.5.2", "5.5.3", "5.6.0", "5.6.1", "5.6.2", "5.6.3",
                "5.6.4", "5.6.5", "5.6.6", "5.6.7", "5.6.8", "5.6.9", "5.6.10", "5.6.11", "5.6.12", "5.6.13",
                "6.0.0", "6.0.1", "6.1.0", "6.1.1", "6.1.2", "6.1.3", "6.1.4", "6.2.0", "6.2.1", "6.2.2", "6.2.3", "6.2.4",
                "6.3.0", "6.3.1", "6.3.2", "6.4.0", "6.4.1", "6.4.2", "6.5.0", "6.6.0"
            ),
            getVersionCollection("6.6.0").getIndexCompatible()
        );

        assertVersionsEquals(
            asList("7.0.0", "7.0.1", "7.1.0", "7.1.1", "7.2.0", "7.3.0", "8.0.0"),
            getVersionCollection("8.0.0").getIndexCompatible()
        );
    }

    public void testIndexCompatibleUnreleased() {
        assertVersionsEquals(
            asList("6.4.2", "6.5.0", "7.0.0"),
            getVersionCollection("7.0.0-alpha1").getUnreleasedIndexCompatible()
        );

        assertVersionsEquals(
            asList("5.6.13", "6.4.2", "6.5.0"),
            getVersionCollection("6.5.0").getUnreleasedIndexCompatible()
        );

        assertVersionsEquals(
            asList("5.6.13", "6.4.2"),
            getVersionCollection("6.4.2").getUnreleasedIndexCompatible()
        );

        assertVersionsEquals(
            asList("5.6.13", "6.4.2", "6.5.0", "6.6.0"),
            getVersionCollection("6.6.0").getUnreleasedIndexCompatible()
        );

        assertVersionsEquals(
            asList("7.1.1", "7.2.0", "7.3.0", "8.0.0"),
            getVersionCollection("8.0.0").getUnreleasedIndexCompatible()
        );
    }

    public void testGetUnreleased() {
        assertVersionsEquals(
            asList("6.4.2", "6.5.0", "7.0.0-alpha1"),
            getVersionCollection("7.0.0-alpha1").getUnreleased()
        );
        assertVersionsEquals(
            asList("5.6.13", "6.4.2", "6.5.0"),
            getVersionCollection("6.5.0").getUnreleased()
        );
        assertVersionsEquals(
            asList("5.6.13", "6.4.2"),
            getVersionCollection("6.4.2").getUnreleased()
        );
        assertVersionsEquals(
            asList("5.6.13", "6.4.2", "6.5.0", "6.6.0"),
            getVersionCollection("6.6.0").getUnreleased()
        );
        assertVersionsEquals(
            asList("7.1.1", "7.2.0", "7.3.0", "8.0.0"),
            getVersionCollection("8.0.0").getUnreleased()
        );
    }

    public void testGetBranch() {
        assertUnreleasedBranchNames(
            asList("6.4", "6.x"),
            getVersionCollection("7.0.0-alpha1")
        );
        assertUnreleasedBranchNames(
            asList("5.6", "6.4"),
            getVersionCollection("6.5.0")
        );
        assertUnreleasedBranchNames(
            singletonList("5.6"),
            getVersionCollection("6.4.2")
        );
        assertUnreleasedBranchNames(
            asList("5.6", "6.4", "6.5"),
            getVersionCollection("6.6.0")
        );
        assertUnreleasedBranchNames(
            asList("7.1", "7.2", "7.x"),
            getVersionCollection("8.0.0")
        );
    }

    public void testGetGradleProjectPath() {
        assertUnreleasedGradleProjectPaths(
            asList(":distribution:bwc:bugfix", ":distribution:bwc:minor"),
            getVersionCollection("7.0.0-alpha1")
        );
        assertUnreleasedGradleProjectPaths(
            asList(":distribution:bwc:maintenance", ":distribution:bwc:bugfix"),
            getVersionCollection("6.5.0")
        );
        assertUnreleasedGradleProjectPaths(
            singletonList(":distribution:bwc:maintenance"),
            getVersionCollection("6.4.2")
        );
        assertUnreleasedGradleProjectPaths(
            asList(":distribution:bwc:maintenance", ":distribution:bwc:bugfix", ":distribution:bwc:minor"),
            getVersionCollection("6.6.0")
        );
        assertUnreleasedGradleProjectPaths(
            asList(":distribution:bwc:bugfix", ":distribution:bwc:staged", ":distribution:bwc:minor"),
            getVersionCollection("8.0.0")
        );
        assertUnreleasedGradleProjectPaths(
            asList(":distribution:bwc:maintenance", ":distribution:bwc:staged", ":distribution:bwc:minor"),
            getVersionCollection("7.1.0")
        );
    }

    public void testCompareToAuthoritative() {
        List<String> listOfVersions = asList("7.0.0", "7.0.1", "7.1.0", "7.1.1", "7.2.0", "7.3.0", "8.0.0");
        List<Version> authoritativeReleasedVersions = Stream.of("7.0.0", "7.0.1", "7.1.0")
                .map(Version::fromString)
                .collect(Collectors.toList());

        BwcVersions vc = new BwcVersions(
            listOfVersions.stream()
                .map(this::formatVersionToLine)
                .collect(Collectors.toList()),
            Version.fromString("8.0.0")
        );
        vc.compareToAuthoritative(authoritativeReleasedVersions);
    }

    public void testCompareToAuthoritativeUnreleasedActuallyReleased() {
        List<String> listOfVersions = asList("7.0.0", "7.0.1", "7.1.0", "7.1.1", "7.2.0", "7.3.0", "8.0.0");
        List<Version> authoritativeReleasedVersions = Stream.of("7.0.0", "7.0.1", "7.1.0", "7.1.1", "8.0.0")
                .map(Version::fromString)
                .collect(Collectors.toList());

        BwcVersions vc = new BwcVersions(
            listOfVersions.stream()
                .map(this::formatVersionToLine)
                .collect(Collectors.toList()),
            Version.fromString("8.0.0")
        );
        expectedEx.expect(IllegalStateException.class);
        expectedEx.expectMessage("but they are released");
        vc.compareToAuthoritative(authoritativeReleasedVersions);
    }

    public void testCompareToAuthoritativeNotReallyRelesed() {
        List<String> listOfVersions = asList("7.0.0", "7.0.1", "7.1.0", "7.1.1", "7.2.0", "7.3.0", "8.0.0");
        List<Version> authoritativeReleasedVersions = Stream.of("7.0.0", "7.0.1")
                .map(Version::fromString)
                .collect(Collectors.toList());
        BwcVersions vc = new BwcVersions(
            listOfVersions.stream()
                .map(this::formatVersionToLine)
                .collect(Collectors.toList()),
            Version.fromString("8.0.0")
        );
        expectedEx.expect(IllegalStateException.class);
        expectedEx.expectMessage("not really released");
        vc.compareToAuthoritative(authoritativeReleasedVersions);
    }

    private void assertUnreleasedGradleProjectPaths(List<String> expectedNAmes, BwcVersions bwcVersions) {
        List<String> actualNames = new ArrayList<>();
        bwcVersions.forPreviousUnreleased(unreleasedVersion ->
            actualNames.add(unreleasedVersion.gradleProjectPath)
        );
        assertEquals(expectedNAmes, actualNames);
    }

    private void assertUnreleasedBranchNames(List<String> expectedBranches, BwcVersions bwcVersions) {
        List<String> actualBranches = new ArrayList<>();
        bwcVersions.forPreviousUnreleased(unreleasedVersionInfo ->
            actualBranches.add(unreleasedVersionInfo.branch)
        );
        assertEquals(expectedBranches, actualBranches);
    }

    private String formatVersionToLine(final String version) {
        return " public static final Version V_" + version.replaceAll("\\.", "_") + " ";
    }

    private void assertVersionsEquals(List<String> expected, List<Version> actual) {
        assertEquals(
            expected.stream()
                .map(Version::fromString)
                .collect(Collectors.toList()),
            actual
        );
    }

    private BwcVersions getVersionCollection(String currentVersion) {
        return new BwcVersions(
            sampleVersions.get(currentVersion).stream()
                .map(this::formatVersionToLine)
                .collect(Collectors.toList()),
            Version.fromString(currentVersion)
        );
    }
}
