package org.elasticsearch.test;/*
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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionCollection;
import org.gradle.api.GradleException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class VersionCollectionTests extends RandomizedTest {

    private VersionCollection makeVersionCollection(String... constants) {
        List<String> sourceLines = new ArrayList<>(constants.length);
        for (final String constant : constants) {
            sourceLines.add(" public static final Version V_" + constant + " ");
        }
        return new VersionCollection(sourceLines);
    }

    private void expectThrowsGradleException(String expectedMessage, Runnable r) {
        try {
            r.run();
        } catch (GradleException e) {
            assertEquals("Exception message", expectedMessage, e.getMessage());
            return;
        }
        fail("Expected to throw [" + expectedMessage + "] but nothing was thrown.");
    }

    @Test
    public void testDetectsEmptyCollection() {
        expectThrowsGradleException("Unexpectedly found no version constants in Versions.java",
            () -> makeVersionCollection());
    }

    @Test
    public void testDetectsOutOfOrderVersions() {
        expectThrowsGradleException("Versions.java contains out of order version constants: 5.0.0 should come before 5.0.1",
            () -> makeVersionCollection("5_0_1", "5_0_0"));
    }

    @Test
    public void testDetectsOutOfOrderSuffixes() {
        expectThrowsGradleException("Versions.java contains out of order version constants: 5.0.0-alpha1 should come before 5.0.0-alpha2",
            () -> makeVersionCollection("5_0_0_alpha2", "5_0_0_alpha1"));
    }

    @Test
    public void testDetectsSuffixedVersionAfterNon() {
        expectThrowsGradleException("Versions.java contains out of order version constants: 5.0.0-alpha1 should come before 5.0.0",
            () -> makeVersionCollection("5_0_0", "5_0_0_alpha1"));
    }

    @Test
    public void testDropsObsoletedSuffixedVersions() {
        VersionCollection vc = makeVersionCollection("5_0_0_alpha1", "5_0_0_alpha2", "6_0_0_alpha1");
        assertArrayEquals(new Version[]{Version.fromString("5.0.0-alpha2-SNAPSHOT"), Version.fromString("6.0.0-alpha1-SNAPSHOT")},
            vc.getVersions().toArray());
        assertEquals(Version.fromString("6.0.0-alpha1-SNAPSHOT"), vc.getCurrentVersion());
    }

    @Test
    public void testSnapshotVersionsAreLabelled() {
        VersionCollection vc = makeVersionCollection("5_4_0", "5_4_1", "5_5_0", "5_5_1", "5_5_2", "5_6_0", "5_6_1", "5_6_2", "5_6_3",
            "6_0_0", "6_0_1", "6_0_2", "6_1_0", "6_2_0", "7_0_0_alpha1");
        assertArrayEquals(new Version[]{
            Version.fromString("5.4.0"), Version.fromString("5.4.1"), Version.fromString("5.5.0"), Version.fromString("5.5.1"),
            Version.fromString("5.5.2"), Version.fromString("5.6.0"), Version.fromString("5.6.1"), Version.fromString("5.6.2"),
            Version.fromString("5.6.3-SNAPSHOT"), Version.fromString("6.0.0"), Version.fromString("6.0.1"),
            Version.fromString("6.0.2-SNAPSHOT"), Version.fromString("6.1.0-SNAPSHOT"), Version.fromString("6.2.0-SNAPSHOT"),
            Version.fromString("7.0.0-alpha1-SNAPSHOT")}, vc.getVersions().toArray());
        assertEquals(Version.fromString("7.0.0-alpha1-SNAPSHOT"), vc.getCurrentVersion());
    }

    @Test
    public void testBWCSnapshotsOn6xBranchBeforeReleaseOf_6_1_0() {
        VersionCollection vc = makeVersionCollection("5_6_3", "6_0_0", "6_0_1", "6_0_2", "6_1_0", "6_2_0");
        assertEquals(Version.fromString("6.1.0-SNAPSHOT"), vc.getBWCSnapshotForCurrentMajor());
        assertEquals(Version.fromString("6.0.2-SNAPSHOT"), vc.getBWCSnapshotForPreviousMinorOfCurrentMajor());
        assertEquals(Version.fromString("5.6.3-SNAPSHOT"), vc.getBWCSnapshotForPreviousMajor());
        assertArrayEquals(new Version[]{
            Version.fromString("5.6.3-SNAPSHOT"), Version.fromString("6.1.0-SNAPSHOT")}, vc.getBasicIntegrationTestVersions().toArray());
    }

    @Test
    public void testBWCSnapshotsOn6xBranchAfterReleaseOf_6_1_0() {
        VersionCollection vc = makeVersionCollection("5_6_3", "6_0_0", "6_0_1", "6_0_2", "6_1_0", "6_1_1", "6_2_0");
        assertEquals(Version.fromString("6.1.1-SNAPSHOT"), vc.getBWCSnapshotForCurrentMajor());
        assertNull(vc.getBWCSnapshotForPreviousMinorOfCurrentMajor());
        assertEquals(Version.fromString("5.6.3-SNAPSHOT"), vc.getBWCSnapshotForPreviousMajor());
        assertArrayEquals(new Version[]{
            Version.fromString("5.6.3-SNAPSHOT"), Version.fromString("6.1.1-SNAPSHOT")}, vc.getBasicIntegrationTestVersions().toArray());
    }

    @Test
    public void testBWCSnapshotsOn7xBranchBeforeAnyReleases() {
        VersionCollection vc = makeVersionCollection("5_6_3", "6_0_0", "6_0_1", "6_0_2", "6_1_0", "6_1_1", "6_2_0", "7_0_0_alpha1");
        assertNull(vc.getBWCSnapshotForCurrentMajor());
        assertNull(vc.getBWCSnapshotForPreviousMinorOfCurrentMajor());
        assertEquals(Version.fromString("6.2.0-SNAPSHOT"), vc.getBWCSnapshotForPreviousMajor());
        assertArrayEquals(new Version[]{Version.fromString("6.2.0-SNAPSHOT")}, vc.getBasicIntegrationTestVersions().toArray());
    }

    @Test
    public void testCompatibleVersionsOn6xBranchBeforeReleaseOf_6_1_0() {
        VersionCollection vc = makeVersionCollection("5_5_1", "5_6_0", "5_6_2", "5_6_3", "6_0_0", "6_0_1", "6_0_2", "6_1_0", "6_2_0");
        assertArrayEquals(new Version[]{
                Version.fromString("5.5.1"), Version.fromString("5.6.0"), Version.fromString("5.6.2"),
                Version.fromString("5.6.3-SNAPSHOT"),
                Version.fromString("6.0.0"), Version.fromString("6.0.1"), Version.fromString("6.0.2-SNAPSHOT"),
                Version.fromString("6.1.0-SNAPSHOT")},
            vc.getVersionsIndexCompatibleWithCurrent().toArray());
        assertArrayEquals(new Version[]{Version.fromString("5.6.0"), Version.fromString("5.6.2"), Version.fromString("5.6.3-SNAPSHOT"),
            Version.fromString("6.0.0"), Version.fromString("6.0.1"), Version.fromString("6.0.2-SNAPSHOT"),
            Version.fromString("6.1.0-SNAPSHOT")}, vc.getVersionsWireCompatibleWithCurrent().toArray());
    }

    @Test
    public void testCompatibleVersionsOn6xBranchAfterReleaseOf_6_1_0() {
        VersionCollection vc = makeVersionCollection(
            "5_5_1", "5_6_0", "5_6_2", "5_6_3", "6_0_0", "6_0_1", "6_0_2", "6_1_0", "6_1_1", "6_2_0");
        assertArrayEquals(new Version[]{
                Version.fromString("5.5.1"), Version.fromString("5.6.0"), Version.fromString("5.6.2"),
                Version.fromString("5.6.3-SNAPSHOT"),
                Version.fromString("6.0.0"), Version.fromString("6.0.1"), Version.fromString("6.0.2-SNAPSHOT"),
                Version.fromString("6.1.0"), Version.fromString("6.1.1-SNAPSHOT")},
            vc.getVersionsIndexCompatibleWithCurrent().toArray());
        assertArrayEquals(new Version[]{Version.fromString("5.6.0"), Version.fromString("5.6.2"), Version.fromString("5.6.3-SNAPSHOT"),
            Version.fromString("6.0.0"), Version.fromString("6.0.1"), Version.fromString("6.0.2-SNAPSHOT"),
            Version.fromString("6.1.0"), Version.fromString("6.1.1-SNAPSHOT")}, vc.getVersionsWireCompatibleWithCurrent().toArray());
    }

    @Test
    public void testCompatibleVersionsOn7xBranchBeforeAnyReleases() {
        VersionCollection vc = makeVersionCollection("5_6_3", "6_0_0", "6_0_1", "6_0_2", "6_1_0", "6_1_1", "6_2_0", "7_0_0_alpha1");
        assertArrayEquals(new Version[]{
            Version.fromString("6.0.0"), Version.fromString("6.0.1"),
            Version.fromString("6.0.2-SNAPSHOT"), Version.fromString("6.1.0"), Version.fromString("6.1.1-SNAPSHOT"),
            Version.fromString("6.2.0-SNAPSHOT")}, vc.getVersionsIndexCompatibleWithCurrent().toArray());
        assertArrayEquals(new Version[]{Version.fromString("6.2.0-SNAPSHOT")}, vc.getVersionsWireCompatibleWithCurrent().toArray());
    }
}
