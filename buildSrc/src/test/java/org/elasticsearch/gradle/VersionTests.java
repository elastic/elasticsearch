package org.elasticsearch.gradle;

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

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class VersionTests extends GradleUnitTestCase {

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    public void testVersionParsing() {
        assertVersionEquals("7.0.1", 7, 0, 1, "", false);
        assertVersionEquals("7.0.1-alpha2", 7, 0, 1, "-alpha2", false);
        assertVersionEquals("5.1.2-rc3", 5, 1, 2, "-rc3", false);
        assertVersionEquals("6.1.2-SNAPSHOT", 6, 1, 2, "", true);
        assertVersionEquals("6.1.2-beta1-SNAPSHOT", 6, 1, 2, "-beta1", true);
    }

    public void testCompareWithStringVersions() {
        assertTrue("1.10.20 is not interpreted as before 2.0.0",
            Version.fromString("1.10.20").before("2.0.0")
        );
        assertTrue("7.0.0-alpha1 is not interpreted as before 7.0.0-alpha2",
            Version.fromString("7.0.0-alpha1").before("7.0.0-alpha2")
        );
        assertTrue("7.0.0-alpha1 should be equal to 7.0.0-alpha1",
            Version.fromString("7.0.0-alpha1").equals(Version.fromString("7.0.0-alpha1"))
        );
        assertTrue("7.0.0-SNAPSHOT should be equal to 7.0.0-SNAPSHOT",
            Version.fromString("7.0.0-SNAPSHOT").equals(Version.fromString("7.0.0-SNAPSHOT"))
        );
        assertEquals(Version.fromString("5.2.1-SNAPSHOT"), Version.fromString("5.2.1-SNAPSHOT"));
    }

    public void testCollections() {
        assertTrue(
            Arrays.asList(
                Version.fromString("5.2.0"), Version.fromString("5.2.1-SNAPSHOT"), Version.fromString("6.0.0"),
                Version.fromString("6.0.1"), Version.fromString("6.1.0")
            ).containsAll(Arrays.asList(
                Version.fromString("6.0.1"), Version.fromString("5.2.1-SNAPSHOT")
            ))
        );
        Set<Version> versions = new HashSet<>();
        versions.addAll(Arrays.asList(
            Version.fromString("5.2.0"), Version.fromString("5.2.1-SNAPSHOT"), Version.fromString("6.0.0"),
            Version.fromString("6.0.1"), Version.fromString("6.1.0")
        ));
        Set<Version> subset = new HashSet<>();
        subset.addAll(Arrays.asList(
            Version.fromString("6.0.1"), Version.fromString("5.2.1-SNAPSHOT")
        ));
        assertTrue(versions.containsAll(subset));
    }

    public void testToString() {
        assertEquals("7.0.1", new Version(7, 0, 1, null, false).toString());
    }

    public void testCompareVersions() {
        assertEquals(0, new Version(7, 0, 0, null, true).compareTo(
            new Version(7, 0, 0, null, true)
        ));
        assertEquals(0, new Version(7, 0, 0, null, true).compareTo(
            new Version(7, 0, 0, "", true)
        ));

        // snapshot is not taken into account TODO inconsistent with equals
        assertEquals(
            0,
            new Version(7, 0, 0, "", false).compareTo(
            new Version(7, 0, 0, null, true))
        );
        // without sufix is smaller than with TODO
        assertOrder(
            new Version(7, 0, 0, null, false),
            new Version(7, 0, 0, "-alpha1", false)
        );
        // numbered sufix
        assertOrder(
            new Version(7, 0, 0, "-alpha1", false),
            new Version(7, 0, 0, "-alpha2", false)
        );
        // ranked sufix
        assertOrder(
            new Version(7, 0, 0, "-alpha8", false),
            new Version(7, 0, 0, "-rc1", false)
        );
        // ranked sufix
        assertOrder(
            new Version(7, 0, 0, "-alpha8", false),
            new Version(7, 0, 0, "-beta1", false)
        );
        // ranked sufix
        assertOrder(
            new Version(7, 0, 0, "-beta8", false),
            new Version(7, 0, 0, "-rc1", false)
        );
        // major takes precedence
        assertOrder(
            new Version(6, 10, 10, "-alpha8", true),
            new Version(7, 0, 0, "-alpha2", false)
        );
        // then minor
        assertOrder(
            new Version(7, 0, 10, "-alpha8", true),
            new Version(7, 1, 0, "-alpha2", false)
        );
        // then revision
        assertOrder(
            new Version(7, 1, 0, "-alpha8", true),
            new Version(7, 1, 10, "-alpha2", false)
        );
    }

    public void testExceptionEmpty() {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Invalid version format");
        Version.fromString("");
    }

    public void testExceptionSyntax() {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Invalid version format");
        Version.fromString("foo.bar.baz");
    }

    public void testExceptionSuffixNumber() {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Invalid suffix");
        new Version(7, 1, 1, "-alpha", true);
    }

    public void testExceptionSuffix() {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Suffix must contain one of:");
        new Version(7, 1, 1, "foo1", true);
    }

    private void assertOrder(Version smaller, Version bigger) {
        assertEquals(smaller + " should be smaller than " + bigger, -1, smaller.compareTo(bigger));
    }

    private void assertVersionEquals(String stringVersion, int major, int minor, int revision, String sufix, boolean snapshot) {
        Version version = Version.fromString(stringVersion);
        assertEquals(major, version.getMajor());
        assertEquals(minor, version.getMinor());
        assertEquals(revision, version.getRevision());
        if (snapshot) {
            assertTrue("Expected version to be a snapshot but it was not", version.isSnapshot());
        } else {
            assertFalse("Expected version not to be a snapshot but it was", version.isSnapshot());
        }
        assertEquals(sufix, version.getSuffix());
    }

}
