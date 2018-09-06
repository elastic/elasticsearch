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
        assertVersionEquals("7.0.1", 7, 0, 1, "");
    }

    public void testCompareWithStringVersions() {
        assertTrue("1.10.20 is not interpreted as before 2.0.0",
            Version.fromString("1.10.20").before("2.0.0")
        );
    }

    public void testCollections() {
        assertTrue(
            Arrays.asList(
                Version.fromString("6.0.0"),
                Version.fromString("6.0.1"),
                Version.fromString("6.1.0")
            ).containsAll(Arrays.asList(
                Version.fromString("6.0.1")
            ))
        );
        Set<Version> versions = new HashSet<>();
        versions.addAll(Arrays.asList(
            Version.fromString("5.2.0"), Version.fromString("6.0.0"),
            Version.fromString("6.0.1"), Version.fromString("6.1.0")
        ));
        Set<Version> subset = new HashSet<>();
        subset.addAll(Arrays.asList(
            Version.fromString("6.0.1"), Version.fromString("5.2.0")
        ));
        assertTrue(versions.containsAll(subset));
    }

    public void testToString() {
        assertEquals("7.0.1", new Version(7, 0, 1).toString());
    }

    public void testCompareVersions() {
        assertEquals(0, new Version(7, 0, 0).compareTo(
            new Version(7, 0, 0)
        ));

        assertOrder(
            new Version(7, 1, 0),
            new Version(7, 1, 1)
        );
        assertOrder(
            new Version(7, 1, 10),
            new Version(7, 2, 0)
        );
        assertOrder(
            new Version(7, 10, 10),
            new Version(8, 0, 0)
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


    private void assertOrder(Version smaller, Version bigger) {
        assertEquals(smaller + " should be smaller than " + bigger, -1, smaller.compareTo(bigger));
    }

    private void assertVersionEquals(String stringVersion, int major, int minor, int revision, String sufix) {
        Version version = Version.fromString(stringVersion);
        assertEquals(major, version.getMajor());
        assertEquals(minor, version.getMinor());
        assertEquals(revision, version.getRevision());
    }

}
