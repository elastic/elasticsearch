/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.util;

import org.elasticsearch.test.ESTestCase;

public class VersionTests extends ESTestCase {
    public void testVersionIsUnknownWithoutAJar() {
        // We aren't running in a jar so we have a bunch of "Unknown"
        assertEquals("Unknown", Version.versionNumber());
        assertEquals("Unknown", Version.versionHash());
        assertEquals(0, Version.versionMajor());
        assertEquals(0, Version.versionMinor());
    }

    public void test70Version() {
        int[] ver = Version.from("7.0.0-alpha");
        assertEquals(7, ver[0]);
        assertEquals(0, ver[1]);
        assertEquals(0, ver[2]);
    }

    public void test712Version() {
        int[] ver = Version.from("7.1.2");
        assertEquals(7, ver[0]);
        assertEquals(1, ver[1]);
        assertEquals(2, ver[2]);
    }

    public void testCurrent() {
        int[] ver = Version.from(org.elasticsearch.Version.CURRENT.toString());
        assertEquals(org.elasticsearch.Version.CURRENT.major, ver[0]);
        assertEquals(org.elasticsearch.Version.CURRENT.minor, ver[1]);
        assertEquals(org.elasticsearch.Version.CURRENT.revision, ver[2]);
    }

    public void testInvalidVersion() {
        Error err = expectThrows(Error.class, () -> Version.from("7.1"));
        assertEquals("Detected Elasticsearch SQL JDBC driver but found invalid version 7.1", err.getMessage());
    }
}
