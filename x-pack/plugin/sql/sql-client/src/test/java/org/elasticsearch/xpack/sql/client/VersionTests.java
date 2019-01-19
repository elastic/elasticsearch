/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.client.Version;

public class VersionTests extends ESTestCase {
    public void test70Version() {
        byte[] ver = Version.from("7.0.0-alpha");
        assertEquals(7, ver[0]);
        assertEquals(0, ver[1]);
        assertEquals(0, ver[2]);
    }

    public void test712Version() {
        byte[] ver = Version.from("7.1.2");
        assertEquals(7, ver[0]);
        assertEquals(1, ver[1]);
        assertEquals(2, ver[2]);
    }

    public void testCurrent() {
        Version ver = Version.fromString(org.elasticsearch.Version.CURRENT.toString());
        assertEquals(org.elasticsearch.Version.CURRENT.major, ver.major);
        assertEquals(org.elasticsearch.Version.CURRENT.minor, ver.minor);
        assertEquals(org.elasticsearch.Version.CURRENT.revision, ver.revision);
    }

    public void testFromString() {
        Version ver = Version.fromString("1.2.3");
        assertEquals(1, ver.major);
        assertEquals(2, ver.minor);
        assertEquals(3, ver.revision);
        assertEquals("Unknown", ver.hash);
        assertEquals("1.2.3", ver.version);
    }

    public void testInvalidVersion() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> Version.from("7.1"));
        assertEquals("Invalid version 7.1", err.getMessage());
    }
}
