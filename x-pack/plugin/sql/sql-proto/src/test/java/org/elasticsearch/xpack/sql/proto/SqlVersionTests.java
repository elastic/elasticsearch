/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.test.ESTestCase;

public class SqlVersionTests extends ESTestCase {
    public void test123FromString() {
        SqlVersion ver = SqlVersion.fromString("1.2.3");
        assertEquals(1, ver.major);
        assertEquals(2, ver.minor);
        assertEquals(3, ver.revision);
        assertEquals(1 * SqlVersion.MAJOR_MULTIPLIER + 2 * SqlVersion.MINOR_MULTIPLIER + 3 * SqlVersion.REVISION_MULTIPLIER, ver.id);
        assertEquals("1.2.3", ver.version);
    }

    public void test123AlphaFromString() {
        SqlVersion ver = SqlVersion.fromString("1.2.3-Alpha");
        assertEquals(1, ver.major);
        assertEquals(2, ver.minor);
        assertEquals(3, ver.revision);
        assertEquals(1 * SqlVersion.MAJOR_MULTIPLIER + 2 * SqlVersion.MINOR_MULTIPLIER + 3 * SqlVersion.REVISION_MULTIPLIER, ver.id);
        assertEquals("1.2.3-Alpha", ver.version);
    }

    public void test123AlphaSnapshotFromString() {
        SqlVersion ver = SqlVersion.fromString("1.2.3-Alpha-SNAPSHOT");
        assertEquals(1, ver.major);
        assertEquals(2, ver.minor);
        assertEquals(3, ver.revision);
        assertEquals(1 * SqlVersion.MAJOR_MULTIPLIER + 2 * SqlVersion.MINOR_MULTIPLIER + 3 * SqlVersion.REVISION_MULTIPLIER, ver.id);
        assertEquals("1.2.3-Alpha-SNAPSHOT", ver.version);
    }

    public void testVersionsEqual() {
        SqlVersion ver1 = SqlVersion.fromString("1.2.3");
        SqlVersion ver2 = SqlVersion.fromString("1.2.3");
        assertEquals(ver1, ver2);
    }

    public void testVersionsAndStringEqual() {
        SqlVersion ver1 = SqlVersion.fromString("1.2.3");
        String ver2 = "1.2.3";
        assertEquals(ver1, ver2);
    }

    public void testVersionsAndStringNotEqual() {
        SqlVersion ver1 = SqlVersion.fromString("1.2.3");
        String ver2 = "1.2.4";
        assertNotEquals(ver1, ver2);
    }

    public void testVersionsAndInvalidStringNotEqual() {
        SqlVersion ver1 = SqlVersion.fromString("1.2.3");
        String ver2 = "invalid";
        assertNotEquals(ver1, ver2);
    }

}
