/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.sql.proto.SqlVersion.MAJOR_MULTIPLIER;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.MINOR_MULTIPLIER;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.REVISION_MULTIPLIER;

public class SqlVersionTests extends ESTestCase {
    public void test123FromString() {
        SqlVersion ver = SqlVersion.fromString("1.2.3");
        assertEquals(1, ver.major);
        assertEquals(2, ver.minor);
        assertEquals(3, ver.revision);
        assertEquals(REVISION_MULTIPLIER - 1, ver.build);
        assertEquals(1 * MAJOR_MULTIPLIER + 2 * MINOR_MULTIPLIER + 3 * REVISION_MULTIPLIER + REVISION_MULTIPLIER - 1, ver.id);
        assertEquals("1.2.3", ver.version);
    }

    public void test123AlphaFromString() {
        SqlVersion ver = SqlVersion.fromString("1.2.3-Alpha");
        assertEquals(1, ver.major);
        assertEquals(2, ver.minor);
        assertEquals(3, ver.revision);
        assertEquals(REVISION_MULTIPLIER - 1, ver.build);
        assertEquals(1 * MAJOR_MULTIPLIER + 2 * MINOR_MULTIPLIER + 3 * REVISION_MULTIPLIER + REVISION_MULTIPLIER - 1, ver.id);
        assertEquals("1.2.3-Alpha", ver.version);
    }

    public void test123AlphaSnapshotFromString() {
        SqlVersion ver = SqlVersion.fromString("1.2.3-Alpha-SNAPSHOT");
        assertEquals(1, ver.major);
        assertEquals(2, ver.minor);
        assertEquals(3, ver.revision);
        assertEquals(REVISION_MULTIPLIER - 1, ver.build);
        assertEquals(1 * MAJOR_MULTIPLIER + 2 * MINOR_MULTIPLIER + 3 * REVISION_MULTIPLIER + REVISION_MULTIPLIER - 1, ver.id);
        assertEquals("1.2.3-Alpha-SNAPSHOT", ver.version);
    }

    public void testFromId() {
        SqlVersion ver = new SqlVersion((byte)randomIntBetween(0, 99), (byte)randomIntBetween(0, 99), (byte)randomIntBetween(0, 99));
        assertEquals(ver, SqlVersion.fromId(ver.id));
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
