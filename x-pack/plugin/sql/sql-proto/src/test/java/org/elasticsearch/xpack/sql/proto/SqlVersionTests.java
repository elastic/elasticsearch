/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.xpack.sql.proto.SqlVersion.MAJOR_MULTIPLIER;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.MINOR_MULTIPLIER;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.REVISION_MULTIPLIER;
import static org.hamcrest.Matchers.containsString;

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

    public void test123AlphaWithIdFromString() {
        String version = "1.2.3-Alpha[" + randomIntBetween(0, 100_000_000) + "]";
        SqlVersion ver = SqlVersion.fromString(version);
        assertEquals(1, ver.major);
        assertEquals(2, ver.minor);
        assertEquals(3, ver.revision);
        assertEquals(REVISION_MULTIPLIER - 1, ver.build);
        assertEquals(1 * MAJOR_MULTIPLIER + 2 * MINOR_MULTIPLIER + 3 * REVISION_MULTIPLIER + REVISION_MULTIPLIER - 1, ver.id);
        assertEquals(version, ver.version);
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
        SqlVersion ver = new SqlVersion((byte) randomIntBetween(0, 99), (byte) randomIntBetween(0, 99), (byte) randomIntBetween(0, 99));
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

    public void testFromTransportString() {
        assertEquals(SqlVersion.fromString("1.2.3"), SqlVersion.fromTransportString("1.2.3"));
        assertEquals(SqlVersion.fromString("4.5.6"), SqlVersion.fromTransportString("1.2.3-4.5.6"));
        assertEquals(SqlVersion.fromString("1.2.3"), SqlVersion.fromTransportString("1.2.3-[456]"));
    }

    public void testFromTransportStringFail() {
        for (String v : List.of("-", "1-1", "1.1.x-1", "1.1.1-2.2.2-3.3.3", "1.1.1-2.2.2-snapshot", "[1]-[1]")) {
            Throwable t = expectThrows(
                IllegalArgumentException.class,
                "failed to throw for: [" + v + "]",
                () -> SqlVersion.fromTransportString(v)
            );
            assertThat(t.getMessage(), containsString("Invalid version format [" + v + "]"));
        }
    }
}
