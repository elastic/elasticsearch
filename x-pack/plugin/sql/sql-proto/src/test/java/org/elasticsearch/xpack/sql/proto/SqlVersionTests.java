/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto;

import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.Version.CURRENT;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.MAJOR_MULTIPLIER;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.MINOR_MULTIPLIER;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.REVISION_MULTIPLIER;
import static org.elasticsearch.xpack.sql.proto.SqlVersions.V_7_7_0;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.INTRODUCING_VERSIONING_INDEPENDENT_FEATURES;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.isClientCompatible;

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

    public void testVersionCompatibilityClientWithNoCompatibility() {
        SqlVersion server = SqlVersion.fromId(CURRENT.id);
        int major = randomIntBetween(1, 7);
        SqlVersion client = new SqlVersion(major, randomIntBetween(0, major == 7 ? 6 : 99), randomIntBetween(0, 99));
        assertFalse(isClientCompatible(server, client));
    }

    public void testVersionCompatibilityClientNewer() {
        SqlVersion server = randomReleasedVersion(false);
        SqlVersion client = new SqlVersion(server.major, server.minor, (byte) (server.revision + 1));
        assertFalse(isClientCompatible(server, client));
    }

    public void testVersionCompatibilityClientVersionIndependentFeatures() {
        SqlVersion server = new SqlVersion(
            randomIntBetween(INTRODUCING_VERSIONING_INDEPENDENT_FEATURES.major, 99),
            randomIntBetween(INTRODUCING_VERSIONING_INDEPENDENT_FEATURES.minor, 99),
            randomIntBetween(INTRODUCING_VERSIONING_INDEPENDENT_FEATURES.revision, 99)
        );
        SqlVersion client = new SqlVersion(
            randomIntBetween(INTRODUCING_VERSIONING_INDEPENDENT_FEATURES.major, 99),
            randomIntBetween(INTRODUCING_VERSIONING_INDEPENDENT_FEATURES.minor, 99),
            randomIntBetween(INTRODUCING_VERSIONING_INDEPENDENT_FEATURES.revision, 99)
        );
        assertTrue(server + " vs. " + client, isClientCompatible(server, client));
    }

    public void testVersionCompatibilityClientTooOld() {
        SqlVersion server = randomReleasedVersion(false);
        SqlVersion client = new SqlVersion(server.major - 2, randomIntBetween(0, 99), randomIntBetween(0, 99));
        assertFalse(isClientCompatible(server, client));
    }

    public void testVersionCompatibile() {
        SqlVersion client = new SqlVersion(
            randomIntBetween(V_7_7_0.major, 99 - 1),
            randomIntBetween(V_7_7_0.minor, 99),
            randomIntBetween(0, 99)
        );
        int serverMajor = client.major + (randomBoolean() ? 0 : 1);
        int serverMinor = randomIntBetween(client.major == serverMajor ? client.minor : 0, 99);
        int serverRevision = randomIntBetween(client.major == serverMajor && client.minor == serverMinor ? client.revision : 0, 99);
        SqlVersion server = new SqlVersion(serverMajor, serverMinor, serverRevision);
        assertTrue(isClientCompatible(server, client));
    }

    private static SqlVersion randomReleasedVersion(boolean includeVersioningIndependent) {
        var allVersions = SqlVersions.getAllVersions();
        return allVersions.get(randomIntBetween(0, allVersions.size() - 1 - (includeVersioningIndependent ? 0 : 1)));
    }
}
