/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import static org.elasticsearch.xpack.sql.action.SqlVersionId.LAST_SEM_VER_COMPATIBLE;
import static org.elasticsearch.xpack.sql.action.SqlVersionId.from;
import static org.hamcrest.Matchers.containsString;

public class SqlVersionIdTests extends ESTestCase {

    public void testToReleaseVersion() {
        for (var tv : TransportVersionUtils.allReleasedVersions()) {
            String releaseVersion = from(tv).toReleaseVersion();
            try {
                assertNotNull(SqlVersion.fromString(releaseVersion));
            } catch (Exception ignored) {
                assertNotNull(SqlVersion.fromTransportString(releaseVersion));
            }
        }
    }

    public void testCurrentRelease() {
        assertNotNull(SqlVersion.fromString(SqlVersionId.currentRelease()));
    }

    public void testIsNotClientCompatible() {
        for (var tv = TransportVersionUtils.getFirstVersion(); tv.before(TransportVersions.V_7_7_0); tv = TransportVersionUtils
            .getNextVersion(tv)) {
            assertFalse(SqlVersionId.isClientCompatible(from(tv)));
        }
    }

    public void testIsClientCompatible() {
        for (var tv = TransportVersions.V_7_7_0; tv.before(TransportVersion.current()); tv = TransportVersionUtils.getNextVersion(tv)) {
            assertTrue(SqlVersionId.isClientCompatible(from(tv)));
        }
        assertTrue(SqlVersionId.isClientCompatible(SqlVersionId.CURRENT));
    }

    public void testNoSqlVersion() {
        assertThrows(IllegalArgumentException.class, SqlVersionId.CURRENT::sqlVersion);
    }

    public void testFromSemVerString() {
        for (var tv : TransportVersionUtils.allReleasedVersions()) {
            String releaseVersion = tv.toReleaseVersion();
            try {
                SqlVersionId version = SqlVersionId.fromSemVerString(releaseVersion);
                assertNotNull(version);
                if (SqlVersionId.isSemVerCompatible(version)) {
                    assertEquals(releaseVersion, version.toReleaseVersion());
                }
            } catch (IllegalArgumentException exception) {
                assertThat(exception.getMessage(), containsString("Invalid version format [" + releaseVersion + "]"));
            }
        }
    }

    public void testIsSemVerCompatible() {
        TransportVersionUtils.allReleasedVersions()
            .stream()
            .map(SqlVersionId::from)
            .forEach(version -> assertEquals(version.onOrBefore(LAST_SEM_VER_COMPATIBLE), SqlVersionId.isSemVerCompatible(version)));
    }

    public void testFromTransportString() {
        for (var tv = TransportVersionUtils.getFirstVersion(); tv.before(TransportVersion.fromId(LAST_SEM_VER_COMPATIBLE.id())); tv =
            TransportVersionUtils.getNextVersion(tv)) {
            assertEquals(from(tv), SqlVersionId.fromSemVerString(tv.toReleaseVersion()));
        }

        assertNull(SqlVersionId.fromSemVerString(null));
        assertNull(SqlVersionId.fromSemVerString(""));
    }
}
