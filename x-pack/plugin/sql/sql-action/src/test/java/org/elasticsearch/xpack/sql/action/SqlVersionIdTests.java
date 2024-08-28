/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

public class SqlVersionIdTests extends ESTestCase {

    public void testToReleaseVersion() {
        for (var tv : TransportVersionUtils.allReleasedVersions()) {
            String releaseVersion = new SqlVersionId(tv).toReleaseVersion();
            try {
                assertNotNull(SqlVersion.fromString(releaseVersion));
            } catch (Exception __) {
                assertNotNull(SqlVersion.fromTransportString(releaseVersion));
            }
        }
    }

    public void testCurrentsNotEqual() {
        try {
            SqlVersion.fromString(Build.current().version());
        } catch (Exception e) {
            assumeNoException("Build current version is just a hash", e);
        }
        assertNotEquals(SqlVersionId.currentRelease(), SqlVersionId.CURRENT.toReleaseVersion());
    }

    public void testCurrentRelease() {
        assertNotNull(SqlVersion.fromString(SqlVersionId.currentRelease()));
    }

    public void testIsNotClientCompatible() {
        for (var tv = TransportVersionUtils.getFirstVersion(); tv.before(TransportVersions.V_7_7_0); tv = TransportVersionUtils
            .getNextVersion(tv)) {
            assertFalse(SqlVersionId.isClientCompatible(new SqlVersionId(tv)));
        }
    }

    public void testIsClientCompatible() {
        for (var tv = TransportVersions.V_7_7_0; tv.before(TransportVersion.current()); tv = TransportVersionUtils.getNextVersion(tv)) {
            assertTrue(SqlVersionId.isClientCompatible(new SqlVersionId(tv)));
        }
        assertTrue(SqlVersionId.isClientCompatible(SqlVersionId.CURRENT));
    }

    public void testNoSqlVersion() {
        assertThrows(UnsupportedOperationException.class, SqlVersionId.CURRENT::sqlVersion);
    }
}
