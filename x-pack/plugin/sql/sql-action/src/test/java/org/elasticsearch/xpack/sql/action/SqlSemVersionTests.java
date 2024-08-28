/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class SqlSemVersionTests extends ESTestCase {

    public void testToReleaseVersion() {
        for (var tv : TransportVersionUtils.allReleasedVersions()) {
            String releaseVersion = tv.toReleaseVersion();
            try {
                SqlSemVersion semVersion = SqlSemVersion.fromString(releaseVersion);
                assertNotNull(SqlSemVersion.fromString(releaseVersion));
                assertEquals(releaseVersion, semVersion.toReleaseVersion());
            } catch (Exception exception) {
                assertThat(exception.getMessage(), containsString("Invalid version format [" + releaseVersion + "]"));
            }
        }
    }

    public void testFromTransportString() {
        assertEquals(
            SqlSemVersion.fromString("7.0.0").sqlVersion(),
            SqlVersion.fromTransportString(TransportVersions.V_7_0_0.toReleaseVersion())
        );
        assertEquals(
            SqlSemVersion.fromString("8.9.2").sqlVersion(),
            SqlVersion.fromTransportString(TransportVersions.V_8_9_X.toReleaseVersion())
        );
        assertEquals(
            SqlSemVersion.fromString("8.12.0").sqlVersion(),
            SqlVersion.fromTransportString(TransportVersions.V_8_12_0.toReleaseVersion())
        );

        assertEquals(
            SqlSemVersion.fromString("8.15.0").sqlVersion(),
            SqlVersion.fromTransportString(TransportVersions.WATERMARK_THRESHOLDS_STATS.toReleaseVersion())
        );
        assertEquals(
            SqlSemVersion.fromString("8.15.0").sqlVersion(),
            SqlVersion.fromTransportString(TransportVersions.FIX_VECTOR_SIMILARITY_INNER_HITS_BACKPORT_8_15.toReleaseVersion())
        );
        assertEquals(
            SqlSemVersion.fromString("8.15.1-snapshot[8725000]").sqlVersion(),
            SqlVersion.fromTransportString(TransportVersions.ESQL_PROFILE_SLEEPS.toReleaseVersion())
        );

        assertNull(SqlSemVersion.fromString(null));
        assertNull(SqlSemVersion.fromString(""));
    }

    public void testFromTransportStringFail() {
        for (String v : List.of("-", "1-1", "1.1.x-1", "1.1.1-2.2.2-3.3.3", "1.1.1-2.2.2-snapshot")) {
            Throwable t = expectThrows(
                IllegalArgumentException.class,
                "failed to throw for: [" + v + "]",
                () -> SqlSemVersion.fromString(v)
            );
            assertThat(t.getMessage(), containsString("Invalid version format [" + v + "]"));
        }
    }
}
