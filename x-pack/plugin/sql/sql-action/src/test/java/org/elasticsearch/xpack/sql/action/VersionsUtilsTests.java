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
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class VersionsUtilsTests extends ESTestCase {

    public void testFromTransportString() {
        assertEquals(SqlVersion.fromString("7.0.0"), SqlVersion.fromTransportString(TransportVersions.V_7_0_0.toReleaseVersion()));
        assertEquals(SqlVersion.fromString("8.9.2"), SqlVersion.fromTransportString(TransportVersions.V_8_9_X.toReleaseVersion()));
        assertEquals(SqlVersion.fromString("8.12.0"), SqlVersion.fromTransportString(TransportVersions.V_8_12_0.toReleaseVersion()));

        assertEquals(
            SqlVersion.fromString("8.15.0"),
            SqlVersion.fromTransportString(TransportVersions.WATERMARK_THRESHOLDS_STATS.toReleaseVersion())
        );
        assertEquals(
            SqlVersion.fromString("8.15.0"),
            SqlVersion.fromTransportString(TransportVersions.FIX_VECTOR_SIMILARITY_INNER_HITS_BACKPORT_8_15.toReleaseVersion())
        );
        assertEquals(
            SqlVersion.fromString("8.15.1-snapshot[8725000]"),
            SqlVersion.fromTransportString(TransportVersions.ESQL_PROFILE_SLEEPS.toReleaseVersion())
        );

        assertNotNull(SqlVersion.fromTransportString(TransportVersion.current().toReleaseVersion()));
        assertNull(SqlVersion.fromTransportString(null));
        assertNull(SqlVersion.fromTransportString(""));
    }

    public void testFromTransportStringFail() {
        for (String v : List.of("-", "1-1", "1.1.x-1", "1.1.1-2.2.2-3.3.3", "1.1.1-2.2.2-snapshot")) {
            Throwable t = expectThrows(
                IllegalArgumentException.class,
                "failed to throw for: [" + v + "]",
                () -> SqlVersion.fromTransportString(v)
            );
            assertThat(t.getMessage(), containsString("Invalid version format [" + v + "]"));
        }
    }

}
