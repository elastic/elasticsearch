/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.sql.action.SqlSemVersion;
import org.elasticsearch.xpack.sql.action.SqlVersionId;

import java.util.List;
import java.util.Random;

import static org.elasticsearch.xpack.sql.index.VersionCompatibilityChecks.INTRODUCING_UNSIGNED_LONG;
import static org.elasticsearch.xpack.sql.index.VersionCompatibilityChecks.INTRODUCING_VERSION_FIELD_TYPE;
import static org.elasticsearch.xpack.sql.proto.SqlVersion.DATE_NANOS_SUPPORT_VERSION;

public final class SqlVersionIdUtils {

    public static final SqlVersionId PRE_UNSIGNED_LONG = getPreviousVersion(INTRODUCING_UNSIGNED_LONG);
    public static final SqlVersionId POST_UNSIGNED_LONG = getNextVersion(INTRODUCING_UNSIGNED_LONG);
    public static final SqlVersionId PRE_VERSION_FIELD = getPreviousVersion(INTRODUCING_VERSION_FIELD_TYPE);
    public static final SqlVersionId POST_VERSION_FIELD = getNextVersion(INTRODUCING_VERSION_FIELD_TYPE);

    public static final SqlVersionId INTRODUCING_DATE_NANOS = new SqlSemVersion(DATE_NANOS_SUPPORT_VERSION);

    public static List<SqlVersionId> UNSIGNED_LONG_TEST_VERSIONS = List.of(
        PRE_UNSIGNED_LONG,
        INTRODUCING_UNSIGNED_LONG,
        POST_UNSIGNED_LONG,
        SqlVersionId.CURRENT
    );

    public static List<SqlVersionId> VERSION_FIELD_TEST_VERSIONS = List.of(
        PRE_VERSION_FIELD,
        INTRODUCING_VERSION_FIELD_TYPE,
        POST_VERSION_FIELD,
        SqlVersionId.CURRENT
    );

    public static SqlVersionId getPreviousVersion(SqlVersionId versionId) {
        TransportVersion transport = TransportVersion.fromId(versionId.id());
        TransportVersion pre = TransportVersionUtils.getPreviousVersion(transport);
        return new SqlVersionId(pre);
    }

    public static SqlVersionId getNextVersion(SqlVersionId versionId) {
        TransportVersion transport = TransportVersion.fromId(versionId.id());
        TransportVersion next = TransportVersionUtils.getNextVersion(transport);
        return new SqlVersionId(next);
    }

    public static SqlVersionId randomVersion() {
        return new SqlVersionId(TransportVersionUtils.randomVersion());
    }

    public static SqlVersionId randomVersionBetween(Random random, SqlVersionId from, SqlVersionId to) {
        TransportVersion fromTransport = TransportVersion.fromId(from.id());
        TransportVersion toTransport = TransportVersion.fromId(to.id());
        TransportVersion randomTransport = TransportVersionUtils.randomVersionBetween(random, fromTransport, toTransport);
        return new SqlVersionId(randomTransport);
    }

    public static SqlVersionId getFirstVersion() {
        return new SqlVersionId(TransportVersionUtils.getFirstVersion());
    }

    public static List<SqlVersionId> allReleasedVersions() {
        return TransportVersionUtils.allReleasedVersions().stream().map(SqlVersionId::new).toList();
    }
}
