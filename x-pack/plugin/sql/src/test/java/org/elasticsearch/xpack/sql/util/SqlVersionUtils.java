/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersions;

import java.util.List;

import static org.elasticsearch.xpack.sql.proto.SqlVersions.SERVER_COMPAT_VERSION;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.INTRODUCING_UNSIGNED_LONG;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.INTRODUCING_VERSION_FIELD_TYPE;

public final class SqlVersionUtils {

    public static final SqlVersion PRE_UNSIGNED_LONG = SqlVersions.getPreviousVersion(INTRODUCING_UNSIGNED_LONG);
    public static final SqlVersion POST_UNSIGNED_LONG = SqlVersions.getNextVersion(INTRODUCING_UNSIGNED_LONG);
    public static final SqlVersion PRE_VERSION_FIELD = SqlVersions.getPreviousVersion(INTRODUCING_VERSION_FIELD_TYPE);
    public static final SqlVersion POST_VERSION_FIELD = SqlVersions.getNextVersion(INTRODUCING_VERSION_FIELD_TYPE);

    public static List<SqlVersion> UNSIGNED_LONG_TEST_VERSIONS = List.of(
        PRE_UNSIGNED_LONG,
        INTRODUCING_UNSIGNED_LONG,
        POST_UNSIGNED_LONG,
        SERVER_COMPAT_VERSION
    );

    public static List<SqlVersion> VERSION_FIELD_TEST_VERSIONS = List.of(
        PRE_VERSION_FIELD,
        INTRODUCING_VERSION_FIELD_TYPE,
        POST_VERSION_FIELD,
        SERVER_COMPAT_VERSION
    );
}
