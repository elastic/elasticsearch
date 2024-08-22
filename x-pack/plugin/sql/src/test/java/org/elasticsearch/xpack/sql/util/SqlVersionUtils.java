/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.util;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.List;

import static org.elasticsearch.xpack.ql.index.VersionCompatibilityChecks.INTRODUCING_UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.index.VersionCompatibilityChecks.INTRODUCING_VERSION_FIELD_TYPE;

public final class SqlVersionUtils {

    public static final TransportVersion PRE_UNSIGNED_LONG = TransportVersionUtils.getPreviousVersion(INTRODUCING_UNSIGNED_LONG);
    public static final TransportVersion POST_UNSIGNED_LONG = TransportVersionUtils.getNextVersion(INTRODUCING_UNSIGNED_LONG);
    public static final TransportVersion PRE_VERSION_FIELD = TransportVersionUtils.getPreviousVersion(INTRODUCING_VERSION_FIELD_TYPE);
    public static final TransportVersion POST_VERSION_FIELD = TransportVersionUtils.getNextVersion(INTRODUCING_VERSION_FIELD_TYPE);

    public static List<TransportVersion> UNSIGNED_LONG_TEST_VERSIONS = List.of(
        PRE_UNSIGNED_LONG,
        INTRODUCING_UNSIGNED_LONG,
        POST_UNSIGNED_LONG,
        TransportVersion.current()
    );

    public static List<TransportVersion> VERSION_FIELD_TEST_VERSIONS = List.of(
        PRE_VERSION_FIELD,
        INTRODUCING_VERSION_FIELD_TYPE,
        POST_VERSION_FIELD,
        TransportVersion.current()
    );
}
