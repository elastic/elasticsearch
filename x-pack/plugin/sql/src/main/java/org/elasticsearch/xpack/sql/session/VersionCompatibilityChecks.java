/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import static org.elasticsearch.Version.V_7_16_0;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

public final class VersionCompatibilityChecks {

    public static final SqlVersion INTRODUCING_UNSIGNED_LONG = SqlVersion.fromId(V_7_16_0.id);

    private VersionCompatibilityChecks() {}

    /**
     * Is the provided {@code dataType} being supported in the provided {@code version}?
     */
    public static boolean isTypeSupportedInVersion(DataType dataType, SqlVersion version) {
        if (dataType == UNSIGNED_LONG) {
            return supportsUnsignedLong(version);
        }
        return true;
    }
    /**
     * Does the provided {@code version} support the unsigned_long type (PR#60050)?
     */
    public static boolean supportsUnsignedLong(SqlVersion version) {
        // TODO: add equality only once actually ported to 7.16
        return INTRODUCING_UNSIGNED_LONG.compareTo(version) < 0;
    }
}
