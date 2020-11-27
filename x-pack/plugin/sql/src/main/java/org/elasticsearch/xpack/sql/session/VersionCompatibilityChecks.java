/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import static org.elasticsearch.Version.V_7_11_0;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.sql.proto.Mode.isDriver;

public final class VersionCompatibilityChecks {

    public static final SqlVersion INTRODUCING_UNSIGNED_LONG = SqlVersion.fromId(V_7_11_0.id);

    private VersionCompatibilityChecks() {}

    public static boolean isTypeSupportedByClient(Mode mode, DataType dataType, SqlVersion version) {
        return isDriver(mode) == false || isTypeSupportedInVersion(dataType, version);
    }
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
        return INTRODUCING_UNSIGNED_LONG.compareTo(version) <= 0;
    }
}
