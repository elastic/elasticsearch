/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.index;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.action.SqlVersionId;

import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;

public final class VersionCompatibilityChecks {

    public static final SqlVersionId INTRODUCING_UNSIGNED_LONG = new SqlVersionId(TransportVersions.V_8_2_0);
    public static final SqlVersionId INTRODUCING_VERSION_FIELD_TYPE = new SqlVersionId(TransportVersions.V_8_4_0);

    private VersionCompatibilityChecks() {}

    public static boolean isTypeSupportedInVersion(DataType dataType, SqlVersionId version) {
        if (dataType == UNSIGNED_LONG) {
            return supportsUnsignedLong(version);
        }
        if (dataType == VERSION) {
            return supportsVersionType(version);
        }
        return true;
    }

    /**
     * Does the provided {@code version} support the unsigned_long type (PR#60050)?
     */
    public static boolean supportsUnsignedLong(SqlVersionId version) {
        return version.onOrAfter(INTRODUCING_UNSIGNED_LONG);
    }

    /**
     * Does the provided {@code version} support the version type (PR#85502)?
     */
    public static boolean supportsVersionType(SqlVersionId version) {
        return version.onOrAfter(INTRODUCING_VERSION_FIELD_TYPE);
    }

    public static @Nullable SqlVersionId versionIntroducingType(DataType dataType) {
        if (dataType == UNSIGNED_LONG) {
            return INTRODUCING_UNSIGNED_LONG;
        }
        if (dataType == VERSION) {
            return INTRODUCING_VERSION_FIELD_TYPE;
        }

        return null;
    }
}
