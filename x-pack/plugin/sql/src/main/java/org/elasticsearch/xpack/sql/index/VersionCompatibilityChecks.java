/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.INTRODUCING_UNSIGNED_LONG;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.INTRODUCING_VERSION_FIELD_TYPE;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.supportsUnsignedLong;
import static org.elasticsearch.xpack.sql.proto.VersionCompatibility.supportsVersionType;

public final class VersionCompatibilityChecks {

    public static final TransportVersion INTRODUCING_UNSIGNED_LONG_TRANSPORT = TransportVersions.V_8_2_0;

    private VersionCompatibilityChecks() {}

    public static boolean isTypeSupportedInVersion(DataType dataType, SqlVersion version) {
        if (dataType == UNSIGNED_LONG) {
            return supportsUnsignedLong(version);
        }
        if (dataType == VERSION) {
            return supportsVersionType(version);
        }
        return true;
    }

    public static @Nullable SqlVersion versionIntroducingType(DataType dataType) {
        if (dataType == UNSIGNED_LONG) {
            return INTRODUCING_UNSIGNED_LONG;
        }
        if (dataType == VERSION) {
            return INTRODUCING_VERSION_FIELD_TYPE;
        }

        return null;
    }
}
