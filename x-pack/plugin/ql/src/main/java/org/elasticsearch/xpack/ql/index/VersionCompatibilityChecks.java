/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.Version.V_8_2_0;
import static org.elasticsearch.Version.V_8_4_0;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;

public final class VersionCompatibilityChecks {

    public static final Version INTRODUCING_UNSIGNED_LONG = V_8_2_0;
    public static final TransportVersion INTRODUCING_UNSIGNED_LONG_TRANSPORT = TransportVersion.V_8_2_0;
    public static final Version INTRODUCING_VERSION_FIELD_TYPE = V_8_4_0;

    private VersionCompatibilityChecks() {}

    public static boolean isTypeSupportedInVersion(DataType dataType, Version version) {
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
    public static boolean supportsUnsignedLong(Version version) {
        return INTRODUCING_UNSIGNED_LONG.compareTo(version) <= 0;
    }

    /**
     * Does the provided {@code version} support the version type (PR#85502)?
     */
    public static boolean supportsVersionType(Version version) {
        return INTRODUCING_VERSION_FIELD_TYPE.compareTo(version) <= 0;
    }

    public static @Nullable Version versionIntroducingType(DataType dataType) {
        if (dataType == UNSIGNED_LONG) {
            return INTRODUCING_UNSIGNED_LONG;
        }
        if (dataType == VERSION) {
            return INTRODUCING_VERSION_FIELD_TYPE;
        }

        return null;
    }
}
