/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.Version;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.ql.type.DataType;

import static org.elasticsearch.Version.V_8_2_0;
import static org.elasticsearch.xpack.ql.type.DataTypes.UNSIGNED_LONG;

public final class VersionCompatibilityChecks {

    public static final Version INTRODUCING_UNSIGNED_LONG = V_8_2_0;

    private VersionCompatibilityChecks() {}

    public static boolean isTypeSupportedInVersion(DataType dataType, Version version) {
        if (dataType == UNSIGNED_LONG) {
            return supportsUnsignedLong(version);
        }
        return true;
    }

    /**
     * Does the provided {@code version} support the unsigned_long type (PR#60050)?
     */
    public static boolean supportsUnsignedLong(Version version) {
        return INTRODUCING_UNSIGNED_LONG.compareTo(version) <= 0;
    }

    public static @Nullable Version versionIntroducingType(DataType dataType) {
        if (dataType == UNSIGNED_LONG) {
            return INTRODUCING_UNSIGNED_LONG;
        }

        return null;
    }
}
