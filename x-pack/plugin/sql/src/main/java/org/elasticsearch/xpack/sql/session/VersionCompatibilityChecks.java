/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.Version.V_7_15_0;
import static org.elasticsearch.xpack.ql.type.DataTypes.isArray;
import static org.elasticsearch.xpack.sql.type.SqlDataTypes.types;

public final class VersionCompatibilityChecks {

    public static final SqlVersion INTRODUCING_ARRAY_TYPES = SqlVersion.fromId(V_7_15_0.id); // TODO: update if merging in 7.16.0

    private static final Map<DataType, SqlVersion> TYPE_TO_VERSION_MAP = new HashMap<>();

    static {
        types().stream().filter(DataTypes::isArray).forEach(x -> TYPE_TO_VERSION_MAP.put(x, INTRODUCING_ARRAY_TYPES));
        // Note: future new array types (introduced with new ES types) will require a mapping update here below.
    }

    private VersionCompatibilityChecks() {}

    /**
     * Is the provided {@code dataType} being supported in the provided {@code version}?
     */
    public static boolean isTypeSupportedInVersion(DataType dataType, SqlVersion version) {
        if (isArray(dataType)) {
            return supportsArrayTypes(version);
        }
        return true;
    }
    /**
     * Does the provided {@code version} support the array types?
     */
    public static boolean supportsArrayTypes(SqlVersion version) {
        // TODO: add equality only once actually ported to target branch
        return version.compareTo(INTRODUCING_ARRAY_TYPES) > 0;
    }

    public static SqlVersion versionIntroducingType(DataType type) {
        return TYPE_TO_VERSION_MAP.get(type);
    }
}
