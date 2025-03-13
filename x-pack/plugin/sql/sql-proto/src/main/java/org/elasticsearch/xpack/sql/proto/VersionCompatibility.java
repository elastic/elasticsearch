/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto;

public class VersionCompatibility {

    public static final SqlVersion INTRODUCING_VERSION_COMPATIBILITY = SqlVersions.V_7_7_0;
    public static final SqlVersion INTRODUCING_DATE_NANOS = SqlVersions.V_7_12_0;
    public static final SqlVersion INTRODUCING_UNSIGNED_LONG = SqlVersions.V_8_2_0;
    public static final SqlVersion INTRODUCING_WARNING_HANDLING = SqlVersions.V_8_2_0;
    public static final SqlVersion INTRODUCING_VERSION_FIELD_TYPE = SqlVersions.V_8_4_0;
    public static final SqlVersion INTRODUCING_VERSIONING_INDEPENDENT_FEATURES = SqlVersions.V_8_16_0;

    public static boolean hasVersionCompatibility(SqlVersion version) {
        return version.onOrAfter(INTRODUCING_VERSION_COMPATIBILITY);
    }

    /** Is the client on or past a version that is SQL-specific, independent of stack versioning? */
    public static boolean hasVersioningIndependentFeatures(SqlVersion version) {
        return version.onOrAfter(INTRODUCING_VERSIONING_INDEPENDENT_FEATURES);
    }

    // A client is version-compatible with the server if:
    // - it is on or past the version-independent feature gating version; OR
    // - it supports version compatibility (past or on 7.7.0); AND
    // - it's not on a version newer than server's; AND
    // - it's major version is at most one unit behind server's.
    public static boolean isClientCompatible(SqlVersion server, SqlVersion client) {
        if (hasVersioningIndependentFeatures(client)) {
            return true;
        }
        // ES's CURRENT not available (core not a dependency), so it needs to be passed in as a parameter.
        return hasVersionCompatibility(client) && server.onOrAfter(client) && server.major - client.major <= 1;
    }

    // TODO: move to VersionCompatibilityChecks
    public static boolean supportsDateNanos(SqlVersion version) {
        return version.onOrAfter(INTRODUCING_DATE_NANOS);
    }

    /**
     * Does the provided {@code version} support the unsigned_long type (PR#60050)?
     */
    public static boolean supportsUnsignedLong(SqlVersion version) {
        return version.onOrAfter(INTRODUCING_UNSIGNED_LONG);
    }

    /**
     * Does the provided {@code version} support the version type (PR#85502)?
     */
    public static boolean supportsVersionType(SqlVersion version) {
        return version.onOrAfter(INTRODUCING_VERSION_FIELD_TYPE);
    }
}
