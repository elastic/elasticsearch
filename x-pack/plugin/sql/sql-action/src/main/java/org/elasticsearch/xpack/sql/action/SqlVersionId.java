/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.VersionId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

public final class SqlVersionId implements VersionId<SqlVersionId> {

    private final int id;

    // String representation of the current release version, used in error messages.
    // Read from the build info, if this contains a SemVer version, otherwise from the transport version.
    // Will be the TrasnportVersion's id() in Serverless (and not a SemVer string).
    private static final String CURRENT_RELEASE;

    // Value to initialize the SQL session or requests with, in case the client doesn't send a version.
    public static final SqlVersionId CURRENT = from(TransportVersion.current());
    // The first version that supports versioning.
    public static final SqlVersionId INTRODUCING_VERSION_COMPATIBILITY = from(TransportVersions.V_7_7_0);
    // last version whose ID is compatible with the SemVer versioning scheme.
    public static final SqlVersionId LAST_SEM_VER_COMPATIBLE = from(TransportVersions.V_8_8_1);

    static {
        String currentVersion;
        try {
            SqlVersion current = SqlVersion.fromString(Build.current().version());
            currentVersion = current.version;
        } catch (IllegalArgumentException iae) {
            currentVersion = TransportVersion.current().toReleaseVersion();
        }
        CURRENT_RELEASE = currentVersion;
    }

    SqlVersionId(int id) {
        this.id = id;
    }

    public static SqlVersionId from(VersionId<?> id) {
        return new SqlVersionId(id.id());
    }

    @Nullable
    public static SqlVersionId from(SqlVersion sqlVersion) {
        if (sqlVersion == null) return null;

        // Promote any version below the last SemVer compatible version to CURRENT. Past that point an ID-based comparison isn't possible,
        // since the IDs are generated differently. For instance an 8.16.0 - with the SqlVersion ID of 8_16_00_99 - will be considered
        // before TransportVersion.V_8_12_0, with the TransportVersion ID of 8_560_00_0.
        // This is just a workaround, since there's currently no way to map from a Sql/Version back to a TransportVersion.
        return sqlVersion.id > LAST_SEM_VER_COMPATIBLE.id() ? CURRENT : new SqlVersionId(sqlVersion.id);
    }

    // for testing only
    public static SqlVersionId fromSemVerString(String version) {
        return from(SqlVersion.fromString(version));
    }

    /** A client is version-compatible with the server if it supports version compatibility (on or past 7.7.0).
     *
     * Note: The need for client to be at most one major behind can no longer be enforced in a SemVer-less environment.
     * Also, the client will send the server the stack release version which can't be compared to TransportVersion-based version, since
     * it can happen that:
     *  TransportVersion.current().toReleaseVersion() != Build.current().version()
     * i.e. the former will return the next patch of current equivalent stack version, while the latter will return the next minor.
     *
     * @param client SqlVersionId of the client.
     * @return true if the client is compatible with the server.
     */
    public static boolean isClientCompatible(SqlVersionId client) {
        return client.onOrAfter(INTRODUCING_VERSION_COMPATIBILITY);
    }

    public static boolean isSemVerCompatible(SqlVersionId version) {
        return version.onOrBefore(LAST_SEM_VER_COMPATIBLE);
    }

    /**
     * The version id this object represents
     */
    @Override
    public int id() {
        return id;
    }

    public SqlVersion sqlVersion() {
        if (isSemVerCompatible(this)) return SqlVersion.fromId(id);

        throw new IllegalArgumentException(
            "Cannot convert incompatible SqlVersionId[" + toReleaseVersion() + "[" + id + "]] to a SqlVersion object."
        );
    }

    public String toReleaseVersion() {
        return TransportVersion.fromId(id).toReleaseVersion();
    }

    /**
     * Returns the current release version of the stack.
     */
    public static String currentRelease() {
        return CURRENT_RELEASE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        return (o.getClass() == getClass()) && ((SqlVersionId) o).id == id;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
