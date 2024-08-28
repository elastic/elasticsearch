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
import org.elasticsearch.xpack.sql.proto.SqlVersion;

public class SqlVersionId implements VersionId<SqlVersionId> {

    private final int id;

    // String representation of the current release version, used in error messages.
    // Read from the build info, if this contains a SemVer version, otherwise from the transport version.
    // Will be the TrasnportVersion's id() in Serverless (and not a SemVer string).
    private static final String CURRENT_RELEASE;

    // Value to initialize the SQL session or requests with, in case the client doesn't send a version.
    public static final SqlVersionId CURRENT = new SqlVersionId(TransportVersion.current());
    // The first version that supports versioning.
    public static final SqlVersionId INTRODUCING_VERSION_COMPATIBILITY = new SqlVersionId(TransportVersions.V_7_7_0);

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

    public SqlVersionId(VersionId<?> id) {
        this(id.id());
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

    /**
     * The version id this object represents
     */
    @Override
    public int id() {
        return id;
    }

    public SqlVersion sqlVersion() {
        throw new UnsupportedOperationException("cannot convert a generic SqlVersionId to a SqlVersion object.");
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
}
