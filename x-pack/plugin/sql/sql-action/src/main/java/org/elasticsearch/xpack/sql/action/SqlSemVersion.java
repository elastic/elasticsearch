/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

/**
 * This is a specialization of the SqlVersionId that maps to the SemVer versioning scheme in use by the SQL API (wrapping SqlVersion).
 */
public final class SqlSemVersion extends SqlVersionId {

    private final SqlVersion sqlVersion;

    public SqlSemVersion(SqlVersion sqlVersion) {
        // Note: currently, all 8.x stack releases have an ID smaller than current transport versions. With 9.x, this might change.
        // With the possible consequence that SqlSemVersion(9.0.0) > SqlVersionId(Transport.current()).
        // TODO: will need to find a way to map a 9.x stack release to a transport version and its ID, if we'll ever need to introduce
        // new features. Most likely, we'll just use the transport IDs on the clients and thus SqlSemVersion will cater for the "old"
        // released clients only.
        super(sqlVersion.id);
        this.sqlVersion = sqlVersion;
    }

    @Override
    public SqlVersion sqlVersion() {
        return sqlVersion;
    }

    @Override
    public String toReleaseVersion() {
        return sqlVersion.version;
    }

    @Nullable
    public static SqlSemVersion fromString(String version) {
        SqlVersion ver = SqlVersion.fromString(version);
        return ver == null ? null : new SqlSemVersion(ver);
    }
}
