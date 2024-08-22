/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

public final class VersionsUtils {

    public static final SqlVersion CURRENT = SqlVersion.fromString(Build.current().version());

    // Translating a TransportVersion to a SqlVersion/Version must go through the former's string representation, which involves a
    // mapping; the .id() can't be used directly.
    public static SqlVersion from(TransportVersion transportVersion) {
        return SqlVersion.fromTransportString(transportVersion.toReleaseVersion());
    }

    // Translating a SqlVersion/Version to a TransportVersion can make use of the .id() directly, as the latter's looks up the known
    // IDs of the stack releases.
    public static TransportVersion from(SqlVersion sqlVersion) {
        return TransportVersion.fromId(sqlVersion.id);
    }
}
