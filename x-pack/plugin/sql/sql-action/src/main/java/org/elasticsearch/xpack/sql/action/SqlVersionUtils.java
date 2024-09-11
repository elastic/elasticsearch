/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.elasticsearch.xpack.sql.proto.SqlVersions;

public final class SqlVersionUtils {
    /**
     * What's the version of the server that the clients should be compatible with?
     * This will be either the stack version, or SqlVersions.getLatestVersion() if the stack version is not available.
     */
    public static final SqlVersion SERVER_COMPAT_VERSION = getServerCompatVersion();

    private static SqlVersion getServerCompatVersion() {
        try {
            return SqlVersion.fromString(Build.current().version());
        } catch (Exception e) {
            return SqlVersions.getLatestVersion();
        }
    }
}
