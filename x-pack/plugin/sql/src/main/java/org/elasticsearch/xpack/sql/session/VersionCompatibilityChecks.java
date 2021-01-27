/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.session;

import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

/**
 * The class contains checks gating newly introduced functionality that is exposed to clients, but not supported by those older than the
 * version introducing the respective feature.
 * Each new feature will have a method added, taking a version and returning a boolean reflecting that version's support of the feature.
 * The comparison can only have the equal added once the feature is backported (see JdbcTestUtils.java). Example:
 * <code>
 *     public static boolean supportsUnsignedLong(SqlVersion version) {
 *         // TODO: add equality only once actually ported to 7.11
 *         return VERSION_INTRODUCING_UNSIGNED_LONG.compareTo(version) &lt; 0;
 *     }
 * </code>
 */
public final class VersionCompatibilityChecks {

    private VersionCompatibilityChecks() {}

    /**
     * Is the provided {@code dataType} being supported in the provided {@code version}?
     */
    public static boolean isAvailable(DataType dataType, SqlVersion version) {
        return true;
    }
}
