/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.whitelist;

import org.elasticsearch.xpack.sql.expression.function.scalar.datetime.DateTimeFunction;

/**
 * Whitelisted class for SQL scripts.
 * Acts as a registry of the various static methods used <b>internally</b> by the scalar functions
 * (to simplify the whitelist definition).
 */
public final class InternalSqlScriptUtils {

    private InternalSqlScriptUtils() {}

    public static Integer dateTimeChrono(long millis, String tzId, String chronoName) {
        return DateTimeFunction.dateTimeChrono(millis, tzId, chronoName);
    }
}
