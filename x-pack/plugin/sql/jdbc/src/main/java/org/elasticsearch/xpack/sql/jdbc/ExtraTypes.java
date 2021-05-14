/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.jdbc;

import java.sql.JDBCType;
import java.sql.Types;

/**
 * Provides ODBC-based codes for the missing SQL data types from {@link Types}/{@link JDBCType}.
 */
class ExtraTypes {

    private ExtraTypes() {}

    static final int INTERVAL_YEAR = 101;
    static final int INTERVAL_MONTH = 102;
    static final int INTERVAL_DAY = 103;
    static final int INTERVAL_HOUR = 104;
    static final int INTERVAL_MINUTE = 105;
    static final int INTERVAL_SECOND = 106;
    static final int INTERVAL_YEAR_MONTH = 107;
    static final int INTERVAL_DAY_HOUR = 108;
    static final int INTERVAL_DAY_MINUTE = 109;
    static final int INTERVAL_DAY_SECOND = 110;
    static final int INTERVAL_HOUR_MINUTE = 111;
    static final int INTERVAL_HOUR_SECOND = 112;
    static final int INTERVAL_MINUTE_SECOND = 113;
    static final int GEOMETRY = 114;

}
