/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.type;

import java.sql.JDBCType;
import java.sql.SQLType;
import java.sql.Types;

/**
 * Provides ODBC-based codes for the missing SQL data types from {@link Types}/{@link JDBCType}.
 */
enum ExtTypes implements SQLType {

    INTERVAL_YEAR(101),
    INTERVAL_MONTH(102),
    INTERVAL_DAY(103),
    INTERVAL_HOUR(104),
    INTERVAL_MINUTE(105),
    INTERVAL_SECOND(106),
    INTERVAL_YEAR_TO_MONTH(107),
    INTERVAL_DAY_TO_HOUR(108),
    INTERVAL_DAY_TO_MINUTE(109),
    INTERVAL_DAY_TO_SECOND(110),
    INTERVAL_HOUR_TO_MINUTE(111),
    INTERVAL_HOUR_TO_SECOND(112),
    INTERVAL_MINUTE_TO_SECOND(113),
    GEOMETRY(114);

    private final Integer type;

    ExtTypes(Integer type) {
        this.type = type;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String getVendor() {
        return "org.elasticsearch";
    }

    @Override
    public Integer getVendorTypeNumber() {
        return type;
    }
}
