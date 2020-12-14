/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.jdbc;


import java.sql.SQLType;
import java.sql.Types;

public enum EsType implements SQLType {

    NULL(                     Types.NULL),
    UNSUPPORTED(              Types.OTHER),
    BOOLEAN(                  Types.BOOLEAN),
    BYTE(                     Types.TINYINT),
    SHORT(                    Types.SMALLINT),
    INTEGER(                  Types.INTEGER),
    LONG(                     Types.BIGINT),
    DOUBLE(                   Types.DOUBLE),
    FLOAT(                    Types.REAL),
    HALF_FLOAT(               Types.FLOAT),
    SCALED_FLOAT(             Types.FLOAT),
    KEYWORD(                  Types.VARCHAR),
    TEXT(                     Types.VARCHAR),
    OBJECT(                   Types.STRUCT),
    NESTED(                   Types.STRUCT),
    BINARY(                   Types.VARBINARY),
    DATE(                     Types.DATE),
    TIME(                     Types.TIME),
    DATETIME(                 Types.TIMESTAMP),
    IP(                       Types.VARCHAR),
    INTERVAL_YEAR(            ExtraTypes.INTERVAL_YEAR),
    INTERVAL_MONTH(           ExtraTypes.INTERVAL_MONTH),
    INTERVAL_YEAR_TO_MONTH(   ExtraTypes.INTERVAL_YEAR_MONTH),
    INTERVAL_DAY(             ExtraTypes.INTERVAL_DAY),
    INTERVAL_HOUR(            ExtraTypes.INTERVAL_HOUR),
    INTERVAL_MINUTE(          ExtraTypes.INTERVAL_MINUTE),
    INTERVAL_SECOND(          ExtraTypes.INTERVAL_SECOND),
    INTERVAL_DAY_TO_HOUR(     ExtraTypes.INTERVAL_DAY_HOUR),
    INTERVAL_DAY_TO_MINUTE(   ExtraTypes.INTERVAL_DAY_MINUTE),
    INTERVAL_DAY_TO_SECOND(   ExtraTypes.INTERVAL_DAY_SECOND),
    INTERVAL_HOUR_TO_MINUTE(  ExtraTypes.INTERVAL_HOUR_MINUTE),
    INTERVAL_HOUR_TO_SECOND(  ExtraTypes.INTERVAL_HOUR_SECOND),
    INTERVAL_MINUTE_TO_SECOND(ExtraTypes.INTERVAL_MINUTE_SECOND),
    GEO_POINT(                ExtraTypes.GEOMETRY),
    GEO_SHAPE(                ExtraTypes.GEOMETRY),
    SHAPE(                    ExtraTypes.GEOMETRY);

    private final Integer type;

    EsType(int type) {
        this.type = Integer.valueOf(type);
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
