/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal;
import org.elasticsearch.xpack.sql.type.SqlDataTypes;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isType;

public final class SqlTypeResolutions {

    private SqlTypeResolutions() {}

    public static TypeResolution isDate(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, SqlDataTypes::isDateBased, operationName, paramOrd, "date", "datetime");
    }

    public static TypeResolution isDateOrTime(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, SqlDataTypes::isDateOrTimeBased, operationName, paramOrd, "date", "time", "datetime");
    }

    public static TypeResolution isDateOrInterval(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, SqlDataTypes::isDateOrIntervalBased, operationName, paramOrd, "date", "datetime", "an interval data type");
    }

    public static TypeResolution isNumericOrDate(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, dt -> dt.isNumeric() || SqlDataTypes.isDateBased(dt), operationName, paramOrd,
            "date", "datetime", "numeric");
    }

    public static TypeResolution isNumericOrDateOrTime(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, dt -> dt.isNumeric() || SqlDataTypes.isDateOrTimeBased(dt), operationName, paramOrd,
            "date", "time", "datetime", "numeric");
    }

    public static TypeResolution isGeo(Expression e, String operationName, ParamOrdinal paramOrd) {
        return isType(e, SqlDataTypes::isGeo, operationName, paramOrd, "geo_point", "geo_shape");
    }
}
