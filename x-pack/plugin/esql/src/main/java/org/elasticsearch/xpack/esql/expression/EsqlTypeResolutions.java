/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expression.TypeResolution;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEO_POINT;

public final class EsqlTypeResolutions {

    public static TypeResolution isGeoPoint(Expression e, String operationName, ParamOrdinal paramOrd) {
        return TypeResolutions.isType(e, dt -> dt == GEO_POINT, operationName, paramOrd, "geo_point");
    }
}
