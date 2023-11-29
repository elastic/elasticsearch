/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.Locale;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;

public class EsqlTypeResolutions {

    public static Expression.TypeResolution isExact(Expression e, String operationName, TypeResolutions.ParamOrdinal paramOrd) {
        if (e instanceof FieldAttribute fa) {
            if (DataTypes.isString(fa.dataType())) {
                // ESQL can extract exact values for TEXT fields
                return Expression.TypeResolution.TYPE_RESOLVED;
            }
            EsField.Exact exact = fa.getExactInfo();
            if (exact.hasExact() == false) {
                return new Expression.TypeResolution(
                    format(
                        null,
                        "[{}] cannot operate on {}field of data type [{}]: {}",
                        operationName,
                        paramOrd == null || paramOrd == DEFAULT ? "" : paramOrd.name().toLowerCase(Locale.ROOT) + " argument ",
                        e.dataType().typeName(),
                        exact.errorMsg()
                    )
                );
            }
        }
        return Expression.TypeResolution.TYPE_RESOLVED;
    }
}
