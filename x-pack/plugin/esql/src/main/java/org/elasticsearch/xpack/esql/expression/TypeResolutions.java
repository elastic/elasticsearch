/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.type.EsField;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

public final class TypeResolutions {
    public static Expression.TypeResolution isExact(Expression e, String message) {
        if (e instanceof FieldAttribute fa) {
            EsField.Exact exact = fa.getExactInfo();
            if (exact.hasExact() == false) {
                return new Expression.TypeResolution(format(null, message, e.dataType().typeName(), exact.errorMsg()));
            }
        }
        return Expression.TypeResolution.TYPE_RESOLVED;
    }
}
