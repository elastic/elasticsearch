/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.ExistsQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.NotQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;

public class IsNulls extends ExpressionTranslator<IsNull> {
    @Override
    protected Query asQuery(IsNull isNull, TranslatorHandler handler) {
        return doTranslate(isNull, handler);
    }

    public static Query doTranslate(IsNull isNull, TranslatorHandler handler) {
        return handler.wrapFunctionQuery(isNull, isNull.field(), () -> translate(isNull, handler));
    }

    private static Query translate(IsNull isNull, TranslatorHandler handler) {
        return new NotQuery(isNull.source(), new ExistsQuery(isNull.source(), handler.nameOf(isNull.field())));
    }
}
