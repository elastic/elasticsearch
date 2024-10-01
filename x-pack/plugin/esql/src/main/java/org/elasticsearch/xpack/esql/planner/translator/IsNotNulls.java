/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.ExistsQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;

public class IsNotNulls extends ExpressionTranslator<IsNotNull> {
    @Override
    protected Query asQuery(IsNotNull isNotNull, TranslatorHandler handler) {
        return doTranslate(isNotNull, handler);
    }

    public static Query doTranslate(IsNotNull isNotNull, TranslatorHandler handler) {
        return handler.wrapFunctionQuery(isNotNull, isNotNull.field(), () -> translate(isNotNull, handler));
    }

    private static Query translate(IsNotNull isNotNull, TranslatorHandler handler) {
        return new ExistsQuery(isNotNull.source(), handler.nameOf(isNotNull.field()));
    }
}
