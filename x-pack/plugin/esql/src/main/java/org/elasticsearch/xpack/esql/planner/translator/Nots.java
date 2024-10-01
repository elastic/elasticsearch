/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;

public class Nots extends ExpressionTranslator<Not> {
    @Override
    protected Query asQuery(Not not, TranslatorHandler handler) {
        return doTranslate(not, handler);
    }

    public static Query doTranslate(Not not, TranslatorHandler handler) {
        Query wrappedQuery = handler.asQuery(not.field());
        Query q = wrappedQuery.negate(not.source());
        return q;
    }
}
