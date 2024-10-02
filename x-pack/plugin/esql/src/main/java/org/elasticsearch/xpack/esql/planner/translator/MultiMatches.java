/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.MultiMatchQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;

public class MultiMatches extends ExpressionTranslator<MultiMatchQueryPredicate> {

    @Override
    protected Query asQuery(MultiMatchQueryPredicate q, TranslatorHandler handler) {
        return doTranslate(q, handler);
    }

    public static Query doTranslate(MultiMatchQueryPredicate q, TranslatorHandler handler) {
        return new MultiMatchQuery(q.source(), q.query(), q.fields(), q);
    }
}
