/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;

public class Matches extends ExpressionTranslator<MatchQueryPredicate> {
    @Override
    protected Query asQuery(MatchQueryPredicate q, TranslatorHandler handler) {
        return doTranslate(q, handler);
    }

    public static Query doTranslate(MatchQueryPredicate q, TranslatorHandler handler) {
        return new MatchQuery(q.source(), handler.nameOf(q.field()), q.query(), q);
    }
}
