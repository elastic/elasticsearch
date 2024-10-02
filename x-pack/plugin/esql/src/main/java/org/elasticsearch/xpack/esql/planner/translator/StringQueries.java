/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.translator;

import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.esql.core.planner.ExpressionTranslator;
import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;

public class StringQueries extends ExpressionTranslator<StringQueryPredicate> {
    @Override
    protected Query asQuery(StringQueryPredicate q, TranslatorHandler handler) {
        return doTranslate(q, handler);
    }

    public static Query doTranslate(StringQueryPredicate q, TranslatorHandler handler) {
        return new QueryStringQuery(q.source(), q.query(), q.fields(), q);
    }
}
