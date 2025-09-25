/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

import java.util.List;

/**
 * Marker class for simplified full-text search over all text fields in an index represented in ES|QL as:
 *   "FROM index | WHERE query"
 *
 * This will be resolved to {@link MultiMatch} over all text fields during the analysis phase.
 */
public class FullTextSearch extends MultiMatch {

    public FullTextSearch(Source source, Expression query) {
        super(source, query, List.of(new UnresolvedStar(source, null)), null);
    }

    @Override
    protected Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        throw new IllegalStateException("FullTextSearch function should be resolved to MULTI_MATCH before translation");
    }
}
