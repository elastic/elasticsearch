/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

class KqlAstBuilder extends KqlBaseBaseVisitor<QueryBuilder> {
    private final SearchExecutionContext searchExecutionContext;

    KqlAstBuilder(SearchExecutionContext searchExecutionContext) {
        this.searchExecutionContext = searchExecutionContext;
    }

    public QueryBuilder toQueryBuilder(ParserRuleContext ctx) {
        if (ctx instanceof KqlBaseParser.TopLevelQueryContext topLeveQueryContext) {
            return new MatchAllQueryBuilder();
        }

        throw new IllegalArgumentException("context should be of type TopLevelQueryContext");
    }
}
