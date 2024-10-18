/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.function.Consumer;

class KqlAstBuilder extends KqlBaseBaseVisitor<QueryBuilder> {
    private final SearchExecutionContext searchExecutionContext;

    KqlAstBuilder(SearchExecutionContext searchExecutionContext) {
        this.searchExecutionContext = searchExecutionContext;
    }

    public QueryBuilder toQueryBuilder(ParserRuleContext ctx) {
        if (ctx instanceof KqlBaseParser.TopLevelQueryContext topLeveQueryContext) {
            if (topLeveQueryContext.query() != null) {
                return ParserUtils.typedParsing(this, topLeveQueryContext.query(), QueryBuilder.class);
            }

            return new MatchAllQueryBuilder();
        }

        throw new IllegalArgumentException("context should be of type TopLevelQueryContext");
    }

    @Override
    public QueryBuilder visitBooleanQuery(KqlBaseParser.BooleanQueryContext ctx) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        Consumer<QueryBuilder> clauseAdder = ctx.AND() != null ? builder::must : builder::should;

        // TODO: KQLContext has an option to wrap the clauses into a filter instead of a must clause. Do we need it?

        for (ParserRuleContext subQueryCtx : ctx.query()) {
            if (subQueryCtx instanceof KqlBaseParser.BooleanQueryContext booleanSubQueryCtx
                && (booleanSubQueryCtx.AND() == null) == (ctx.AND() == null)) {
                BoolQueryBuilder parsedSubQuery = ParserUtils.typedParsing(this, subQueryCtx, BoolQueryBuilder.class);
                (ctx.AND() != null ? parsedSubQuery.must() : parsedSubQuery.should()).forEach(clauseAdder);
            } else {
                clauseAdder.accept(ParserUtils.typedParsing(this, subQueryCtx, QueryBuilder.class));
            }
        }

        return builder;
    }

    @Override
    public QueryBuilder visitNotQuery(KqlBaseParser.NotQueryContext ctx) {
        return QueryBuilders.boolQuery().mustNot(ParserUtils.typedParsing(this, ctx.simpleQuery(), QueryBuilder.class));
    }

    @Override
    public QueryBuilder visitExpression(KqlBaseParser.ExpressionContext ctx) {
        // TODO: implementation
        return new MatchNoneQueryBuilder();
    }

    @Override
    public QueryBuilder visitParenthesizedQuery(KqlBaseParser.ParenthesizedQueryContext ctx) {
        return ParserUtils.typedParsing(this, ctx.query(), QueryBuilder.class);
    }

    @Override
    public QueryBuilder visitNestedQuery(KqlBaseParser.NestedQueryContext ctx) {
        // TODO: implementation
        return new MatchNoneQueryBuilder();
    }
}
