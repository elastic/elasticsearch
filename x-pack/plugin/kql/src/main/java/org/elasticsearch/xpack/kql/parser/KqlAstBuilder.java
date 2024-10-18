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
        assert ctx.operator != null;
        return isAndQuery(ctx) ? visitAndBooleanQuery(ctx) : visitOrBooleanQuery(ctx);
    }

    public QueryBuilder visitAndBooleanQuery(KqlBaseParser.BooleanQueryContext ctx) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery();

        // TODO: KQLContext has an option to wrap the clauses into a filter instead of a must clause. Do we need it?
        for (ParserRuleContext subQueryCtx : ctx.query()) {
            if (subQueryCtx instanceof KqlBaseParser.BooleanQueryContext booleanSubQueryCtx && isAndQuery(booleanSubQueryCtx)) {
                ParserUtils.typedParsing(this, subQueryCtx, BoolQueryBuilder.class).must().forEach(builder::must);
            } else {
                builder.must(ParserUtils.typedParsing(this, subQueryCtx, QueryBuilder.class));
            }
        }

        return builder;
    }

    public QueryBuilder visitOrBooleanQuery(KqlBaseParser.BooleanQueryContext ctx) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery().minimumShouldMatch(1);

        for (ParserRuleContext subQueryCtx : ctx.query()) {
            if (subQueryCtx instanceof KqlBaseParser.BooleanQueryContext booleanSubQueryCtx && isOrQuery(booleanSubQueryCtx)) {
                ParserUtils.typedParsing(this, subQueryCtx, BoolQueryBuilder.class).should().forEach(builder::should);
            } else {
                builder.should(ParserUtils.typedParsing(this, subQueryCtx, QueryBuilder.class));
            }
        }

        return builder;
    }

    @Override
    public QueryBuilder visitNotQuery(KqlBaseParser.NotQueryContext ctx) {
        return QueryBuilders.boolQuery().mustNot(ParserUtils.typedParsing(this, ctx.simpleQuery(), QueryBuilder.class));
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

    @Override
    public QueryBuilder visitMatchAllQuery(KqlBaseParser.MatchAllQueryContext ctx) {
        return new MatchAllQueryBuilder();
    }

    @Override
    public QueryBuilder visitExistsQuery(KqlBaseParser.ExistsQueryContext ctx) {
        // TODO: implementation
        return new MatchNoneQueryBuilder();
    }

    @Override
    public QueryBuilder visitRangeQuery(KqlBaseParser.RangeQueryContext ctx) {
        // TODO: implementation
        return new MatchNoneQueryBuilder();
    }

    @Override
    public QueryBuilder visitTermQuery(KqlBaseParser.TermQueryContext ctx) {
        // TODO: implementation
        return new MatchNoneQueryBuilder();
    }

    @Override
    public QueryBuilder visitPhraseQuery(KqlBaseParser.PhraseQueryContext ctx) {
        // TODO: implementation
        return new MatchNoneQueryBuilder();
    }

    private static boolean isAndQuery(KqlBaseParser.BooleanQueryContext ctx) {
        return ctx.operator.getType() == KqlBaseParser.AND;
    }

    private static boolean isOrQuery(KqlBaseParser.BooleanQueryContext ctx) {
        return ctx.operator.getType() == KqlBaseParser.OR;
    }
}
