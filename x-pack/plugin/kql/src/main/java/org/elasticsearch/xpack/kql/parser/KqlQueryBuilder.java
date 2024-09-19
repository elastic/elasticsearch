/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.xpack.ql.parser.ParserUtils;

public class KqlQueryBuilder extends KqlBaseBaseVisitor<Query> {

    private static final Logger log = LogManager.getLogger(KqlQueryBuilder.class);

    public Query query(ParserRuleContext ctx) {
        if (ctx instanceof KqlBaseParser.TopLevelQueryContext topLeveQueryContext) {
            return ParserUtils.typedParsing(this, topLeveQueryContext.query(), Query.class);
        }

        throw new IllegalArgumentException("context should be of type TopLevelQueryContext");
    }

    @Override
    public Query visitLogicalNot(KqlBaseParser.LogicalNotContext ctx) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        Query subQuery = super.visitLogicalNot(ctx);

        return builder.add(subQuery, BooleanClause.Occur.MUST_NOT).build();
    }

    @Override
    public BooleanQuery visitLogicalAnd(KqlBaseParser.LogicalAndContext ctx) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        for (ParserRuleContext subQuery: ctx.query()) {
            if (subQuery instanceof KqlBaseParser.LogicalAndContext booleanSubQuery) {
                this.visitLogicalAnd(booleanSubQuery).clauses().forEach(builder::add);
            } else {
                builder.add(this.visit(subQuery), BooleanClause.Occur.MUST);
            }
        }

        return builder.build();
    }

    @Override
    public BooleanQuery visitLogicalOr(KqlBaseParser.LogicalOrContext ctx) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();

        for (ParserRuleContext subQuery: ctx.query()) {
            if (subQuery instanceof KqlBaseParser.LogicalOrContext booleanSubQuery) {
                this.visitLogicalOr(booleanSubQuery).clauses().forEach(builder::add);
            } else {
                builder.add(this.visit(subQuery), BooleanClause.Occur.SHOULD);
            }
        }

        return builder.build();
    }


    @Override
    public Query visitParenthesizedQuery(KqlBaseParser.ParenthesizedQueryContext ctx) {
        return this.visit(ctx.query());
    }

    @Override
    public Query visitExpression(KqlBaseParser.ExpressionContext ctx) {
        return new MatchAllDocsQuery();
    }

    @Override
    public Query visitFieldRangeQuery(KqlBaseParser.FieldRangeQueryContext ctx) {
        return super.visitFieldRangeQuery(ctx);
    }

    @Override
    public Query visitNestedQuery(KqlBaseParser.NestedQueryContext ctx) {
        return super.visitNestedQuery(ctx);
    }

    @Override
    public Query visitFieldTermQuery(KqlBaseParser.FieldTermQueryContext ctx) {
        return super.visitFieldTermQuery(ctx);
    }
}
