/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.kql.parser.KqlParserExecutionContext.isDateField;
import static org.elasticsearch.xpack.kql.parser.KqlParserExecutionContext.isKeywordField;
import static org.elasticsearch.xpack.kql.parser.KqlParserExecutionContext.isRuntimeField;
import static org.elasticsearch.xpack.kql.parser.ParserUtils.escapeLuceneQueryString;
import static org.elasticsearch.xpack.kql.parser.ParserUtils.hasWildcard;

class KqlAstBuilder extends KqlBaseBaseVisitor<QueryBuilder> {
    private final KqlParserExecutionContext kqlParserExecutionContext;

    KqlAstBuilder(KqlParserExecutionContext kqlParserExecutionContext) {
        this.kqlParserExecutionContext = kqlParserExecutionContext;
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

        return rewriteConjonctionQuery(builder);
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

        return rewriteDisjunctionQuery(builder);
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
        assert ctx.fieldName() != null; // Should not happen since the grammar does not allow the fieldname to be null.

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumShouldMatch(1);
        withFields(ctx.fieldName(), (fieldName, mappedFieldType) -> {
            if (isRuntimeField(mappedFieldType) == false) {
                boolQueryBuilder.should(QueryBuilders.existsQuery(fieldName));
            }
        });

        return rewriteDisjunctionQuery(boolQueryBuilder);
    }

    @Override
    public QueryBuilder visitRangeQuery(KqlBaseParser.RangeQueryContext ctx) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumShouldMatch(1);

        String queryText = ParserUtils.extractText(ctx.rangeQueryValue());
        BiFunction<RangeQueryBuilder, String, RangeQueryBuilder> rangeOperation = rangeOperation(ctx.operator);

        withFields(ctx.fieldName(), (fieldName, mappedFieldType) -> {
            RangeQueryBuilder rangeQuery = rangeOperation.apply(QueryBuilders.rangeQuery(fieldName), queryText);
            // TODO: add timezone for date fields
            boolQueryBuilder.should(rangeQuery);
        });

        return rewriteDisjunctionQuery(boolQueryBuilder);
    }

    @Override
    public QueryBuilder visitFieldLessQuery(KqlBaseParser.FieldLessQueryContext ctx) {
        String queryText = ParserUtils.extractText(ctx.fieldQueryValue());

        if (hasWildcard(ctx.fieldQueryValue())) {
            // TODO: set default fields.
            return QueryBuilders.queryStringQuery(escapeLuceneQueryString(queryText, true));
        }

        boolean isPhraseMatch = ctx.fieldQueryValue().QUOTED_STRING() != null;

        return QueryBuilders.multiMatchQuery(queryText)
            // TODO: add default fields?
            .type(isPhraseMatch ? MultiMatchQueryBuilder.Type.PHRASE : MultiMatchQueryBuilder.Type.BEST_FIELDS)
            .lenient(true);
    }

    @Override
    public QueryBuilder visitFieldQuery(KqlBaseParser.FieldQueryContext ctx) {

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumShouldMatch(1);
        String queryText = ParserUtils.extractText(ctx.fieldQueryValue());
        boolean hasWildcard = hasWildcard(ctx.fieldQueryValue());

        withFields(ctx.fieldName(), (fieldName, mappedFieldType) -> {
            QueryBuilder fieldQuery = null;

            if (hasWildcard && isKeywordField(mappedFieldType)) {
                fieldQuery = QueryBuilders.wildcardQuery(fieldName, queryText)
                    .caseInsensitive(kqlParserExecutionContext.isCaseSensitive() == false);
            } else if (hasWildcard) {
                fieldQuery = QueryBuilders.queryStringQuery(escapeLuceneQueryString(queryText, true)).field(fieldName);
            } else if (isDateField(mappedFieldType)) {
                // TODO: add timezone
                fieldQuery = QueryBuilders.rangeQuery(fieldName).gte(queryText).lte(queryText);
            } else if (isKeywordField(mappedFieldType)) {
                fieldQuery = QueryBuilders.termQuery(fieldName, queryText)
                    .caseInsensitive(kqlParserExecutionContext.isCaseSensitive() == false);
            } else if (ctx.fieldQueryValue().QUOTED_STRING() != null) {
                fieldQuery = QueryBuilders.matchPhraseQuery(fieldName, queryText);
            } else {
                fieldQuery = QueryBuilders.matchQuery(fieldName, queryText);
            }

            if (fieldQuery != null) {
                boolQueryBuilder.should(fieldQuery);
            }
        });

        return rewriteDisjunctionQuery(boolQueryBuilder);
    }

    private static boolean isAndQuery(KqlBaseParser.BooleanQueryContext ctx) {
        return ctx.operator.getType() == KqlBaseParser.AND;
    }

    private static boolean isOrQuery(KqlBaseParser.BooleanQueryContext ctx) {
        return ctx.operator.getType() == KqlBaseParser.OR;
    }

    private void withFields(KqlBaseParser.FieldNameContext ctx, BiConsumer<String, MappedFieldType> fieldConsummer) {
        kqlParserExecutionContext.resolveFields(ctx).forEach(fieldDef -> fieldConsummer.accept(fieldDef.v1(), fieldDef.v2()));
    }

    private QueryBuilder rewriteDisjunctionQuery(BoolQueryBuilder boolQueryBuilder) {
        assert boolQueryBuilder.must().isEmpty() && boolQueryBuilder.filter().isEmpty() && boolQueryBuilder.mustNot().isEmpty();

        if (boolQueryBuilder.should().isEmpty()) {
            return new MatchNoneQueryBuilder();
        }

        return boolQueryBuilder.should().size() == 1 ? boolQueryBuilder.should().getFirst() : boolQueryBuilder;
    }

    private QueryBuilder rewriteConjonctionQuery(BoolQueryBuilder boolQueryBuilder) {
        assert boolQueryBuilder.should().isEmpty() && boolQueryBuilder.filter().isEmpty() && boolQueryBuilder.mustNot().isEmpty();

        if (boolQueryBuilder.must().isEmpty()) {
            return new MatchNoneQueryBuilder();
        }

        return boolQueryBuilder.must().size() == 1 ? boolQueryBuilder.must().getFirst() : boolQueryBuilder;
    }

    private BiFunction<RangeQueryBuilder, String, RangeQueryBuilder> rangeOperation(Token operator) {
        return switch (operator.getType()) {
            case KqlBaseParser.OP_LESS -> RangeQueryBuilder::lt;
            case KqlBaseParser.OP_LESS_EQ -> RangeQueryBuilder::lte;
            case KqlBaseParser.OP_MORE -> RangeQueryBuilder::gt;
            case KqlBaseParser.OP_MORE_EQ -> RangeQueryBuilder::gte;
            default -> throw new IllegalArgumentException(format(null, "Invalid range operator {}\"", operator.getText()));
        };
    }
}
