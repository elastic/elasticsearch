/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.xpack.kql.parser.KqlParsingContext.isDateField;
import static org.elasticsearch.xpack.kql.parser.KqlParsingContext.isKeywordField;
import static org.elasticsearch.xpack.kql.parser.KqlParsingContext.isRuntimeField;
import static org.elasticsearch.xpack.kql.parser.KqlParsingContext.isSearchableField;
import static org.elasticsearch.xpack.kql.parser.ParserUtils.escapeLuceneQueryString;
import static org.elasticsearch.xpack.kql.parser.ParserUtils.extractText;
import static org.elasticsearch.xpack.kql.parser.ParserUtils.hasWildcard;
import static org.elasticsearch.xpack.kql.parser.ParserUtils.typedParsing;

class KqlAstBuilder extends KqlBaseBaseVisitor<QueryBuilder> {
    private final KqlParsingContext kqlParsingContext;

    KqlAstBuilder(KqlParsingContext kqlParsingContext) {
        this.kqlParsingContext = kqlParsingContext;
    }

    public QueryBuilder toQueryBuilder(ParserRuleContext ctx) {
        if (ctx instanceof KqlBaseParser.TopLevelQueryContext topLeveQueryContext) {
            if (topLeveQueryContext.query() != null) {
                return typedParsing(this, topLeveQueryContext.query(), QueryBuilder.class);
            }

            return new MatchAllQueryBuilder();
        }

        throw new IllegalArgumentException("context should be of type TopLevelQueryContext");
    }

    @Override
    public QueryBuilder visitBooleanQuery(KqlBaseParser.BooleanQueryContext ctx) {
        assert ctx.operator != null;
        return isAndQuery(ctx) ? visitAndBooleanQuery(ctx.query()) : visitOrBooleanQuery(ctx.query());
    }

    public QueryBuilder visitAndBooleanQuery(List<? extends ParserRuleContext> clauses) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery();

        // TODO: KQLContext has an option to wrap the clauses into a filter instead of a must clause. Do we need it?
        for (ParserRuleContext subQueryCtx : clauses) {
            if (isAndQuery(subQueryCtx)) {
                typedParsing(this, subQueryCtx, BoolQueryBuilder.class).must().forEach(builder::must);
            } else {
                builder.must(typedParsing(this, subQueryCtx, QueryBuilder.class));
            }
        }

        return rewriteConjunctionQuery(builder);
    }

    public QueryBuilder visitOrBooleanQuery(List<? extends ParserRuleContext> clauses) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery().minimumShouldMatch(1);

        for (ParserRuleContext subQueryCtx : clauses) {
            if (isOrQuery(subQueryCtx)) {
                typedParsing(this, subQueryCtx, BoolQueryBuilder.class).should().forEach(builder::should);
            } else {
                builder.should(typedParsing(this, subQueryCtx, QueryBuilder.class));
            }
        }

        return rewriteDisjunctionQuery(builder);
    }

    @Override
    public QueryBuilder visitNotQuery(KqlBaseParser.NotQueryContext ctx) {
        return QueryBuilders.boolQuery().mustNot(typedParsing(this, ctx.simpleQuery(), QueryBuilder.class));
    }

    @Override
    public QueryBuilder visitParenthesizedQuery(KqlBaseParser.ParenthesizedQueryContext ctx) {
        return typedParsing(this, ctx.query(), QueryBuilder.class);
    }

    @Override
    public QueryBuilder visitNestedQuery(KqlBaseParser.NestedQueryContext ctx) {
        String nestedFieldName = extractText(ctx.fieldName());

        if (kqlParsingContext.isNestedField(nestedFieldName) == false) {
            throw new KqlParsingException(
                "[{}] is not a valid nested field name.",
                ctx.start.getLine(),
                ctx.start.getCharPositionInLine(),
                nestedFieldName
            );
        }
        QueryBuilder subQuery = kqlParsingContext.withNestedPath(
            nestedFieldName,
            () -> typedParsing(this, ctx.nestedSubQuery(), QueryBuilder.class)
        );

        if (subQuery instanceof MatchNoneQueryBuilder) {
            return subQuery;
        }

        return wrapWithNestedQuery(
            nestedFieldName,
            QueryBuilders.nestedQuery(kqlParsingContext.fullFieldName(nestedFieldName), subQuery, ScoreMode.None)
        );
    }

    @Override
    public QueryBuilder visitBooleanNestedQuery(KqlBaseParser.BooleanNestedQueryContext ctx) {
        assert ctx.operator != null;
        return isAndQuery(ctx) ? visitAndBooleanQuery(ctx.nestedSubQuery()) : visitOrBooleanQuery(ctx.nestedSubQuery());
    }

    @Override
    public QueryBuilder visitNestedParenthesizedQuery(KqlBaseParser.NestedParenthesizedQueryContext ctx) {
        return typedParsing(this, ctx.nestedSubQuery(), QueryBuilder.class);
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
                boolQueryBuilder.should(wrapWithNestedQuery(fieldName, QueryBuilders.existsQuery(fieldName)));
            }
        });

        return rewriteDisjunctionQuery(boolQueryBuilder);
    }

    @Override
    public QueryBuilder visitRangeQuery(KqlBaseParser.RangeQueryContext ctx) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumShouldMatch(1);

        String queryText = extractText(ctx.rangeQueryValue());
        BiFunction<RangeQueryBuilder, String, RangeQueryBuilder> rangeOperation = rangeOperation(ctx.operator);

        withFields(ctx.fieldName(), (fieldName, mappedFieldType) -> {
            RangeQueryBuilder rangeQuery = rangeOperation.apply(QueryBuilders.rangeQuery(fieldName), queryText);

            if (kqlParsingContext.timeZone() != null) {
                rangeQuery.timeZone(kqlParsingContext.timeZone().getId());
            }

            boolQueryBuilder.should(wrapWithNestedQuery(fieldName, rangeQuery));
        });

        return rewriteDisjunctionQuery(boolQueryBuilder);
    }

    @Override
    public QueryBuilder visitFieldLessQuery(KqlBaseParser.FieldLessQueryContext ctx) {
        String queryText = extractText(ctx.fieldQueryValue());

        if (hasWildcard(ctx.fieldQueryValue())) {
            QueryStringQueryBuilder queryString = QueryBuilders.queryStringQuery(escapeLuceneQueryString(queryText, true));
            if (kqlParsingContext.defaultField() != null) {
                queryString.defaultField(kqlParsingContext.defaultField());
            }
            return queryString;
        }

        boolean isPhraseMatch = ctx.fieldQueryValue().QUOTED_STRING() != null;

        MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(queryText)
            .type(isPhraseMatch ? MultiMatchQueryBuilder.Type.PHRASE : MultiMatchQueryBuilder.Type.BEST_FIELDS)
            .lenient(true);

        if (kqlParsingContext.defaultField() != null) {
            kqlParsingContext.resolveDefaultFieldNames()
                .stream()
                .filter(kqlParsingContext::isSearchableField)
                .forEach(multiMatchQuery::field);
        }

        return multiMatchQuery;
    }

    @Override
    public QueryBuilder visitFieldQuery(KqlBaseParser.FieldQueryContext ctx) {

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumShouldMatch(1);
        String queryText = extractText(ctx.fieldQueryValue());
        boolean hasWildcard = hasWildcard(ctx.fieldQueryValue());

        withFields(ctx.fieldName(), (fieldName, mappedFieldType) -> {
            QueryBuilder fieldQuery = null;

            if (hasWildcard && isKeywordField(mappedFieldType)) {
                fieldQuery = QueryBuilders.wildcardQuery(fieldName, queryText).caseInsensitive(kqlParsingContext.caseInsensitive());
            } else if (hasWildcard) {
                fieldQuery = QueryBuilders.queryStringQuery(escapeLuceneQueryString(queryText, true)).field(fieldName);
            } else if (isDateField(mappedFieldType)) {
                RangeQueryBuilder rangeFieldQuery = QueryBuilders.rangeQuery(fieldName).gte(queryText).lte(queryText);
                if (kqlParsingContext.timeZone() != null) {
                    rangeFieldQuery.timeZone(kqlParsingContext.timeZone().getId());
                }
                fieldQuery = rangeFieldQuery;
            } else if (isKeywordField(mappedFieldType)) {
                fieldQuery = QueryBuilders.termQuery(fieldName, queryText).caseInsensitive(kqlParsingContext.caseInsensitive());
            } else if (ctx.fieldQueryValue().QUOTED_STRING() != null) {
                fieldQuery = QueryBuilders.matchPhraseQuery(fieldName, queryText);
            } else {
                fieldQuery = QueryBuilders.matchQuery(fieldName, queryText);
            }

            if (fieldQuery != null) {
                boolQueryBuilder.should(wrapWithNestedQuery(fieldName, fieldQuery));
            }
        });

        return rewriteDisjunctionQuery(boolQueryBuilder);
    }

    private static boolean isAndQuery(ParserRuleContext ctx) {
        return switch (ctx) {
            case KqlBaseParser.BooleanQueryContext booleanQueryCtx -> booleanQueryCtx.operator.getType() == KqlBaseParser.AND;
            case KqlBaseParser.BooleanNestedQueryContext booleanNestedCtx -> booleanNestedCtx.operator.getType() == KqlBaseParser.AND;
            default -> false;
        };
    }

    private static boolean isOrQuery(ParserRuleContext ctx) {
        return switch (ctx) {
            case KqlBaseParser.BooleanQueryContext booleanQueryCtx -> booleanQueryCtx.operator.getType() == KqlBaseParser.OR;
            case KqlBaseParser.BooleanNestedQueryContext booleanNestedCtx -> booleanNestedCtx.operator.getType() == KqlBaseParser.OR;
            default -> false;
        };
    }

    private void withFields(KqlBaseParser.FieldNameContext ctx, BiConsumer<String, MappedFieldType> fieldConsummer) {
        assert ctx != null : "Field ctx cannot be null";
        String fieldNamePattern = extractText(ctx);

        Set<String> fieldNames = kqlParsingContext.resolveFieldNames(fieldNamePattern);

        if (ctx.value.getType() == KqlBaseParser.QUOTED_STRING && Regex.isSimpleMatchPattern(fieldNamePattern)) {
            // When using quoted string, wildcards are not expanded.
            // No field can match and we can return early.
            return;
        }

        if (ctx.value.getType() == KqlBaseParser.QUOTED_STRING) {
            assert fieldNames.size() < 2 : "expecting only one matching field";
        }

        fieldNames.forEach(fieldName -> {
            MappedFieldType fieldType = kqlParsingContext.fieldType(fieldName);
            if (isSearchableField(fieldName, fieldType)) {
                fieldConsummer.accept(fieldName, fieldType);
            }
        });
    }

    private QueryBuilder rewriteDisjunctionQuery(BoolQueryBuilder boolQueryBuilder) {
        assert boolQueryBuilder.must().isEmpty() && boolQueryBuilder.filter().isEmpty() && boolQueryBuilder.mustNot().isEmpty();

        if (boolQueryBuilder.should().isEmpty()) {
            return new MatchNoneQueryBuilder();
        }

        return boolQueryBuilder.should().size() == 1 ? boolQueryBuilder.should().getFirst() : boolQueryBuilder;
    }

    private QueryBuilder rewriteConjunctionQuery(BoolQueryBuilder boolQueryBuilder) {
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

    private QueryBuilder wrapWithNestedQuery(String fieldName, QueryBuilder query) {
        String nestedPath = kqlParsingContext.nestedPath(fieldName);

        if (nestedPath == null || nestedPath.equals(kqlParsingContext.currentNestedPath())) {
            return query;
        }

        return wrapWithNestedQuery(nestedPath, QueryBuilders.nestedQuery(nestedPath, query, ScoreMode.None));
    }
}
