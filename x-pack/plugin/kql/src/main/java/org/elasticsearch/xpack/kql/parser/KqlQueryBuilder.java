/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kql.parser;

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.index.mapper.AbstractScriptFieldType;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.ql.parser.ParserUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class KqlQueryBuilder extends KqlBaseBaseVisitor<QueryBuilder> {

    private static final Logger log = LogManager.getLogger(KqlQueryBuilder.class);

    private final SearchExecutionContext searchExecutionContext;
    private final KqlStringBuilder stringBuilder;

    public KqlQueryBuilder(SearchExecutionContext searchExecutionContext) {
        this.searchExecutionContext = searchExecutionContext;
        this.stringBuilder = new KqlStringBuilder();
    }

    public QueryBuilder query(ParserRuleContext ctx) {
        if (ctx instanceof KqlBaseParser.TopLevelQueryContext topLeveQueryContext) {
            if (topLeveQueryContext.query() == null) {
                // In KQL, empty query matches all docs.
                return new MatchAllQueryBuilder();
            }

            return ParserUtils.typedParsing(this, topLeveQueryContext.query(), QueryBuilder.class);
        }

        throw new IllegalArgumentException("context should be of type TopLevelQueryContext");
    }

    @Override
    public QueryBuilder visitLogicalNot(KqlBaseParser.LogicalNotContext ctx) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        return builder.mustNot(ParserUtils.typedParsing(this, ctx.simpleQuery(), QueryBuilder.class));
    }

    @Override
    public QueryBuilder visitLogicalAnd(KqlBaseParser.LogicalAndContext ctx) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery();

        // TODO: KQLContext has an option to wrap the clauses into a filter instead of a must clause. Do we need it?

        for (ParserRuleContext subQuery : ctx.query()) {
            if (subQuery instanceof KqlBaseParser.LogicalAndContext) {
                ParserUtils.typedParsing(this, subQuery, BoolQueryBuilder.class).must().forEach(builder::must);
            } else {
                builder.must(ParserUtils.typedParsing(this, subQuery, QueryBuilder.class));
            }
        }

        return builder;
    }

    @Override
    public QueryBuilder visitLogicalOr(KqlBaseParser.LogicalOrContext ctx) {
        BoolQueryBuilder builder = QueryBuilders.boolQuery();

        for (ParserRuleContext subQuery : ctx.query()) {
            if (subQuery instanceof KqlBaseParser.LogicalOrContext) {
                ParserUtils.typedParsing(this, subQuery, BoolQueryBuilder.class).should().forEach(builder::should);
            } else {
                builder.should(ParserUtils.typedParsing(this, subQuery, QueryBuilder.class));
            }
        }

        return builder;
    }

    @Override
    public QueryBuilder visitParenthesizedQuery(KqlBaseParser.ParenthesizedQueryContext ctx) {
        return ParserUtils.typedParsing(this, ctx.query(), QueryBuilder.class);
    }

    @Override
    public QueryBuilder visitFieldTermQuery(KqlBaseParser.FieldTermQueryContext ctx) {
        if (isMatchAllQuery(ctx)) {
            return QueryBuilders.matchAllQuery();
        }

        String queryText = ctx.termQueryValue().accept(stringBuilder);
        boolean isPhraseQuery = ctx.termQueryValue().quotedStringExpression() != null;
        boolean isWildcardQuery = ctx.termQueryValue().wildcardExpression() != null;

        if (ctx.fieldName() == null) {
            return buildMultiMatchQuery(queryText, isPhraseQuery);
        }

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().minimumShouldMatch(1);
        resolveFields(ctx.fieldName()).forEach(fieldDefinition -> {
            addFieldQuery(boolQueryBuilder, fieldDefinition, queryText, isPhraseQuery, isWildcardQuery);
        });

        return boolQueryBuilder.should().isEmpty() ? new MatchNoneQueryBuilder() : boolQueryBuilder;
    }

    private QueryBuilder buildMultiMatchQuery(String queryText, boolean isPhraseQuery) {
        return QueryBuilders.multiMatchQuery(queryText)
            .type(isPhraseQuery ? MultiMatchQueryBuilder.Type.PHRASE : MultiMatchQueryBuilder.Type.BEST_FIELDS)
            .lenient(true);
    }

    private void addFieldQuery(
        BoolQueryBuilder boolQueryBuilder,
        Map.Entry<String, MappedFieldType> fieldDefinition,
        String queryText,
        boolean isPhraseQuery,
        boolean isWildcardQuery) {

        String fieldName = fieldDefinition.getKey();
        MappedFieldType fieldType = fieldDefinition.getValue();

        if (isRuntimeField(fieldType)) {
            addRuntimeFieldQuery(boolQueryBuilder, fieldName, queryText, isWildcardQuery);
            return;
        }

        if (isWildcardQuery) {
            boolQueryBuilder.should(QueryBuilders.existsQuery(fieldName));
            return;
        }

        if (isDateField(fieldType)) {
            addDateFieldQuery(boolQueryBuilder, fieldName, queryText);
            return;
        }

        if (isKeywordField(fieldType)) {
            addKeywordFieldQuery(boolQueryBuilder, fieldName, queryText, isWildcardQuery);
            return;
        }

        if (isPhraseQuery) {
            boolQueryBuilder.should(QueryBuilders.matchPhraseQuery(fieldName, queryText));
        } else {
            boolQueryBuilder.should(QueryBuilders.matchQuery(fieldName, queryText));
        }
    }

    private boolean isMatchAllQuery(KqlBaseParser.FieldTermQueryContext ctx) {
        boolean isExistsQuery = ctx.termQueryValue().wildcardExpression() != null;
        boolean isAllFieldsQuery = ctx.fieldName() != null && ctx.fieldName().wildcardExpression() != null;
        return (ctx.fieldName() == null || isAllFieldsQuery) && isExistsQuery;
    }

    private void addRuntimeFieldQuery(
        BoolQueryBuilder boolQueryBuilder,
        String fieldName,
        String queryText,
        boolean isWildcardQuery) {
        if (isWildcardQuery == false) {
            // TODO: Implement runtime field query
            log.debug("Runtime field queries not yet implemented for field: {}", fieldName);
        }
    }

    private void addDateFieldQuery(BoolQueryBuilder boolQueryBuilder, String fieldName, String queryText) {
        // TODO: Implement date field query
        // range: { [field.name]: { gte: value, lte: value, ...timeZoneParam }}
        log.debug("Date field queries not yet implemented for field: {}", fieldName);
    }

    private void addKeywordFieldQuery(
        BoolQueryBuilder boolQueryBuilder,
        String fieldName,
        String queryText,
        boolean isWildcardQuery) {
        if (isWildcardQuery) {
            boolQueryBuilder.should(
                QueryBuilders.wildcardQuery(fieldName, queryText).caseInsensitive(true)
            );
        } else {
            boolQueryBuilder.should(
                QueryBuilders.termQuery(fieldName, queryText).caseInsensitive(true)
            );
        }
    }

    @Override
    public QueryBuilder visitFieldRangeQuery(KqlBaseParser.FieldRangeQueryContext ctx) {
        return super.visitFieldRangeQuery(ctx);
    }

    @Override
    public QueryBuilder visitNestedQuery(KqlBaseParser.NestedQueryContext ctx) {
        return super.visitNestedQuery(ctx);
    }

    private Iterable<Map.Entry<String, MappedFieldType>> resolveFields(KqlBaseParser.FieldNameContext fieldNameContext) {
        Iterable<Map.Entry<String, MappedFieldType>> fields = List.of();

        if (fieldNameContext == null || fieldNameContext.wildcardExpression() != null) {
            // TODO: filter out internal fields
            fields = searchExecutionContext.getAllFields();
        } else if (fieldNameContext.quotedStringExpression() != null) {
            String fieldName = fieldNameContext.accept(stringBuilder);
            log.trace("Quoted field name : {}", fieldName);
            if (searchExecutionContext.isFieldMapped(fieldName)) {
                fields = List.of(Map.entry(fieldName, searchExecutionContext.getFieldType(fieldName)));
            }
        }  else {
            String fieldPattern = fieldNameContext.accept(stringBuilder);
            log.trace("Unquoted field name : {}", fieldPattern);
            fields = searchExecutionContext.getMatchingFieldNames(fieldPattern)
                .stream()
                .map(fieldName -> Map.entry(fieldName, searchExecutionContext.getFieldType(fieldName)))
                .collect(Collectors.toList());
        }

        return fields;
    }

    private boolean isRuntimeField(MappedFieldType fieldType) {
        return fieldType instanceof AbstractScriptFieldType<?>;
    }

    private boolean isDateField(MappedFieldType fieldType) {
        return fieldType.typeName().equals(DateFieldMapper.CONTENT_TYPE);
    }

    private boolean isKeywordField(MappedFieldType fieldType) {
        return fieldType.typeName().equals(KeywordFieldMapper.CONTENT_TYPE);
    }
}
