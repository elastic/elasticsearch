/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.querydsl.query.SingleValueQuery;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds HIGHLIGHT queries through Query DSL. Verification and local planning share this path so they accept the same
 * query forms.
 */
public final class HighlightQueryBuilders {

    private HighlightQueryBuilders() {}

    /** Folded string query text, or {@code null} when the query does not fold to a string. */
    private static String queryTextIfLiteral(Expression query) {
        if (query.foldable() == false) {
            return null;
        }
        Object folded = query.fold(FoldContext.small());
        return folded instanceof BytesRef || folded instanceof String ? BytesRefs.toString(folded) : null;
    }

    /**
     * Checks that the expression contains only full-text functions supported by HIGHLIGHT.
     */
    private static void verifyQueryStructure(Expression expr, List<String> onFields) {
        // TODO: Allow HIGHLIGHT queries to use expressions other than full-text functions.
        switch (expr) {
            case Match match -> requireOnField(fieldName(match.field()), onFields);
            case MatchPhrase matchPhrase -> requireOnField(fieldName(matchPhrase.field()), onFields);
            case QueryString queryString -> {
                String defaultField = queryStringDefaultField(queryString);
                if (defaultField != null) {
                    requireOnField(defaultField, onFields);
                }
            }
            case And and -> {
                verifyQueryStructure(and.left(), onFields);
                verifyQueryStructure(and.right(), onFields);
            }
            case Or or -> {
                verifyQueryStructure(or.left(), onFields);
                verifyQueryStructure(or.right(), onFields);
            }
            case Not not -> verifyQueryStructure(not.field(), onFields);
            // KQL resolves fields while rewriting its query builder. Unknown fields become match-none.
            case Kql kql -> {
            }
            // String literals use query_string semantics over the ON fields.
            case Literal literal when DataType.isString(literal.dataType()) -> {
            }
            default -> throw new IllegalArgumentException(
                "HIGHLIGHT query must be a full-text function (MATCH, MATCH_PHRASE, QSTR, KQL) or a boolean combination of them, found ["
                    + expr.sourceText()
                    + "]"
            );
        }
    }

    /**
     * Verifies that a HIGHLIGHT query uses supported full-text forms, references its {@code onFields}, and translates to
     * a valid Lucene query.
     */
    public static void verify(Expression queryExpr, List<String> onFields) {
        String literal = queryTextIfLiteral(queryExpr);
        // Pushdown accepts more expressions than the runtime context, so check the query shape first.
        if (literal == null) {
            verifyQueryStructure(queryExpr, onFields);
        }
        try {
            // Translate now to report invalid options and syntax before planning.
            buildLuceneQuery(queryExpr, onFields);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "Invalid query [" + (literal != null ? literal : queryExpr.sourceText()) + "] in HIGHLIGHT: " + e.getMessage(),
                e
            );
        }
    }

    private static void requireOnField(String field, List<String> onFields) {
        if (onFields.contains(field) == false) {
            throw new IllegalArgumentException("HIGHLIGHT query field [" + field + "] is not in ON fields " + onFields);
        }
    }

    private static String queryStringDefaultField(QueryString queryString) {
        if (queryString.options() instanceof MapExpression map) {
            Expression value = map.get("default_field");
            if (value != null && value.foldable()) {
                return BytesRefs.toString(value.fold(FoldContext.small()));
            }
        }
        return null;
    }

    /**
     * Translates a HIGHLIGHT expression into a Query DSL {@link QueryBuilder}.
     */
    public static QueryBuilder toQueryBuilder(Expression queryExpr, List<String> onFields) {
        String literal = queryTextIfLiteral(queryExpr);
        if (literal != null) {
            Map<String, Float> fields = new LinkedHashMap<>();
            for (String field : onFields) {
                fields.put(field, 1.0f);
            }
            return new QueryStringQuery(queryExpr.source(), literal, fields, Map.of()).toQueryBuilder();
        }
        return build(queryExpr);
    }

    /**
     * Builds a query for runtime columns, such as those produced by ROW or EVAL, that cannot use normal pushdown because
     * they are not {@link FieldAttribute}s.
     */
    private static QueryBuilder build(Expression expr) {
        boolean runtimeSearch = expr.anyMatch(e -> e instanceof FullTextFunction ftf && ftf.isRuntimeSearch());
        if (runtimeSearch == false) {
            // TODO: MATCH on a union-typed field translates to the underlying field name, which won't match the
            // ON column name the MemoryIndex is keyed by.
            var query = TranslatorHandler.TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, expr);
            if (query instanceof SingleValueQuery) {
                // Structural verification should have rejected this non-full-text expression.
                throw new EsqlIllegalArgumentException("Unexpected pushdown query for expression [" + expr.sourceText() + "] in HIGHLIGHT");
            }
            return query.toQueryBuilder();
        }
        // TODO: Use TranslatorHandler for runtime searches instead of rebuilding the query here.
        return switch (expr) {
            case And and -> QueryBuilders.boolQuery().must(build(and.left())).must(build(and.right()));
            case Or or -> QueryBuilders.boolQuery().should(build(or.left())).should(build(or.right()));
            case Not not -> QueryBuilders.boolQuery().mustNot(build(not.field()));
            case Match match -> QueryBuilders.matchQuery(fieldName(match.field()), queryText(match.query()));
            case MatchPhrase matchPhrase -> QueryBuilders.matchPhraseQuery(fieldName(matchPhrase.field()), queryText(matchPhrase.query()));
            default -> throw new IllegalStateException("Unexpected expression [" + expr.sourceText() + "] in HIGHLIGHT");
        };
    }

    private static String fieldName(Expression field) {
        return field instanceof NamedExpression named ? named.name() : Expressions.name(field);
    }

    private static String queryText(Expression query) {
        return BytesRefs.toString(query.fold(FoldContext.small()));
    }

    /** Rewrites the builder and converts it to a Lucene query. */
    public static Query toLuceneQuery(QueryBuilder builder, SearchExecutionContext context) {
        return context.toQuery(builder).query();
    }

    /** Builds and discards a Lucene query so {@link #verify} can report invalid syntax or options. */
    private static void buildLuceneQuery(Expression queryExpr, List<String> onFields) {
        toLuceneQuery(toQueryBuilder(queryExpr, onFields), RuntimeSearchExecutionContext.create(onFields));
    }

    /**
     * Builds the runtime query after the plan has been deserialized. The returned analyzer comes from the same context
     * as the query and must also be used to populate the operator's MemoryIndex.
     */
    public static TranslatedQuery translate(Expression queryExpr, List<String> fieldNames) {
        String literal = queryTextIfLiteral(queryExpr);
        String queryText = literal != null ? literal : queryExpr.sourceText();
        RuntimeSearchExecutionContext context = RuntimeSearchExecutionContext.create(fieldNames);
        Query query = toLuceneQuery(toQueryBuilder(queryExpr, fieldNames), context);
        return new TranslatedQuery(queryText, query, context.searchAnalyzer());
    }

    /** Runtime query state produced by {@link #translate}. */
    public record TranslatedQuery(String queryText, Query query, Analyzer analyzer) {}
}
