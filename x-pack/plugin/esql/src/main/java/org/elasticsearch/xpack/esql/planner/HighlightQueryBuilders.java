/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.highlight.HighlightSupport;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds HIGHLIGHT queries through Query DSL. Verification and local planning share this path so they accept the same
 * query forms.
 */
public final class HighlightQueryBuilders {

    /**
     * Name of HIGHLIGHT's default analyzer. When no analyzer is requested, callers resolve this name from the node's
     * {@link AnalysisRegistry} so the default matches the registered {@code standard} analyzer (including its position
     * increment gap). The runtime context registers it under this public name so nested full-text functions can name it
     * in their own {@code analyzer} option.
     */
    public static final String DEFAULT_ANALYZER_NAME = "standard";

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
    private static void verifyQueryStructure(Expression expr, @Nullable List<String> onFields) {
        // TODO: Allow HIGHLIGHT queries to use expressions other than full-text functions.
        switch (expr) {
            case Match match -> requireOnField(fieldName(match.field()), onFields);
            case MatchPhrase matchPhrase -> requireOnField(fieldName(matchPhrase.field()), onFields);
            case QueryString queryString -> {
                String defaultField = HighlightSupport.queryStringDefaultField(queryString);
                if (defaultField != null) {
                    requireOnField(defaultField, onFields);
                }
            }
            case BinaryLogic binary -> {
                verifyQueryStructure(binary.left(), onFields);
                verifyQueryStructure(binary.right(), onFields);
            }
            case Not not -> verifyQueryStructure(not.field(), onFields);
            // KQL resolves fields while rewriting its query builder; against a lenient context an unknown field
            // resolves to nothing and the clause becomes match-none, so nothing is checked here.
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

    private static void requireOnField(String field, @Nullable List<String> onFields) {
        if (onFields != null && onFields.contains(field) == false) {
            throw new IllegalArgumentException("HIGHLIGHT query field [" + field + "] is not in ON fields " + onFields);
        }
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

    // This switch, verifyQueryStructure above, and HighlightSupport#isSupportedImplicitPredicate/deriveFields all walk
    // the same full-text expression hierarchy and must stay in sync as new forms are added. This one is the most
    // important to keep current: a missing case here throws IllegalStateException at runtime, not at verification.
    private static QueryBuilder build(Expression expr) {
        return switch (expr) {
            case And and -> QueryBuilders.boolQuery().must(build(and.left())).must(build(and.right()));
            case Or or -> QueryBuilders.boolQuery().should(build(or.left())).should(build(or.right()));
            case Not not -> QueryBuilders.boolQuery().mustNot(build(not.field()));
            case Match match -> match.asLexicalQueryBuilder(fieldName(match.field()));
            case MatchPhrase matchPhrase -> matchPhrase.asLexicalQueryBuilder(fieldName(matchPhrase.field()));
            case QueryString queryString -> pushdownQueryBuilder(queryString);
            case Kql kql -> pushdownQueryBuilder(kql);
            default -> throw new IllegalStateException("Unexpected expression [" + expr.sourceText() + "] in HIGHLIGHT");
        };
    }

    private static QueryBuilder pushdownQueryBuilder(Expression expr) {
        return TranslatorHandler.TRANSLATOR_HANDLER.asQuery(LucenePushdownPredicates.DEFAULT, expr).toQueryBuilder();
    }

    private static String fieldName(Expression field) {
        return field instanceof NamedExpression named ? named.name() : Expressions.name(field);
    }

    /** Rewrites the builder and converts it to a Lucene query. */
    public static Query toLuceneQuery(QueryBuilder builder, SearchExecutionContext context) {
        return context.toQuery(builder).query();
    }

    /** Per-field context plus extra analyzer names ({@code quote_analyzer}, off-ON leaf analyzers) when a registry is present. */
    private static RuntimeSearchExecutionContext runtimeContext(
        Expression queryExpr,
        Map<String, NamedAnalyzer> fieldAnalyzers,
        boolean lenientFields,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        return RuntimeSearchExecutionContext.create(
            fieldAnalyzers,
            resolveExtraAnalyzers(HighlightSupport.leafAnalyzerNamesOf(queryExpr), analysisRegistry),
            lenientFields
        );
    }

    /** Resolves {@code extraAnalyzerNames} through {@code analysisRegistry}, or an empty map when either is empty. */
    private static Map<String, NamedAnalyzer> resolveExtraAnalyzers(
        Set<String> extraAnalyzerNames,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        if (analysisRegistry == null || extraAnalyzerNames.isEmpty()) {
            return Map.of();
        }
        Map<String, NamedAnalyzer> extraAnalyzers = new LinkedHashMap<>();
        for (String extraName : extraAnalyzerNames) {
            extraAnalyzers.put(extraName, namedAnalyzer(extraName, analysisRegistry));
        }
        return extraAnalyzers;
    }

    /** resolveAnalyzer always returns a NamedAnalyzer carrying the text-field position increment gap. */
    private static NamedAnalyzer namedAnalyzer(String name, AnalysisRegistry analysisRegistry) {
        return (NamedAnalyzer) PlannerUtils.resolveAnalyzer(name, analysisRegistry);
    }

    /**
     * Resolves {@code fieldAnalyzerNames} (field to analyzer name, from {@link HighlightSupport#fieldAnalyzers})
     * through {@code analysisRegistry}. Distinct names are resolved once and shared across fields.
     */
    public static Map<String, NamedAnalyzer> resolveFieldAnalyzers(
        Map<String, String> fieldAnalyzerNames,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        Map<String, NamedAnalyzer> resolvedByName = new HashMap<>();
        Map<String, NamedAnalyzer> result = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : fieldAnalyzerNames.entrySet()) {
            String name = entry.getValue();
            NamedAnalyzer resolved = resolvedByName.computeIfAbsent(name, n -> namedAnalyzer(n, analysisRegistry));
            result.put(entry.getKey(), resolved);
        }
        return result;
    }

    /**
     * Verifies that a HIGHLIGHT query uses supported full-text forms, references only the fields of
     * {@code fieldAnalyzers} (when {@code enforceOnFields}), and translates against the same per-field analyzers
     * execution will use. An {@code implicit} query may name fields outside ON and is translated leniently
     * (match-none) rather than failing; its errors are framed as derived from WHERE.
     */
    public static void verify(
        Expression queryExpr,
        Map<String, NamedAnalyzer> fieldAnalyzers,
        boolean enforceOnFields,
        boolean implicit,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        List<String> fields = List.copyOf(fieldAnalyzers.keySet());
        String literal = queryTextIfLiteral(queryExpr);
        if (literal == null) {
            verifyQueryStructure(queryExpr, enforceOnFields ? fields : null);
        }
        try {
            translate(queryExpr, fields, runtimeContext(queryExpr, fieldAnalyzers, implicit, analysisRegistry));
        } catch (RuntimeException e) {
            String prefix = implicit
                ? "Invalid query derived from WHERE for HIGHLIGHT: "
                : "Invalid query [" + (literal != null ? literal : queryExpr.sourceText()) + "] in HIGHLIGHT: ";
            throw new IllegalArgumentException(prefix + e.getMessage(), e);
        }
    }

    private static void translate(Expression queryExpr, List<String> fieldNames, RuntimeSearchExecutionContext context) {
        toLuceneQuery(toQueryBuilder(queryExpr, fieldNames), context);
    }

    /**
     * Resolves {@code fieldAnalyzerNames} per field, then builds the runtime query against a per-field analyzer
     * context. The context is always lenient about fields outside {@code fieldAnalyzerNames}: verification has
     * already rejected any explicit query naming such a field.
     */
    public static TranslatedQuery translate(
        Expression queryExpr,
        Map<String, String> fieldAnalyzerNames,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        Map<String, NamedAnalyzer> fieldAnalyzers = resolveFieldAnalyzers(fieldAnalyzerNames, analysisRegistry);
        List<String> fieldNames = List.copyOf(fieldAnalyzerNames.keySet());
        RuntimeSearchExecutionContext context = runtimeContext(queryExpr, fieldAnalyzers, true, analysisRegistry);
        String literal = queryTextIfLiteral(queryExpr);
        String queryText = literal != null ? literal : queryExpr.sourceText();
        Query query = toLuceneQuery(toQueryBuilder(queryExpr, fieldNames), context);
        return new TranslatedQuery(queryText, query, fieldAnalyzers);
    }

    /** Runtime query state produced by {@link #translate(Expression, Map, AnalysisRegistry)}. */
    public record TranslatedQuery(String queryText, Query query, Map<String, NamedAnalyzer> fieldAnalyzers) {}
}
