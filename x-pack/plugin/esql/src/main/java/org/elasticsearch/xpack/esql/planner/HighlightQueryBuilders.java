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
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds HIGHLIGHT queries through Query DSL. Verification and local planning share this path so they accept the same
 * query forms.
 */
public final class HighlightQueryBuilders {

    /**
     * HIGHLIGHT's default analyzer name. Callers resolve it from {@link AnalysisRegistry} so nested full-text
     * functions can name {@code standard} in their own {@code analyzer} option.
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

    /** Checks HIGHLIGHT only contains supported full-text functions. */
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
        return toQueryBuilder(queryExpr, onFields, false);
    }

    public static QueryBuilder toQueryBuilder(Expression queryExpr, List<String> onFields, boolean overrideLeafAnalyzers) {
        String literal = queryTextIfLiteral(queryExpr);
        if (literal != null) {
            Map<String, Float> fields = new LinkedHashMap<>();
            for (String field : onFields) {
                fields.put(field, 1.0f);
            }
            return new QueryStringQuery(queryExpr.source(), literal, fields, Map.of()).toQueryBuilder();
        }
        return build(queryExpr, overrideLeafAnalyzers);
    }

    // Keep in sync with verifyQueryStructure and HighlightSupport#isSupportedImplicitPredicate/deriveFields.
    private static QueryBuilder build(Expression expr, boolean overrideLeafAnalyzers) {
        return switch (expr) {
            case And and -> QueryBuilders.boolQuery()
                .must(build(and.left(), overrideLeafAnalyzers))
                .must(build(and.right(), overrideLeafAnalyzers));
            case Or or -> QueryBuilders.boolQuery()
                .should(build(or.left(), overrideLeafAnalyzers))
                .should(build(or.right(), overrideLeafAnalyzers));
            case Not not -> QueryBuilders.boolQuery().mustNot(build(not.field(), overrideLeafAnalyzers));
            case Match match -> withoutLeafAnalyzer(match.asLexicalQueryBuilder(fieldName(match.field())), overrideLeafAnalyzers);
            case MatchPhrase matchPhrase -> withoutLeafAnalyzer(
                matchPhrase.asLexicalQueryBuilder(fieldName(matchPhrase.field())),
                overrideLeafAnalyzers
            );
            case QueryString queryString -> withoutLeafAnalyzer(pushdownQueryBuilder(queryString), overrideLeafAnalyzers);
            case Kql kql -> pushdownQueryBuilder(kql);
            default -> throw new IllegalStateException("Unexpected expression [" + expr.sourceText() + "] in HIGHLIGHT");
        };
    }

    /**
     * Clears the leaf {@code analyzer} option when WITH sets one, so WITH tokenizes both the query and the field.
     * Leaves {@code quote_analyzer} in place. WITH does not replace it.
     */
    private static QueryBuilder withoutLeafAnalyzer(QueryBuilder builder, boolean overrideLeafAnalyzers) {
        if (overrideLeafAnalyzers == false) {
            return builder;
        }
        return switch (builder) {
            case MatchQueryBuilder match -> match.analyzer(null);
            case MatchPhraseQueryBuilder phrase -> phrase.analyzer(null);
            case QueryStringQueryBuilder qstr -> qstr.analyzer(null);
            // MATCH, MATCH_PHRASE, and QSTR produce only the cases above. Throw so a new builder type cannot
            // keep a leaf analyzer when WITH set one.
            default -> throw new IllegalStateException("Unexpected query builder [" + builder.getName() + "] in HIGHLIGHT");
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

    /**
     * Per-field context plus the extra analyzers the builders still resolve by name. A {@code null} registry (unit
     * tests) registers none, which makes a named option fail translation with the builder's own message.
     */
    private static RuntimeSearchExecutionContext runtimeContext(
        Expression queryExpr,
        Map<String, NamedAnalyzer> fieldAnalyzers,
        boolean overrideLeafAnalyzers,
        boolean lenientFields,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        Map<String, NamedAnalyzer> extraAnalyzers = new LinkedHashMap<>();
        if (analysisRegistry != null) {
            for (String name : HighlightSupport.analyzerNamesOf(queryExpr, overrideLeafAnalyzers == false)) {
                extraAnalyzers.put(name, PlannerUtils.resolveAnalyzer(name, analysisRegistry));
            }
        }
        return RuntimeSearchExecutionContext.create(fieldAnalyzers, extraAnalyzers, lenientFields);
    }

    /**
     * Checks that the HIGHLIGHT query is a supported full-text form and translates with the same per-field
     * analyzers execution will use. When {@code enforceOnFields} is true, every named field must be in
     * {@code fieldAnalyzers}. An implicit query may name fields outside ON. Those fields become match-none,
     * and errors are prefixed as derived from WHERE. A non-null {@code commandAnalyzerName} strips leaf
     * {@code analyzer} options.
     */
    public static void verify(
        Expression queryExpr,
        Map<String, NamedAnalyzer> fieldAnalyzers,
        @Nullable String commandAnalyzerName,
        boolean enforceOnFields,
        boolean implicit,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        String literal = queryTextIfLiteral(queryExpr);
        if (literal == null) {
            verifyQueryStructure(queryExpr, enforceOnFields ? List.copyOf(fieldAnalyzers.keySet()) : null);
        }
        try {
            translateResolved(queryExpr, fieldAnalyzers, commandAnalyzerName != null, implicit, analysisRegistry);
        } catch (RuntimeException e) {
            String prefix = implicit
                ? "Invalid query derived from WHERE for HIGHLIGHT: "
                : "Invalid query [" + (literal != null ? literal : queryExpr.sourceText()) + "] in HIGHLIGHT: ";
            throw new IllegalArgumentException(prefix + e.getMessage(), e);
        }
    }

    /**
     * Builds the runtime query against a per-field analyzer context. Fields outside {@code fieldAnalyzers}
     * become match-none. Verification already rejected an explicit query that named such a field.
     * A non-null {@code commandAnalyzerName} strips leaf {@code analyzer} options so WITH tokenizes both sides.
     */
    public static TranslatedQuery translate(
        Expression queryExpr,
        Map<String, NamedAnalyzer> fieldAnalyzers,
        @Nullable String commandAnalyzerName,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        return translateResolved(queryExpr, fieldAnalyzers, commandAnalyzerName != null, true, analysisRegistry);
    }

    private static TranslatedQuery translateResolved(
        Expression queryExpr,
        Map<String, NamedAnalyzer> fieldAnalyzers,
        boolean overrideLeafAnalyzers,
        boolean lenient,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        List<String> fieldNames = List.copyOf(fieldAnalyzers.keySet());
        RuntimeSearchExecutionContext context = runtimeContext(queryExpr, fieldAnalyzers, overrideLeafAnalyzers, lenient, analysisRegistry);
        String literal = queryTextIfLiteral(queryExpr);
        String queryText = literal != null ? literal : queryExpr.sourceText();
        Query query = toLuceneQuery(toQueryBuilder(queryExpr, fieldNames, overrideLeafAnalyzers), context);
        return new TranslatedQuery(queryText, query);
    }

    /** Runtime query state produced by {@link #translate}. */
    public record TranslatedQuery(String queryText, Query query) {}
}
