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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerScope;
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

    /**
     * Checks supported full-text forms, ON-field references when {@code enforceOnFields} is true, and translation
     * with the analyzer execution will use. Implicit queries (from WHERE) use a lenient context so non-ON fields
     * become match-none, and failures are framed as derived from WHERE.
     */
    public static void verify(Expression queryExpr, List<String> fields, Analyzer analyzer, boolean enforceOnFields, boolean implicit) {
        String literal = queryTextIfLiteral(queryExpr);
        if (literal == null) {
            verifyQueryStructure(queryExpr, enforceOnFields ? fields : null);
        }
        try {
            translate(queryExpr, fields, runtimeContext(fields, analyzer, implicit));
        } catch (RuntimeException e) {
            String prefix = implicit
                ? "Invalid query derived from WHERE for HIGHLIGHT: "
                : "Invalid query [" + (literal != null ? literal : queryExpr.sourceText()) + "] in HIGHLIGHT: ";
            throw new IllegalArgumentException(prefix + e.getMessage(), e);
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

    // Keep in sync with verifyQueryStructure and HighlightSupport#isSupportedImplicitPredicate/deriveFields.
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

    private static RuntimeSearchExecutionContext runtimeContext(List<String> fieldNames, Analyzer analyzer, boolean lenientFields) {
        NamedAnalyzer namedAnalyzer = analyzer instanceof NamedAnalyzer na
            ? na
            : new NamedAnalyzer("_override", AnalyzerScope.GLOBAL, analyzer);
        return RuntimeSearchExecutionContext.create(fieldNames, namedAnalyzer, lenientFields);
    }

    private static TranslatedQuery translate(Expression queryExpr, List<String> fieldNames, RuntimeSearchExecutionContext context) {
        String literal = queryTextIfLiteral(queryExpr);
        String queryText = literal != null ? literal : queryExpr.sourceText();
        Query query = toLuceneQuery(toQueryBuilder(queryExpr, fieldNames), context);
        return new TranslatedQuery(queryText, query, context.searchAnalyzer());
    }

    /**
     * Builds the runtime query with the analyzer used to index each row's text.
     * Always lenient about fields outside {@code fieldNames}: verification already rejected those on explicit queries.
     */
    private static TranslatedQuery translate(Expression queryExpr, List<String> fieldNames, Analyzer analyzer) {
        return translate(queryExpr, fieldNames, runtimeContext(fieldNames, analyzer, true));
    }

    /**
     * Resolves {@code analyzerName} from {@code analysisRegistry}, then builds the runtime query. A {@code null} name
     * selects the {@link #DEFAULT_ANALYZER_NAME default} analyzer.
     */
    public static TranslatedQuery translate(
        Expression queryExpr,
        List<String> fieldNames,
        @Nullable String analyzerName,
        @Nullable AnalysisRegistry analysisRegistry
    ) {
        String name = analyzerName != null ? analyzerName : DEFAULT_ANALYZER_NAME;
        Analyzer analyzer = PlannerUtils.resolveAnalyzer(name, analysisRegistry);
        return translate(queryExpr, fieldNames, runtimeContext(fieldNames, analyzer, true));
    }

    /** Runtime query state produced by {@link #translate}. */
    public record TranslatedQuery(String queryText, Query query, Analyzer analyzer) {}
}
