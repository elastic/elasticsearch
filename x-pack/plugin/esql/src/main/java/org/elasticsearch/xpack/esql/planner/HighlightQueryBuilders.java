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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.querydsl.query.QueryStringQuery;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Kql;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchPhrase;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryString;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
    private static void verifyQueryStructure(Expression expr, List<String> onFields, boolean enforceOnFields) {
        // TODO: Allow HIGHLIGHT queries to use expressions other than full-text functions.
        switch (expr) {
            case Match match -> {
                if (enforceOnFields) {
                    requireOnField(fieldName(match.field()), onFields);
                }
            }
            case MatchPhrase matchPhrase -> {
                if (enforceOnFields) {
                    requireOnField(fieldName(matchPhrase.field()), onFields);
                }
            }
            case QueryString queryString -> {
                String defaultField = queryStringDefaultField(queryString);
                if (defaultField != null && enforceOnFields) {
                    requireOnField(defaultField, onFields);
                }
            }
            case And and -> {
                verifyQueryStructure(and.left(), onFields, enforceOnFields);
                verifyQueryStructure(and.right(), onFields, enforceOnFields);
            }
            case Or or -> {
                verifyQueryStructure(or.left(), onFields, enforceOnFields);
                verifyQueryStructure(or.right(), onFields, enforceOnFields);
            }
            case Not not -> verifyQueryStructure(not.field(), onFields, enforceOnFields);
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

    /** Returns whether {@code expr} can contribute to an implicit HIGHLIGHT query. */
    public static boolean isSupportedImplicitPredicate(Expression expr) {
        return switch (expr) {
            case Match match -> true;
            case MatchPhrase matchPhrase -> true;
            case QueryString queryString -> true;
            case Kql kql -> true;
            case And and -> isSupportedImplicitPredicate(and.left()) && isSupportedImplicitPredicate(and.right());
            case Or or -> isSupportedImplicitPredicate(or.left()) && isSupportedImplicitPredicate(or.right());
            default -> false;
        };
    }

    /**
     * Verifies that a HIGHLIGHT query uses supported full-text forms, references its {@code onFields}, and translates
     * with the {@code analyzer} that execution will use.
     */
    public static void verify(Expression queryExpr, List<String> onFields, Analyzer analyzer, boolean enforceOnFields) {
        String literal = queryTextIfLiteral(queryExpr);
        // Pushdown accepts more expressions than the runtime context, so check the query shape first.
        if (literal == null) {
            verifyQueryStructure(queryExpr, onFields, enforceOnFields);
        }
        // Translate before planning to catch invalid options, syntax, and fields outside ON.
        try {
            translate(queryExpr, onFields, runtimeContext(onFields, analyzer));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "Invalid query [" + (literal != null ? literal : queryExpr.sourceText()) + "] in HIGHLIGHT: " + e.getMessage(),
                e
            );
        }
    }

    /** Returns all non-metadata string fields, using the last field when names collide. */
    public static List<NamedExpression> allHighlightableFields(List<Attribute> childrenOutput) {
        Map<String, NamedExpression> byName = new LinkedHashMap<>();
        for (Attribute attr : childrenOutput) {
            if (DataType.isString(attr.dataType()) && attr instanceof MetadataAttribute == false) {
                byName.remove(attr.name());
                byName.put(attr.name(), attr);
            }
        }
        return List.copyOf(byName.values());
    }

    /** Returns the query's string fields, or all string fields when the query does not name a concrete field. */
    public static List<NamedExpression> deriveFields(Expression query, List<Attribute> childrenOutput) {
        Set<String> names = new LinkedHashSet<>();
        boolean fieldsKnown = collectQueryFieldNames(query, names);
        if (fieldsKnown == false) {
            return allHighlightableFields(childrenOutput);
        }
        List<NamedExpression> result = new ArrayList<>(names.size());
        for (String name : names) {
            for (Attribute attr : childrenOutput) {
                if (attr.name().equals(name) && DataType.isString(attr.dataType())) {
                    result.add(attr);
                    break;
                }
            }
        }
        return result;
    }

    private static boolean collectQueryFieldNames(Expression query, Set<String> names) {
        return switch (query) {
            case Match match -> {
                names.add(Expressions.name(match.field()));
                yield true;
            }
            case MatchPhrase matchPhrase -> {
                names.add(Expressions.name(matchPhrase.field()));
                yield true;
            }
            case QueryString queryString -> {
                String defaultField = queryStringDefaultField(queryString);
                if (defaultField == null || Regex.isSimpleMatchPattern(defaultField)) {
                    yield false;
                }
                names.add(defaultField);
                yield true;
            }
            case Kql kql -> false;
            case Literal literal -> false;
            case And and -> {
                boolean left = collectQueryFieldNames(and.left(), names);
                boolean right = collectQueryFieldNames(and.right(), names);
                yield left && right;
            }
            case Or or -> {
                boolean left = collectQueryFieldNames(or.left(), names);
                boolean right = collectQueryFieldNames(or.right(), names);
                yield left && right;
            }
            case Not not -> true;
            default -> false;
        };
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

    private static RuntimeSearchExecutionContext runtimeContext(List<String> fieldNames, Analyzer analyzer) {
        NamedAnalyzer namedAnalyzer = analyzer instanceof NamedAnalyzer na
            ? na
            : new NamedAnalyzer("_override", AnalyzerScope.GLOBAL, analyzer);
        return RuntimeSearchExecutionContext.create(fieldNames, namedAnalyzer);
    }

    private static TranslatedQuery translate(Expression queryExpr, List<String> fieldNames, RuntimeSearchExecutionContext context) {
        String literal = queryTextIfLiteral(queryExpr);
        String queryText = literal != null ? literal : queryExpr.sourceText();
        Query query = toLuceneQuery(toQueryBuilder(queryExpr, fieldNames), context);
        return new TranslatedQuery(queryText, query, context.searchAnalyzer());
    }

    /**
     * Builds the runtime query with the analyzer used to index each row's text.
     */
    private static TranslatedQuery translate(Expression queryExpr, List<String> fieldNames, Analyzer analyzer) {
        return translate(queryExpr, fieldNames, runtimeContext(fieldNames, analyzer));
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
        return translate(queryExpr, fieldNames, PlannerUtils.resolveAnalyzer(name, analysisRegistry));
    }

    /** Runtime query state produced by {@link #translate}. */
    public record TranslatedQuery(String queryText, Query query, Analyzer analyzer) {}
}
