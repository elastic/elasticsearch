/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware.FinishedTranslatable;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware.Translatable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.querydsl.query.MatchAll;
import org.elasticsearch.xpack.esql.core.querydsl.query.NestedQuery;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedPredicate;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * Runs {@code WHERE NESTED_ANY(<predicate>)} in Lucene as a {@code nested} query, so only parents with a matching child document
 * leave the index. Every shard must accept nested queries, which {@code search.allow_expensive_queries: false} refuses. Each shard
 * mapping must allow Lucene to compare the child values as their block loaders read them, including
 * having searchable doc values and no keyword normalizer that changes query literals. An exact translation replaces the
 * predicate; one that needs a recheck is pushed and still evaluated; anything else stays in memory. A {@code true} predicate, as
 * in {@code NESTED_ANY(items, true)}, matches any child document. {@code NOT NESTED_ANY(<predicate>)} is pushed as
 * {@code NOT nested(<predicate>)} only when the translation is exact: negating a superset would drop parents that match.
 */
public final class PushNestedPredicateToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    FilterExec,
    LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(FilterExec filter, LocalPhysicalOptimizerContext context) {
        if (scan(filter.child()) == null) {
            return filter;
        }
        LucenePushdownPredicates children = childFields(context.flags());
        List<QueryBuilder> queries = new ArrayList<>();
        List<Expression> remaining = new ArrayList<>();
        for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
            boolean negated = conjunct instanceof Not;
            Expression candidate = negated ? ((Not) conjunct).field() : conjunct;
            Pushed pushed = candidate instanceof NestedPredicate predicate
                ? push(predicate, negated, children, context.searchStats())
                : null;
            if (pushed != null) {
                queries.add(pushed.query);
            }
            if (pushed == null || pushed.exact == false) {
                remaining.add(conjunct);
            }
        }
        if (queries.isEmpty()) {
            return filter;
        }
        PhysicalPlan child = withQuery(filter.child(), queries);
        return remaining.isEmpty() ? child : new FilterExec(filter.source(), child, Predicates.combineAnd(remaining));
    }

    record Pushed(QueryBuilder query, boolean exact) {}

    static Pushed push(NestedPredicate predicate, boolean negated, LucenePushdownPredicates children, SearchStats stats) {
        Translation translation = translate(predicate, children, stats);
        if (translation == null) {
            return null;
        }
        if (negated && translation.exact() == false) {
            return null;
        }
        return new Pushed(
            negated ? QueryBuilders.boolQuery().mustNot(translation.query()) : translation.query(),
            negated || translation.translatable().finish() == FinishedTranslatable.YES
        );
    }

    /**
     * The nested query for {@code predicate}, and how well it translates, or {@code null} when it cannot be a query on every
     * shard here. {@link Translation#exact} says the query matches exactly the parents the predicate does.
     */
    static Translation translate(NestedPredicate predicate, LucenePushdownPredicates children, SearchStats stats) {
        if (stats.nestedQueriesAllowed() == false) {
            return null;
        }
        Expression childPredicate = childPredicate(predicate, stats);
        if (childPredicate == null) {
            return null;
        }
        boolean matchAll = Literal.TRUE.equals(childPredicate);
        Translatable translatable = matchAll ? Translatable.YES : translatable(childPredicate, children);
        if (translatable.finish() == FinishedTranslatable.NO) {
            return null;
        }
        Query child = matchAll ? new MatchAll(predicate.source()) : TRANSLATOR_HANDLER.asQuery(children, childPredicate);
        QueryBuilder nested = new NestedQuery(predicate.source(), predicate.path(), child, ScoreMode.None).toQueryBuilder();
        return new Translation(nested, translatable);
    }

    /**
     * Keeps an exact conjunct as a query over child documents and leaves every other conjunct to be evaluated.
     * A negated, approximate, or unqueryable conjunct stays in the residual. When nothing can be separated, or the
     * whole predicate is the exact part, {@code predicate} is returned unchanged: the exact whole is a nested query.
     */
    static NestedPredicate exactChildFilter(NestedPredicate predicate, LucenePushdownPredicates children, SearchStats stats) {
        if (stats.nestedQueriesAllowed() == false) {
            return predicate;
        }
        Expression rewritten = predicate.childPredicate();
        if (rewritten == null) {
            return predicate;
        }
        List<Expression> original = Predicates.splitAnd(predicate.body());
        List<Expression> translated = Predicates.splitAnd(rewritten);
        if (original.size() != translated.size()) {
            return predicate;
        }
        List<QueryBuilder> filters = new ArrayList<>();
        List<Expression> residual = new ArrayList<>();
        for (int i = 0; i < original.size(); i++) {
            Expression conjunct = translated.get(i);
            if (exactChildConjunct(conjunct, children, stats)) {
                filters.add(TRANSLATOR_HANDLER.asQuery(children, conjunct).toQueryBuilder());
            } else {
                residual.add(original.get(i));
            }
        }
        if (filters.isEmpty() || residual.isEmpty()) {
            return predicate;
        }
        return predicate.withChildFilter(must(filters), Predicates.combineAnd(residual));
    }

    private static boolean exactChildConjunct(Expression conjunct, LucenePushdownPredicates children, SearchStats stats) {
        if (conjunct instanceof Not) {
            return false;
        }
        AttributeSet referenced = conjunct.references();
        if (referenced.isEmpty()) {
            return false;
        }
        for (Attribute attribute : referenced) {
            if (attribute instanceof FieldAttribute field) {
                if (stats.nestedFieldIsQueryable(field.fieldName(), field.dataType()) == false) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return translatable(conjunct, children) == Translatable.YES;
    }

    private static QueryBuilder must(List<QueryBuilder> filters) {
        if (filters.size() == 1) {
            return filters.getFirst();
        }
        var bool = QueryBuilders.boolQuery();
        for (QueryBuilder filter : filters) {
            bool.must(filter);
        }
        return bool;
    }

    record Translation(QueryBuilder query, Translatable translatable) {
        boolean exact() {
            return translatable == Translatable.YES || translatable == Translatable.YES_BUT_RECHECK_NEGATED;
        }
    }

    /** The scan below row-preserving steps, if a filter there keeps the same rows; not once a limit is pushed into it. */
    static EsQueryExec scan(PhysicalPlan plan) {
        while (plan instanceof ProjectExec || plan instanceof EvalExec) {
            plan = ((UnaryExec) plan).child();
        }
        return plan instanceof EsQueryExec query && query.limit() == null ? query : null;
    }

    private static Expression childPredicate(NestedPredicate predicate, SearchStats stats) {
        List<String> names = predicate.fieldNames();
        for (int i = 0; i < names.size(); i++) {
            if (stats.nestedFieldIsQueryable(new FieldAttribute.FieldName(names.get(i)), predicate.inputs().get(i).dataType()) == false) {
                return null;
            }
        }
        return predicate.childPredicate();
    }

    static PhysicalPlan withQuery(PhysicalPlan plan, List<QueryBuilder> queries) {
        if (plan instanceof EsQueryExec scan) {
            List<QueryBuilder> all = new ArrayList<>();
            all.add(scan.query());
            all.addAll(queries);
            return scan.withQuery(Queries.combine(scan.hasScoring() ? Queries.Clause.MUST : Queries.Clause.FILTER, all));
        }
        UnaryExec unary = (UnaryExec) plan;
        return unary.replaceChild(withQuery(unary.child(), queries));
    }

    /** Child fields reach here only once every applicable shard can query their values as read. */
    static LucenePushdownPredicates childFields(EsqlFlags flags) {
        return new LucenePushdownPredicates() {
            @Override
            public TransportVersion minTransportVersion() {
                return null;
            }

            @Override
            public EsqlFlags flags() {
                return flags;
            }

            @Override
            public boolean hasExactSubfield(FieldAttribute attr) {
                return false;
            }

            @Override
            public boolean isQueryable(FieldAttribute attr) {
                return true;
            }

            @Override
            public boolean isIndexed(FieldAttribute attr) {
                return true;
            }

            @Override
            public boolean canUseEqualityOnSyntheticSourceDelegate(FieldAttribute attr, String value) {
                return false;
            }

            @Override
            public boolean supportsLoaderConfig(
                FieldAttribute field,
                BlockLoaderFunctionConfig config,
                MappedFieldType.FieldExtractPreference preference
            ) {
                return false;
            }

            @Override
            public boolean hasValueQueries(FieldAttribute attr) {
                return false;
            }
        };
    }
}
