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
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware.FinishedTranslatable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.expression.function.scalar.nested.NestedReduction;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedReduceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * Runs {@code WHERE NESTED_ANY(<predicate>)} in Lucene as a {@code nested} query, so only parents with a matching child document
 * leave the index. The in-memory check reads the child values from doc values, so a query on the indexed child fields gives
 * the same answer when every child field is searchable and has doc values here. A predicate that translates exactly replaces
 * the reduction; one that needs a recheck is pushed and still evaluated; anything else stays in memory. A {@code true}
 * predicate, as in {@code NESTED_ANY(items, true)}, matches any child document. {@code NESTED_ALL(<predicate>)} runs as
 * {@code NOT nested(NOT <predicate>)}: a child where the predicate is false or null excludes its parent, and a parent without
 * children matches.
 */
public final class PushNestedPredicateToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<
    FilterExec,
    LocalPhysicalOptimizerContext> {
    @Override
    protected PhysicalPlan rule(FilterExec filter, LocalPhysicalOptimizerContext context) {
        if (context.configuration().pragmas().nestedLoader() != QueryPragmas.NestedLoader.INDEX
            || filter.child() instanceof NestedReduceExec == false
            || scan(((NestedReduceExec) filter.child()).child()) == null) {
            return filter;
        }
        NestedReduceExec reduce = (NestedReduceExec) filter.child();
        Map<NameId, NestedReduction> reductions = new HashMap<>();
        for (Alias alias : reduce.fields()) {
            if (alias.child() instanceof NestedReduction reduction) {
                reductions.put(alias.id(), reduction);
            }
        }
        LucenePushdownPredicates children = childFields(context.flags());
        List<QueryBuilder> queries = new ArrayList<>();
        Set<NameId> replaced = new HashSet<>();
        List<Expression> remaining = new ArrayList<>();
        for (Expression conjunct : Predicates.splitAnd(filter.condition())) {
            NestedReduction reduction = conjunct instanceof ReferenceAttribute ref ? reductions.get(ref.id()) : null;
            Expression predicate = reduction == null ? null : childPredicate(reduction, context.searchStats());
            boolean anyChild = Literal.TRUE.equals(predicate);
            FinishedTranslatable translatable = predicate == null ? FinishedTranslatable.NO
                : anyChild ? FinishedTranslatable.YES
                : translatable(predicate, children).finish();
            if (translatable != FinishedTranslatable.NO) {
                QueryBuilder query = anyChild
                    ? QueryBuilders.matchAllQuery()
                    : TRANSLATOR_HANDLER.asQuery(children, predicate).toQueryBuilder();
                queries.add(
                    reduction.all()
                        ? QueryBuilders.boolQuery().mustNot(nested(reduction, QueryBuilders.boolQuery().mustNot(query)))
                        : nested(reduction, query)
                );
            }
            if (translatable == FinishedTranslatable.YES) {
                replaced.add(((ReferenceAttribute) conjunct).id());
            } else {
                remaining.add(conjunct);
            }
        }
        if (queries.isEmpty()) {
            return filter;
        }
        List<Alias> fields = reduce.fields()
            .stream()
            .map(alias -> replaced.contains(alias.id()) ? alias.replaceChild(Literal.TRUE) : alias)
            .toList();
        PhysicalPlan child = withQuery(reduce.child(), queries);
        PhysicalPlan eval = fields.stream().anyMatch(alias -> alias.child().anyMatch(NestedReduction.class::isInstance))
            ? new NestedReduceExec(reduce.source(), child, fields)
            : new EvalExec(reduce.source(), child, fields);
        return remaining.isEmpty() ? eval : new FilterExec(filter.source(), eval, Predicates.combineAnd(remaining));
    }

    private static QueryBuilder nested(NestedReduction reduction, QueryBuilder child) {
        return new NestedQueryBuilder(reduction.path(), child, ScoreMode.None).ignoreUnmapped(true);
    }

    /** The scan below row-preserving steps, if a filter there keeps the same rows; not once a limit is pushed into it. */
    static EsQueryExec scan(PhysicalPlan plan) {
        while (plan instanceof ProjectExec || plan instanceof EvalExec) {
            plan = ((UnaryExec) plan).child();
        }
        return plan instanceof EsQueryExec query && query.limit() == null ? query : null;
    }

    private static Expression childPredicate(NestedReduction reduction, SearchStats stats) {
        for (String name : reduction.fieldNames()) {
            var field = new FieldAttribute.FieldName(name);
            if (stats.nestedFieldIsSearchable(field) == false || stats.nestedFieldHasDocValues(field) == false) {
                return null;
            }
        }
        return reduction.childPredicate();
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

    /** Child fields reach here only once every shard can search them and read their doc values. */
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
