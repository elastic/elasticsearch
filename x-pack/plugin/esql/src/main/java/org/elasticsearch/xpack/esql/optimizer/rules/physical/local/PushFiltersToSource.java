/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.datasources.FilterPushdownRegistry;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.capabilities.TranslationAware.translatable;
import static org.elasticsearch.xpack.esql.expression.predicate.Predicates.splitAnd;
import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

public class PushFiltersToSource extends PhysicalOptimizerRules.ParameterizedOptimizerRule<FilterExec, LocalPhysicalOptimizerContext> {

    @Override
    protected PhysicalPlan rule(FilterExec filterExec, LocalPhysicalOptimizerContext ctx) {
        PhysicalPlan plan = filterExec;
        if (filterExec.child() instanceof EsQueryExec queryExec) {
            plan = planFilterExec(filterExec, queryExec, ctx);
        } else if (filterExec.child() instanceof EvalExec evalExec && evalExec.child() instanceof EsQueryExec queryExec) {
            plan = planFilterExec(filterExec, evalExec, queryExec, ctx);
        } else if (filterExec.child() instanceof ExternalSourceExec externalExec) {
            plan = planFilterExecForExternalSource(filterExec, externalExec, ctx.filterPushdownRegistry());
        }
        return plan;
    }

    private static PhysicalPlan planFilterExec(FilterExec filterExec, EsQueryExec queryExec, LocalPhysicalOptimizerContext ctx) {
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : splitAnd(filterExec.condition())) {
            switch (translatable(exp, pushdownPredicates).finish()) {
                case NO -> nonPushable.add(exp);
                case YES -> pushable.add(exp);
                case RECHECK -> {
                    pushable.add(exp);
                    nonPushable.add(exp);
                }
            }
        }
        return rewrite(pushdownPredicates, filterExec, queryExec, pushable, nonPushable, List.of());
    }

    private static PhysicalPlan planFilterExec(
        FilterExec filterExec,
        EvalExec evalExec,
        EsQueryExec queryExec,
        LocalPhysicalOptimizerContext ctx
    ) {
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
        AttributeMap<Attribute> aliasReplacedBy = getAliasReplacedBy(evalExec);
        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : splitAnd(filterExec.condition())) {
            Expression resExp = exp.transformUp(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));
            switch (translatable(resExp, pushdownPredicates).finish()) {
                case NO -> nonPushable.add(exp);
                case YES -> pushable.add(exp);
                case RECHECK -> {
                    nonPushable.add(exp);
                    nonPushable.add(exp);
                }
            }
        }
        // Replace field references with their actual field attributes
        pushable.replaceAll(e -> e.transformDown(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r)));
        return rewrite(pushdownPredicates, filterExec, queryExec, pushable, nonPushable, evalExec.fields());
    }

    static AttributeMap<Attribute> getAliasReplacedBy(EvalExec evalExec) {
        AttributeMap.Builder<Attribute> aliasReplacedByBuilder = AttributeMap.builder();
        evalExec.fields().forEach(alias -> {
            if (alias.child() instanceof Attribute attr) {
                aliasReplacedByBuilder.put(alias.toAttribute(), attr);
            }
        });
        return aliasReplacedByBuilder.build();
    }

    private static PhysicalPlan rewrite(
        LucenePushdownPredicates pushdownPredicates,
        FilterExec filterExec,
        EsQueryExec queryExec,
        List<Expression> pushable,
        List<Expression> nonPushable,
        List<Alias> evalFields
    ) {
        // Combine GT, GTE, LT and LTE in pushable to Range if possible
        List<Expression> newPushable = combineEligiblePushableToRange(pushable);
        if (newPushable.size() > 0) { // update the executable with pushable conditions
            Query queryDSL = TRANSLATOR_HANDLER.asQuery(pushdownPredicates, Predicates.combineAnd(newPushable));
            QueryBuilder planQuery = queryDSL.toQueryBuilder();
            Queries.Clause combiningQueryClauseType = queryExec.hasScoring() ? Queries.Clause.MUST : Queries.Clause.FILTER;
            var query = Queries.combine(combiningQueryClauseType, asList(queryExec.query(), planQuery));
            queryExec = new EsQueryExec(
                queryExec.source(),
                queryExec.indexPattern(),
                queryExec.indexMode(),
                queryExec.output(),
                queryExec.limit(),
                queryExec.sorts(),
                queryExec.estimatedRowSize(),
                List.of(new EsQueryExec.QueryBuilderAndTags(query, List.of()))
            );
            // If the eval contains other aliases, not just field attributes, we need to keep them in the plan
            PhysicalPlan plan = evalFields.isEmpty() ? queryExec : new EvalExec(filterExec.source(), queryExec, evalFields);
            if (nonPushable.size() > 0) {
                // update filter with remaining non-pushable conditions
                return new FilterExec(filterExec.source(), plan, Predicates.combineAnd(nonPushable));
            } else {
                // prune Filter entirely
                return plan;
            }
        } // else: nothing changes
        return filterExec;
    }

    private static List<Expression> combineEligiblePushableToRange(List<Expression> pushable) {
        List<EsqlBinaryComparison> bcs = new ArrayList<>();
        List<Range> ranges = new ArrayList<>();
        List<Expression> others = new ArrayList<>();
        boolean changed = false;

        pushable.forEach(e -> {
            if (e instanceof GreaterThan || e instanceof GreaterThanOrEqual || e instanceof LessThan || e instanceof LessThanOrEqual) {
                if (((EsqlBinaryComparison) e).right().foldable()) {
                    bcs.add((EsqlBinaryComparison) e);
                } else {
                    others.add(e);
                }
            } else {
                others.add(e);
            }
        });

        for (int i = 0, step = 1; i < bcs.size() - 1; i += step, step = 1) {
            BinaryComparison main = bcs.get(i);
            for (int j = i + 1; j < bcs.size(); j++) {
                BinaryComparison other = bcs.get(j);
                if (main.left().semanticEquals(other.left())) {
                    // >/>= AND </<=
                    if ((main instanceof GreaterThan || main instanceof GreaterThanOrEqual)
                        && (other instanceof LessThan || other instanceof LessThanOrEqual)) {
                        bcs.remove(j);
                        bcs.remove(i);

                        ranges.add(
                            new Range(
                                main.source(),
                                main.left(),
                                main.right(),
                                main instanceof GreaterThanOrEqual,
                                other.right(),
                                other instanceof LessThanOrEqual,
                                main.zoneId()
                            )
                        );

                        changed = true;
                        step = 0;
                        break;
                    }
                    // </<= AND >/>=
                    else if ((other instanceof GreaterThan || other instanceof GreaterThanOrEqual)
                        && (main instanceof LessThan || main instanceof LessThanOrEqual)) {
                            bcs.remove(j);
                            bcs.remove(i);

                            ranges.add(
                                new Range(
                                    main.source(),
                                    main.left(),
                                    other.right(),
                                    other instanceof GreaterThanOrEqual,
                                    main.right(),
                                    main instanceof LessThanOrEqual,
                                    main.zoneId()
                                )
                            );

                            changed = true;
                            step = 0;
                            break;
                        }
                }
            }
        }
        return changed ? CollectionUtils.combine(others, bcs, ranges) : pushable;
    }

    /**
     * Push filters to external source using the SPI-based FilterPushdownSupport.
     * <p>
     * This method uses the {@link FilterPushdownRegistry} to look up the appropriate
     * {@link FilterPushdownSupport} implementation for the source type. The pushdown
     * support converts ESQL expressions to source-specific filters (e.g., Iceberg expressions).
     * <p>
     * The pushed filter is stored as an opaque Object in {@link ExternalSourceExec#pushedFilter()}.
     * Since external sources execute on coordinator only ({@code ExecutesOn.Coordinator}),
     * the filter is never serialized - it's created during local optimization and consumed
     * immediately by the operator factory in the same JVM.
     *
     * @param filterExec the filter execution node
     * @param externalExec the external source execution node
     * @param registry the filter pushdown registry
     * @return the optimized plan
     */
    private static PhysicalPlan planFilterExecForExternalSource(
        FilterExec filterExec,
        ExternalSourceExec externalExec,
        FilterPushdownRegistry registry
    ) {
        // Look up pushdown support for this source type
        FilterPushdownSupport pushdownSupport = registry != null ? registry.get(externalExec.sourceType()) : null;
        if (pushdownSupport == null) {
            // No pushdown support registered for this source type
            return filterExec;
        }

        // Split filter condition by AND
        List<Expression> filters = splitAnd(filterExec.condition());

        // Use the SPI to push filters
        FilterPushdownSupport.PushdownResult result = pushdownSupport.pushFilters(filters);

        if (result.hasPushedFilter()) {
            // Combine with existing pushed filter if present
            Object combinedFilter = externalExec.pushedFilter();
            if (combinedFilter != null) {
                // The pushdown support should handle combining filters
                // For now, we create a new pushdown with all filters including existing
                // This is a simplification - in practice, the existing filter would be
                // combined by the source-specific implementation
                combinedFilter = result.pushedFilter();
            } else {
                combinedFilter = result.pushedFilter();
            }

            // Create new ExternalSourceExec with combined filter
            ExternalSourceExec newExternalExec = externalExec.withPushedFilter(combinedFilter);

            // If there are non-pushable filters, keep FilterExec
            if (result.hasRemainder()) {
                return new FilterExec(filterExec.source(), newExternalExec, Predicates.combineAnd(result.remainder()));
            } else {
                // All filters pushed down - remove FilterExec
                return newExternalExec;
            }
        }

        // No pushable filters - return original plan
        return filterExec;
    }
}
