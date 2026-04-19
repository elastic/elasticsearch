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
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.core.util.Queries;
import org.elasticsearch.xpack.esql.datasources.FilterEvaluationOrderEstimator;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
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
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
            plan = planFilterExecForExternalSource(filterExec, externalExec, ctx);
        } else if (filterExec.child() instanceof EvalExec evalExec && evalExec.child() instanceof ParameterizedQueryExec pqExec) {
            plan = planFilterExec(filterExec, evalExec, pqExec, ctx);
        } else if (filterExec.child() instanceof ParameterizedQueryExec pqExec) {
            plan = planFilterExec(filterExec, pqExec, ctx);
        }
        return plan;
    }

    private static PhysicalPlan planFilterExec(FilterExec filterExec, EsQueryExec queryExec, LocalPhysicalOptimizerContext ctx) {
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
        PushdownClassification classified = classifyFilters(filterExec.condition(), pushdownPredicates);
        return rewrite(pushdownPredicates, filterExec, queryExec, classified.pushable, classified.nonPushable, List.of());
    }

    private static PhysicalPlan planFilterExec(
        FilterExec filterExec,
        EvalExec evalExec,
        EsQueryExec queryExec,
        LocalPhysicalOptimizerContext ctx
    ) {
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
        AttributeMap<Attribute> aliasReplacedBy = getAliasReplacedBy(evalExec);
        PushdownClassification classified = classifyFilters(filterExec.condition(), pushdownPredicates, aliasReplacedBy);
        classified.pushable.replaceAll(e -> e.transformDown(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r)));
        return rewrite(pushdownPredicates, filterExec, queryExec, classified.pushable, classified.nonPushable, evalExec.fields());
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

    static AttributeMap<Attribute> getAliasReplacedBy(ProjectExec projectExec) {
        AttributeMap.Builder<Attribute> aliasReplacedByBuilder = AttributeMap.builder();
        for (NamedExpression ne : projectExec.projections()) {
            if (ne instanceof Alias alias && alias.child() instanceof Attribute attr) {
                aliasReplacedByBuilder.put(alias.toAttribute(), attr);
            }
        }
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
     * Resolves pushdown support via {@link FormatReader#filterPushdownSupport()} through
     * the {@link FormatReaderRegistry}. The pushdown support converts ESQL expressions to
     * source-specific filters (e.g., ORC SearchArgument, Parquet FilterPredicate).
     * <p>
     * The pushed filter is stored as an opaque Object in {@link ExternalSourceExec#pushedFilter()}.
     * It is built locally during physical plan optimization and consumed by the operator factory
     * in the same JVM — it is never part of the wire protocol.
     *
     * @param filterExec the filter execution node
     * @param externalExec the external source execution node
     * @param ctx the optimizer context (provides registries for pushdown lookup)
     * @return the optimized plan
     */
    private static PhysicalPlan planFilterExecForExternalSource(
        FilterExec filterExec,
        ExternalSourceExec externalExec,
        LocalPhysicalOptimizerContext ctx
    ) {
        // If the external source already has a pushed filter, don't push again.
        // With RECHECK semantics the FilterExec remains in the plan for row-level
        // correctness, so the rule would see the same FilterExec -> ExternalSourceExec
        // pattern on every iteration. Without this guard, the optimizer loops until
        // the rule execution limit is reached.
        if (externalExec.pushedFilter() != null) {
            return filterExec;
        }

        String formatName = resolveFormatName(externalExec.config(), externalExec.sourcePath());
        FilterPushdownSupport pushdownSupport = resolveFilterPushdownSupport(formatName, ctx);
        if (pushdownSupport == null) {
            return filterExec;
        }

        // Split filter condition by AND and reorder by estimated selectivity
        List<Expression> filters = splitAnd(filterExec.condition());
        Map<String, Object> effectiveMetadata = SourceStatisticsSerializer.resolveEffectiveMetadata(
            externalExec.splits(),
            externalExec.sourceMetadata()
        );
        filters = FilterEvaluationOrderEstimator.orderByEstimatedCost(filters, effectiveMetadata);

        // Use the SPI to push filters
        FilterPushdownSupport.PushdownResult result = pushdownSupport.pushFilters(filters);

        if (result.hasPushedFilter()) {
            // Create new ExternalSourceExec with pushed filter and the original ESQL expressions
            ExternalSourceExec newExternalExec = externalExec.withPushedFilterAndExpressions(
                result.pushedFilter(),
                result.pushedExpressions()
            );

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

    /**
     * Resolves the format name for file-based external sources, used for format-aware
     * pushdown dispatch. Checks the config map first (explicit format override from WITH clause),
     * then falls back to extracting the extension from the source path.
     *
     * @return the format name (e.g., "orc", "parquet", "csv"), or null if undetermined
     */
    static String resolveFormatName(Map<String, Object> config, String sourcePath) {
        // Priority 1: explicit format override from config (WITH clause)
        if (config != null) {
            Object formatOverride = config.get("format");
            if (formatOverride != null) {
                String name = formatOverride.toString().toLowerCase(Locale.ROOT);
                if (name.isEmpty() == false) {
                    return name;
                }
            }
        }
        // Priority 2: extract from file extension
        if (sourcePath != null) {
            int lastDot = sourcePath.lastIndexOf('.');
            if (lastDot >= 0 && lastDot < sourcePath.length() - 1) {
                String ext = sourcePath.substring(lastDot + 1);
                // Strip query string or fragment if present (e.g., s3://bucket/file.orc?versionId=... or #frag)
                int queryStart = ext.indexOf('?');
                if (queryStart >= 0) {
                    ext = ext.substring(0, queryStart);
                }
                int fragmentStart = ext.indexOf('#');
                if (fragmentStart >= 0) {
                    ext = ext.substring(0, fragmentStart);
                }
                if (ext.isEmpty() == false) {
                    return ext.toLowerCase(Locale.ROOT);
                }
            }
        }
        return null;
    }

    /**
     * Resolves filter pushdown support for the given format via {@link FormatReader#filterPushdownSupport()}.
     */
    private static FilterPushdownSupport resolveFilterPushdownSupport(String formatName, LocalPhysicalOptimizerContext ctx) {
        FormatReaderRegistry formatReaderRegistry = ctx.formatReaderRegistry();
        if (formatReaderRegistry == null) {
            return null;
        }
        FormatReader formatReader = formatReaderRegistry.findByName(formatName);
        return formatReader != null ? formatReader.filterPushdownSupport() : null;
    }

    private static PhysicalPlan planFilterExec(FilterExec filterExec, ParameterizedQueryExec pqExec, LocalPhysicalOptimizerContext ctx) {
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
        PushdownClassification classified = classifyFilters(filterExec.condition(), pushdownPredicates);
        return rewrite(pushdownPredicates, filterExec, pqExec, classified.pushable, classified.nonPushable, List.of());
    }

    private static PhysicalPlan planFilterExec(
        FilterExec filterExec,
        EvalExec evalExec,
        ParameterizedQueryExec pqExec,
        LocalPhysicalOptimizerContext ctx
    ) {
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), ctx.flags());
        AttributeMap<Attribute> aliasReplacedBy = getAliasReplacedBy(evalExec);
        PushdownClassification classified = classifyFilters(filterExec.condition(), pushdownPredicates, aliasReplacedBy);
        classified.pushable.replaceAll(e -> e.transformDown(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r)));
        return rewrite(pushdownPredicates, filterExec, pqExec, classified.pushable, classified.nonPushable, evalExec.fields());
    }

    private static PhysicalPlan rewrite(
        LucenePushdownPredicates pushdownPredicates,
        FilterExec filterExec,
        ParameterizedQueryExec pqExec,
        List<Expression> pushable,
        List<Expression> nonPushable,
        List<Alias> evalFields
    ) {
        // Combine GT, GTE, LT and LTE in pushable to Range if possible
        List<Expression> newPushable = combineEligiblePushableToRange(pushable);
        if (newPushable.size() > 0) { // update the executable with pushable conditions
            Query queryDSL = TRANSLATOR_HANDLER.asQuery(pushdownPredicates, Predicates.combineAnd(newPushable));
            QueryBuilder planQuery = queryDSL.toQueryBuilder();
            QueryBuilder query = Queries.combine(Queries.Clause.FILTER, asList(pqExec.query(), planQuery));
            PhysicalPlan plan = pqExec.withQuery(query);
            plan = evalFields.isEmpty() ? plan : new EvalExec(filterExec.source(), plan, evalFields);
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

    private record PushdownClassification(List<Expression> pushable, List<Expression> nonPushable) {}

    private static PushdownClassification classifyFilters(Expression condition, LucenePushdownPredicates pushdownPredicates) {
        return classifyFilters(condition, pushdownPredicates, AttributeMap.emptyAttributeMap());
    }

    /**
     * Classifies filter expressions into pushable and non-pushable lists.
     * When {@code aliasReplacedBy} is non-empty, alias resolution is applied before
     * the translatability check, but the original (unresolved) expressions are stored
     * so that non-pushable filters still reference the EvalExec output attributes.
     */
    private static PushdownClassification classifyFilters(
        Expression condition,
        LucenePushdownPredicates pushdownPredicates,
        AttributeMap<Attribute> aliasReplacedBy
    ) {
        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : splitAnd(condition)) {
            Expression resExp = aliasReplacedBy.isEmpty()
                ? exp
                : exp.transformUp(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));
            switch (translatable(resExp, pushdownPredicates).finish()) {
                case NO -> nonPushable.add(exp);
                case YES -> pushable.add(exp);
                case RECHECK -> {
                    pushable.add(exp);
                    nonPushable.add(exp);
                }
            }
        }
        return new PushdownClassification(pushable, nonPushable);
    }
}
