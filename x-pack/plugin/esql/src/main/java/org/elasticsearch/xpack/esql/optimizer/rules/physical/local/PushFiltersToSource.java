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
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.PartitionMetadata;
import org.elasticsearch.xpack.esql.datasources.PhysicalNames;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
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
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

        // Conjuncts that reference partition columns are evaluated against the constant blocks
        // injected by VirtualColumnIterator (and used as L1 pruning hints in FileSplitProvider).
        // Pushing them into the format reader would translate them against file column data,
        // but partition columns are not present in the file payload -- the reader would then
        // either drop all rows (parquet) or behave format-specifically. Keep these conjuncts in
        // the FilterExec so the post-injection evaluator handles them.
        Set<String> partitionColumnNames = partitionColumnNames(externalExec);
        List<Expression> partitionConjuncts = new ArrayList<>();
        List<Expression> pushableCandidates = new ArrayList<>();
        if (partitionColumnNames.isEmpty()) {
            pushableCandidates = filters;
        } else {
            for (Expression filter : filters) {
                if (referencesAnyColumn(filter, partitionColumnNames)) {
                    partitionConjuncts.add(filter);
                } else {
                    pushableCandidates.add(filter);
                }
            }
        }

        var effectiveStats = externalExec.effectiveSplitStats();
        pushableCandidates = FilterEvaluationOrderEstimator.orderByEstimatedCost(pushableCandidates, effectiveStats);

        // A declared `path` rename lives in logical space in the plan, but the opaque per-format predicate the SPI
        // mints must reference the file's PHYSICAL columns. Physicalize only the conjuncts handed to the mint; map the
        // returned pushed/remainder expressions back to logical (via inverse, NameId-preserving) so the plan's FilterExec
        // and reconciliation stay logical. No-op when the dataset declares no rename.
        Map<String, String> renames = externalExec.declaredReadSpec().renames();
        Map<String, String> toLogical = PhysicalNames.inverse(renames);
        List<Expression> mintInput = PhysicalNames.translateExpressionNames(pushableCandidates, renames);
        // Invariant: no logical rename-source name may survive into the opaque predicate the reader receives. This is the
        // correctness-critical surface (a mistranslated pushed predicate silently drops/keeps the wrong rows), so make it
        // an explicit tripwire rather than trusting the translation blindly.
        assert PhysicalNames.noLogicalNamesRemain(
            mintInput.stream().flatMap(e -> e.references().stream()).map(Attribute::name).toList(),
            renames
        ) : "logical rename-source name leaked into the pushed filter: " + mintInput;

        // Use the SPI to push filters
        FilterPushdownSupport.PushdownResult result = pushableCandidates.isEmpty()
            ? FilterPushdownSupport.PushdownResult.none(List.of())
            : pushdownSupport.pushFilters(mintInput);

        if (result.hasPushedFilter()) {
            // Create new ExternalSourceExec with the (physical) pushed filter and the pushed ESQL expressions in logical space
            ExternalSourceExec newExternalExec = externalExec.withPushedFilterAndExpressions(
                result.pushedFilter(),
                PhysicalNames.translateExpressionNames(result.pushedExpressions(), toLogical)
            );

            // Combine partition conjuncts (always kept) with the SPI's remainder (mapped back to logical), if any.
            List<Expression> remainder = new ArrayList<>(partitionConjuncts);
            if (result.hasRemainder()) {
                remainder.addAll(PhysicalNames.translateExpressionNames(result.remainder(), toLogical));
            }
            if (remainder.isEmpty()) {
                return newExternalExec;
            }
            return new FilterExec(filterExec.source(), newExternalExec, Predicates.combineAnd(remainder));
        }

        // No pushable filters - return original plan
        return filterExec;
    }

    private static Set<String> partitionColumnNames(ExternalSourceExec externalExec) {
        FileList fileList = externalExec.fileList();
        if (fileList == null) {
            return Set.of();
        }
        PartitionMetadata partitionMetadata = fileList.partitionMetadata();
        if (partitionMetadata == null || partitionMetadata.isEmpty()) {
            return Set.of();
        }
        return partitionMetadata.partitionColumns().keySet();
    }

    static boolean referencesAnyColumn(Expression expr, Set<String> columnNames) {
        return expr.references().stream().anyMatch(a -> columnNames.contains(a.name()));
    }

    static String resolveFormatName(Map<String, Object> config, String sourcePath) {
        return FormatNameResolver.resolve(config, sourcePath);
    }

    /**
     * Resolves filter pushdown support for the given format via {@link FormatReader#filterPushdownSupport()}.
     */
    private static FilterPushdownSupport resolveFilterPushdownSupport(String formatName, LocalPhysicalOptimizerContext ctx) {
        FormatReaderRegistry formatReaderRegistry = ctx.external() == null ? null : ctx.external().formatReaderRegistry();
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
        List<Expression> conjuncts = combineFieldExtractRangePairs(splitAnd(condition), pushdownPredicates, aliasReplacedBy);

        List<Expression> pushable = new ArrayList<>();
        List<Expression> nonPushable = new ArrayList<>();
        for (Expression exp : conjuncts) {
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

    /**
     * Pre-combines pairs of {@code field_extract(root, "key") >|>= lo} and
     * {@code field_extract(root, "key") <|<= hi} conjuncts into a single {@link Range} node so
     * the closed range can push to a {@code RangeQuery} on the keyed sub-field {@code root.key}.
     * Single-sided ranges over {@code field_extract} are left untouched: the underlying
     * {@code KeyedFlattenedFieldType.rangeQuery} requires both bounds, so an open range cannot
     * be safely pushed.
     * <p>
     * Two {@code field_extract} expressions are considered the same LHS when
     * {@link FieldExtract#tryAsKeyedSubfieldName} returns the same synthetic field name for
     * both. {@code aliasReplacedBy} is applied to each conjunct before the inspection so the
     * {@code | EVAL e = field_extract(F, "k") | WHERE e >= "a" AND e <= "z"} shape is folded
     * identically to writing the {@code field_extract} call inline.
     * <p>
     * Runs <em>before</em> classification because the individual halves are not pushable in
     * isolation; the mirror combiner {@link #combineEligiblePushableToRange} runs <em>after</em>
     * classification because for indexed {@link Attribute} LHS the individual comparisons are
     * already pushable.
     * <p>
     * Package-private so the unit tests in {@code PushFiltersToSourceTests} can drive it
     * directly (same pattern as {@link #referencesAnyColumn}).
     */
    static List<Expression> combineFieldExtractRangePairs(
        List<Expression> conjuncts,
        LucenePushdownPredicates pushdownPredicates,
        AttributeMap<Attribute> aliasReplacedBy
    ) {
        // Holds the BC in its alias-resolved form (so the produced Range references the
        // underlying field_extract directly, never a ReferenceAttribute alias).
        record Half(int conjunctIndex, EsqlBinaryComparison resolvedBc) {}

        Map<String, Half> lowerByKeyedName = new LinkedHashMap<>();
        Map<String, Half> upperByKeyedName = new LinkedHashMap<>();

        for (int i = 0; i < conjuncts.size(); i++) {
            Expression resolved = aliasReplacedBy.isEmpty()
                ? conjuncts.get(i)
                : conjuncts.get(i).transformUp(ReferenceAttribute.class, r -> aliasReplacedBy.resolve(r, r));

            if (resolved instanceof EsqlBinaryComparison bc && bc.right().foldable() && bc.left() instanceof FieldExtract fe) {
                Optional<String> keyedName = fe.tryAsKeyedSubfieldName(pushdownPredicates);
                if (keyedName.isEmpty()) {
                    continue;
                }
                String name = keyedName.get();
                if (bc instanceof GreaterThan || bc instanceof GreaterThanOrEqual) {
                    lowerByKeyedName.putIfAbsent(name, new Half(i, bc));
                } else if (bc instanceof LessThan || bc instanceof LessThanOrEqual) {
                    upperByKeyedName.putIfAbsent(name, new Half(i, bc));
                }
            }
        }

        if (lowerByKeyedName.isEmpty() || upperByKeyedName.isEmpty()) {
            return conjuncts;
        }

        BitSet consumed = new BitSet(conjuncts.size());
        List<Expression> additions = new ArrayList<>();
        for (Map.Entry<String, Half> entry : lowerByKeyedName.entrySet()) {
            Half lower = entry.getValue();
            Half upper = upperByKeyedName.get(entry.getKey());
            if (upper == null) {
                continue;
            }
            consumed.set(lower.conjunctIndex);
            consumed.set(upper.conjunctIndex);
            additions.add(
                new Range(
                    lower.resolvedBc.source(),
                    lower.resolvedBc.left(),
                    lower.resolvedBc.right(),
                    lower.resolvedBc instanceof GreaterThanOrEqual,
                    upper.resolvedBc.right(),
                    upper.resolvedBc instanceof LessThanOrEqual,
                    lower.resolvedBc.zoneId()
                )
            );
        }

        if (additions.isEmpty()) {
            return conjuncts;
        }

        List<Expression> result = new ArrayList<>(conjuncts.size() - additions.size());
        for (int i = 0; i < conjuncts.size(); i++) {
            if (consumed.get(i) == false) {
                result.add(conjuncts.get(i));
            }
        }
        result.addAll(additions);
        return result;
    }
}
