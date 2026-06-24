/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decomposes an {@link Aggregate} whose child is a direct-leaf {@link UnionAll}
 * (heterogeneous FROM) into per-branch partial aggregates combined by a final
 * merge aggregate.
 *
 * <p>Only decomposable aggregates are rewritten:
 * {@code COUNT}, {@code SUM}, {@code MIN}, and {@code MAX}.
 *
 * <p>Transformation sketch (two-branch case):
 * <pre>{@code
 * -- Before:
 * Aggregate[c = COUNT(*), s = SUM(salary), dep] BY [dep]
 *   UnionAll[[dep{r1}, salary{r2}]]
 *     EsRelation[[dep{f1}, salary{f2}]]
 *     ExternalRelation[[dep{f3}, salary{f4}]]
 *
 * -- After:
 * Aggregate[c = SUM($$partial$$c), s = SUM($$partial$$s), dep = Alias($$g_dep)] BY [$$g_dep]
 *   UnionAll[[$$partial$$c, $$partial$$s, $$g_dep]]
 *     Aggregate[$$partial$$c = COUNT(*), $$partial$$s = SUM(salary{f2}), Alias(dep{f1})] BY [Alias(dep{f1})]
 *       EsRelation[[dep{f1}, salary{f2}]]
 *     Aggregate[$$partial$$c = COUNT(*), $$partial$$s = SUM(salary{f4}), Alias(dep{f3})] BY [Alias(dep{f3})]
 *       ExternalRelation[[dep{f3}, salary{f4}]]
 * }</pre>
 *
 * <p>All partial aggregate aliases and shared grouping aliases use pre-allocated {@link NameId}s
 * that are consistent across branches, so the outer UnionAll output and the outer Aggregate's
 * grouping/aggregate references resolve correctly. The outer Aggregate preserves the output IDs
 * of the original Aggregate so that plan nodes above it remain valid.
 */
public class PushAggregateThroughUnionAll extends OptimizerRules.OptimizerRule<Aggregate> {

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        if (!(aggregate.child() instanceof UnionAll unionAll)) {
            return aggregate;
        }
        if (PushDownUtils.isLeafUnionAll(unionAll) == false) {
            return aggregate;
        }

        List<? extends NamedExpression> aggs = aggregate.aggregates();
        List<Expression> groupings = aggregate.groupings();

        // Collect the attribute IDs for grouping column references.
        // Only plain Attribute groupings are supported (e.g. BY dept, not BY dep = dept).
        Set<NameId> groupingAttrIds = new HashSet<>();
        for (Expression g : groupings) {
            if (g instanceof Attribute attr) {
                groupingAttrIds.add(attr.id());
            } else {
                return aggregate;
            }
        }

        // Pre-allocate a shared NameId per grouping attribute.
        // These IDs appear in both the inner-branch grouping Aliases and the outer UnionAll output.
        LinkedHashMap<NameId, NameId> groupingIdToSharedId = new LinkedHashMap<>();
        for (Expression g : groupings) {
            Attribute attr = (Attribute) g;
            groupingIdToSharedId.put(attr.id(), new NameId());
        }

        // Pre-allocate a partial NameId per aggregate function alias, and determine the partial name.
        // Both are used across all branches so the IDs line up in the outer UnionAll.
        LinkedHashMap<NameId, NameId> aggAliasIdToPartialId = new LinkedHashMap<>();
        LinkedHashMap<NameId, String> aggAliasIdToPartialName = new LinkedHashMap<>();

        // Track which grouping IDs appear as explicit passthroughs in aggs. PruneColumns can remove
        // a grouping from aggregates() while keeping it in groupings() (e.g. STATS c = COUNT(*) BY dept | KEEP c
        // drops dept from the output). Groupings absent from aggs must still be emitted by each branch
        // and by the UnionAll so the combiner can group by them.
        Set<NameId> groupingsInAggs = new HashSet<>();
        for (NamedExpression ne : aggs) {
            if (ne instanceof Attribute attr && groupingAttrIds.contains(attr.id())) {
                groupingsInAggs.add(attr.id());
            } else if (ne instanceof Alias alias && alias.child() instanceof AggregateFunction aggFn) {
                if (isDecomposable(aggFn) == false) {
                    return aggregate;
                }
                aggAliasIdToPartialId.put(alias.id(), new NameId());
                aggAliasIdToPartialName.put(alias.id(), "$$partial$$" + alias.name());
            } else {
                return aggregate; // unexpected shape
            }
        }

        // Build per-branch inner aggregates.
        List<LogicalPlan> newBranches = new ArrayList<>(unionAll.children().size());
        for (LogicalPlan branch : unionAll.children()) {
            Map<NameId, Attribute> unionToBranch = buildNameResolutionMap(unionAll.output(), branch.output());

            // Build grouping Aliases: Alias(name, branchAttr, sharedGroupingId).
            // The same Alias is placed in both branchGroupings and (as a passthrough) in branchAggs.
            List<Expression> branchGroupings = new ArrayList<>(groupings.size());
            Map<NameId, Alias> groupingIdToAlias = new HashMap<>(groupings.size() * 2);
            for (Expression g : groupings) {
                Attribute gAttr = (Attribute) g;
                Attribute resolved = unionToBranch.get(gAttr.id());
                if (resolved == null) {
                    return aggregate; // column not found in this branch — bail out
                }
                NameId sharedId = groupingIdToSharedId.get(gAttr.id());
                Alias gAlias = new Alias(gAttr.source(), gAttr.name(), resolved, sharedId, true);
                branchGroupings.add(gAlias);
                groupingIdToAlias.put(gAttr.id(), gAlias);
            }

            // Build the aggregates list for the inner Aggregate (order mirrors the outer aggs):
            // • aggregate function aliases → Alias(partialName, resolvedAggFn, partialId)
            // • grouping passthroughs → the Alias already created above (looked up by ID)
            // • groupings absent from aggs (pruned by PruneColumns) → appended so the branch
            //   still produces the shared grouping ID that the combiner groups by
            List<NamedExpression> branchAggs = new ArrayList<>(aggs.size());
            for (NamedExpression ne : aggs) {
                if (ne instanceof Attribute attr && groupingAttrIds.contains(attr.id())) {
                    branchAggs.add(groupingIdToAlias.get(attr.id()));
                } else if (ne instanceof Alias alias && alias.child() instanceof AggregateFunction aggFn) {
                    NameId partialId = aggAliasIdToPartialId.get(alias.id());
                    String partialName = aggAliasIdToPartialName.get(alias.id());
                    AggregateFunction resolvedAggFn = resolveAggFn(aggFn, unionToBranch);
                    branchAggs.add(new Alias(alias.source(), partialName, resolvedAggFn, partialId, true));
                }
            }
            for (Expression g : groupings) {
                Attribute gAttr = (Attribute) g;
                if (groupingsInAggs.contains(gAttr.id()) == false) {
                    branchAggs.add(groupingIdToAlias.get(gAttr.id()));
                }
            }

            newBranches.add(new Aggregate(aggregate.source(), branch, branchGroupings, branchAggs));
        }

        // Build the explicit output list for the outer UnionAll.
        // Order: aggs-derived columns first (partial agg results + explicit grouping passthroughs),
        // then any groupings that PruneColumns removed from aggs — they must still be produced so
        // the combiner can reference them in its groupings().
        List<Attribute> unionOutput = new ArrayList<>(aggs.size());
        for (NamedExpression ne : aggs) {
            if (ne instanceof Attribute attr && groupingAttrIds.contains(attr.id())) {
                NameId sharedId = groupingIdToSharedId.get(attr.id());
                unionOutput.add(new ReferenceAttribute(attr.source(), null, attr.name(), attr.dataType(), attr.nullable(), sharedId, true));
            } else if (ne instanceof Alias alias) {
                NameId partialId = aggAliasIdToPartialId.get(alias.id());
                String partialName = aggAliasIdToPartialName.get(alias.id());
                unionOutput.add(
                    new ReferenceAttribute(alias.source(), null, partialName, alias.dataType(), alias.nullable(), partialId, true)
                );
            }
        }
        for (Expression g : groupings) {
            Attribute gAttr = (Attribute) g;
            if (groupingsInAggs.contains(gAttr.id()) == false) {
                NameId sharedId = groupingIdToSharedId.get(gAttr.id());
                unionOutput.add(
                    new ReferenceAttribute(gAttr.source(), null, gAttr.name(), gAttr.dataType(), gAttr.nullable(), sharedId, true)
                );
            }
        }
        UnionAll newUnionAll = new UnionAll(unionAll.source(), newBranches, unionOutput);

        // Build the outer combiner Aggregate.
        // Groupings reference the shared grouping IDs from the UnionAll output.
        List<Expression> combinerGroupings = new ArrayList<>(groupings.size());
        for (Expression g : groupings) {
            Attribute gAttr = (Attribute) g;
            NameId sharedId = groupingIdToSharedId.get(gAttr.id());
            combinerGroupings.add(
                new ReferenceAttribute(gAttr.source(), null, gAttr.name(), gAttr.dataType(), gAttr.nullable(), sharedId, true)
            );
        }

        // Combiner aggregates:
        // • aggregate function aliases → Alias(originalName, combinerFn(partialRef), originalId)
        // (original output IDs are preserved so plan nodes above remain valid)
        // • grouping passthroughs → Alias(name, sharedRef, originalId) to restore the original ID
        List<NamedExpression> combinerAggs = new ArrayList<>(aggs.size());
        for (NamedExpression ne : aggs) {
            if (ne instanceof Attribute attr && groupingAttrIds.contains(attr.id())) {
                NameId sharedId = groupingIdToSharedId.get(attr.id());
                ReferenceAttribute sharedRef = new ReferenceAttribute(
                    attr.source(),
                    null,
                    attr.name(),
                    attr.dataType(),
                    attr.nullable(),
                    sharedId,
                    true
                );
                // Wrap in Alias to restore the original output attribute ID.
                combinerAggs.add(new Alias(attr.source(), attr.name(), sharedRef, attr.id(), attr.synthetic()));
            } else if (ne instanceof Alias alias && alias.child() instanceof AggregateFunction aggFn) {
                NameId partialId = aggAliasIdToPartialId.get(alias.id());
                String partialName = aggAliasIdToPartialName.get(alias.id());
                ReferenceAttribute partialRef = new ReferenceAttribute(
                    alias.source(),
                    null,
                    partialName,
                    alias.dataType(),
                    alias.nullable(),
                    partialId,
                    true
                );
                AggregateFunction combinerFn = buildCombinerFn(aggFn, partialRef);
                combinerAggs.add(new Alias(alias.source(), alias.name(), combinerFn, alias.id(), alias.synthetic()));
            }
        }

        return new Aggregate(aggregate.source(), newUnionAll, combinerGroupings, combinerAggs);
    }

    /**
     * Returns true when {@code aggFn} can be decomposed across independent partitions.
     * COUNT, SUM, MIN, and MAX are decomposable; windowed aggregates are never decomposable.
     */
    private static boolean isDecomposable(AggregateFunction aggFn) {
        if (AggregateFunction.NO_WINDOW.equals(aggFn.window()) == false) {
            return false;
        }
        // Note: AVG could also be decomposed, but being a surrogate already decomposed in Sum/Count
        // brings significant additional complexity.
        return aggFn instanceof Count || aggFn instanceof Sum || aggFn instanceof Min || aggFn instanceof Max;
    }

    /**
     * Builds a map from UnionAll output attribute IDs to the corresponding branch attribute,
     * matched by column name.
     */
    private static Map<NameId, Attribute> buildNameResolutionMap(List<Attribute> unionOutput, List<Attribute> branchOutput) {
        Map<String, Attribute> branchByName = new HashMap<>(branchOutput.size() * 2);
        for (Attribute attr : branchOutput) {
            branchByName.put(attr.name(), attr);
        }
        Map<NameId, Attribute> result = new HashMap<>(unionOutput.size() * 2);
        for (Attribute unionAttr : unionOutput) {
            Attribute branchAttr = branchByName.get(unionAttr.name());
            if (branchAttr != null) {
                result.put(unionAttr.id(), branchAttr);
            }
        }
        return result;
    }

    /**
     * Rewrites the field (and filter, if present) of {@code aggFn} so that attribute references
     * point to the branch's attributes rather than the UnionAll's output attributes.
     */
    private static AggregateFunction resolveAggFn(AggregateFunction aggFn, Map<NameId, Attribute> unionToBranch) {
        Expression resolvedField = resolveExpr(aggFn.field(), unionToBranch);
        AggregateFunction resolved = aggFn.withField(resolvedField);
        if (aggFn.hasFilter()) {
            Expression resolvedFilter = resolveExpr(aggFn.filter(), unionToBranch);
            resolved = resolved.withFilter(resolvedFilter);
        }
        return resolved;
    }

    private static Expression resolveExpr(Expression expr, Map<NameId, Attribute> unionToBranch) {
        return expr.transformUp(Attribute.class, attr -> {
            Attribute resolved = unionToBranch.get(attr.id());
            return resolved != null ? resolved : attr;
        });
    }

    /**
     * Returns the combiner aggregate function that merges the partial results from each branch:
     * <ul>
     *   <li>COUNT → SUM (sum the per-branch counts)</li>
     *   <li>SUM   → SUM (sum the per-branch sums)</li>
     *   <li>MIN   → MIN (minimum of per-branch minima)</li>
     *   <li>MAX   → MAX (maximum of per-branch maxima)</li>
     * </ul>
     */
    private static AggregateFunction buildCombinerFn(AggregateFunction aggFn, ReferenceAttribute partialRef) {
        Source src = aggFn.source();
        if (aggFn instanceof Count) {
            return new Sum(src, partialRef);
        } else if (aggFn instanceof Sum) {
            return new Sum(src, partialRef);
        } else if (aggFn instanceof Min) {
            return new Min(src, partialRef);
        } else if (aggFn instanceof Max) {
            return new Max(src, partialRef);
        }
        throw new IllegalStateException("isDecomposable should have rejected: " + aggFn.getClass().getSimpleName());
    }
}
