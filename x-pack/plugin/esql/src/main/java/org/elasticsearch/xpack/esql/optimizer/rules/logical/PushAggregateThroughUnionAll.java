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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.FromPartial;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.StdDev;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.aggregate.ToPartial;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.planner.ToAggregator;

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
 * <p>Two kinds of aggregate are decomposed, via two combine strategies:
 * <ul>
 *   <li><b>Algebraic</b> ({@code COUNT}, {@code SUM}, {@code MIN}, {@code MAX}): each branch emits the
 *   aggregate's partial final value and the coordinator merges them with another plain aggregate
 *   ({@code COUNT}&rarr;{@code SUM}, {@code SUM}&rarr;{@code SUM}, {@code MIN}&rarr;{@code MIN},
 *   {@code MAX}&rarr;{@code MAX}).</li>
 *   <li><b>Intermediate-state</b> ({@code COUNT_DISTINCT}, {@code PERCENTILE}, {@code STDDEV}): these are not
 *   algebraically decomposable, so each branch emits its mergeable intermediate state (HLL sketch, t-digest,
 *   Welford state) as a {@code PARTIAL_AGG} column via {@link ToPartial}, and the coordinator merges those with
 *   {@link FromPartial}.</li>
 * </ul>
 *
 * <p>{@code MEDIAN} and {@code AVG} are not handled here: they are surrogates rewritten before this rule runs
 * ({@code MEDIAN}&rarr;{@code PERCENTILE}, {@code AVG}&rarr;{@code SUM}/{@code COUNT}), so they decompose
 * transitively through the cases above.
 *
 * <p><b>Filters:</b> a per-aggregate filter ({@code COUNT_DISTINCT(x) WHERE ...}) pushes in both paths. For
 * the algebraic path the filtered aggregate is emitted as-is per branch. For the intermediate-state path the
 * filter is carried on the {@link ToPartial} node (the inner aggregate stays unfiltered); the physical layer
 * then wraps the inner aggregator in a filtered supplier so the branch filters before folding into its
 * intermediate state. See {@code ToPartial#supplierWithInnerFilter} and the planner's aggregate factory.
 *
 * <p>Transformation sketch (two-branch case). {@code c}/{@code s} take the algebraic path; {@code d} (a heavy
 * aggregate) takes the intermediate-state path, so its partial column is typed {@code PARTIAL_AGG} and is
 * produced by {@code ToPartial} per branch and merged by {@code FromPartial} on the coordinator:
 * <pre>{@code
 * -- Before:
 * Aggregate[c = COUNT(*), s = SUM(salary), d = COUNT_DISTINCT(salary), dep] BY [dep]
 *   UnionAll[[dep{r1}, salary{r2}]]
 *     EsRelation[[dep{f1}, salary{f2}]]
 *     ExternalRelation[[dep{f3}, salary{f4}]]
 *
 * -- After:
 * Aggregate[c = SUM($$partial$$c), s = SUM($$partial$$s), d = FromPartial($$partial$$d, COUNT_DISTINCT),
 *           dep = Alias($$g_dep)] BY [$$g_dep]
 *   UnionAll[[$$partial$$c, $$partial$$s, $$partial$$d{PARTIAL_AGG}, $$g_dep]]
 *     Aggregate[$$partial$$c = COUNT(*), $$partial$$s = SUM(salary{f2}),
 *               $$partial$$d = ToPartial(COUNT_DISTINCT(salary{f2})), Alias(dep{f1})] BY [Alias(dep{f1})]
 *       EsRelation[[dep{f1}, salary{f2}]]
 *     Aggregate[$$partial$$c = COUNT(*), $$partial$$s = SUM(salary{f4}),
 *               $$partial$$d = ToPartial(COUNT_DISTINCT(salary{f4})), Alias(dep{f3})] BY [Alias(dep{f3})]
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
            // The raw resolved attribute goes in branchGroupings — CombineProjections requires
            // groupings to be plain Attributes, not Aliases. The Alias with the shared ID goes
            // only in branchAggs so the inner Aggregate's output carries the shared ID that the
            // outer UnionAll and combiner Aggregate reference.
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
                branchGroupings.add(resolved);
                groupingIdToAlias.put(gAttr.id(), gAlias);
            }

            // Build the aggregates list for the inner Aggregate (order mirrors the outer aggs):
            // • aggregate function aliases → Alias(partialName, resolvedAggFn, partialId)
            // • grouping passthroughs → the Alias already created above (looked up by ID)
            // • groupings absent from aggs (pruned by PruneColumns) → appended so the branch
            // still produces the shared grouping ID that the combiner groups by
            List<NamedExpression> branchAggs = new ArrayList<>(aggs.size());
            for (NamedExpression ne : aggs) {
                if (ne instanceof Attribute attr && groupingAttrIds.contains(attr.id())) {
                    branchAggs.add(groupingIdToAlias.get(attr.id()));
                } else if (ne instanceof Alias alias && alias.child() instanceof AggregateFunction aggFn) {
                    NameId partialId = aggAliasIdToPartialId.get(alias.id());
                    String partialName = aggAliasIdToPartialName.get(alias.id());
                    AggregateFunction resolvedAggFn = resolveAggFn(aggFn, unionToBranch);
                    branchAggs.add(new Alias(alias.source(), partialName, buildBranchPartial(resolvedAggFn), partialId, true));
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
            } else if (ne instanceof Alias alias && alias.child() instanceof AggregateFunction aggFn) {
                NameId partialId = aggAliasIdToPartialId.get(alias.id());
                String partialName = aggAliasIdToPartialName.get(alias.id());
                DataType partialType = partialDataType(aggFn, alias);
                unionOutput.add(new ReferenceAttribute(alias.source(), null, partialName, partialType, alias.nullable(), partialId, true));
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
                    partialDataType(aggFn, alias),
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
     * Windowed aggregates are never decomposable. Two families qualify: those with a trivial
     * algebraic combiner ({@link #isAlgebraicDecomposable}) and those that merge via intermediate
     * state ({@link #isIntermediateDecomposable}).
     *
     * <p>Per-aggregate filters decompose in both families: a filtered algebraic aggregate is emitted as-is per
     * branch, and a filtered intermediate-state aggregate carries its filter on the {@link ToPartial} node so the
     * branch filters before folding into its intermediate state (see {@link #buildBranchPartial}).
     */
    private static boolean isDecomposable(AggregateFunction aggFn) {
        if (AggregateFunction.NO_WINDOW.equals(aggFn.window()) == false) {
            return false;
        }
        return isAlgebraicDecomposable(aggFn) || isIntermediateDecomposable(aggFn);
    }

    /**
     * Aggregates whose per-branch partial final value can be merged by another plain aggregate:
     * COUNT&rarr;SUM, SUM&rarr;SUM, MIN&rarr;MIN, MAX&rarr;MAX.
     *
     * <p>AVG is intentionally absent: it is a surrogate already rewritten to SUM/COUNT before this rule runs.
     */
    private static boolean isAlgebraicDecomposable(AggregateFunction aggFn) {
        return aggFn instanceof Count || aggFn instanceof Sum || aggFn instanceof Min || aggFn instanceof Max;
    }

    /**
     * Aggregates that are not algebraically decomposable but expose mergeable intermediate state
     * (HLL sketch, t-digest, Welford state). They are pushed by emitting {@link ToPartial} in each branch
     * and merging via {@link FromPartial} on the coordinator.
     *
     * <p>All are {@link ToAggregator}s (required by {@link ToPartial}),
     * but ToAggregator alone is too broad (e.g. VALUES, TOP, SAMPLE), so this is an explicit whitelist.
     * MEDIAN and the histogram/foldable forms of PERCENTILE and COUNT_DISTINCT are rewritten to surrogates
     * before this rule runs and never reach it.
     */
    private static boolean isIntermediateDecomposable(AggregateFunction aggFn) {
        return aggFn instanceof CountDistinct || aggFn instanceof Percentile || aggFn instanceof StdDev;
    }

    /**
     * The data type of the partial column a branch emits for {@code aggFn}: {@code PARTIAL_AGG} for the
     * intermediate-state path (a {@link ToPartial} composite block), otherwise the aggregate's own type.
     */
    private static DataType partialDataType(AggregateFunction aggFn, Alias alias) {
        return isIntermediateDecomposable(aggFn) ? DataType.PARTIAL_AGG : alias.dataType();
    }

    /**
     * Builds the expression a branch emits for a single aggregate. For the algebraic path this is the
     * branch-resolved aggregate itself; for the intermediate-state path it is a {@link ToPartial} wrapping it.
     *
     * <p>When the intermediate-state aggregate carries a per-aggregate filter, the filter is moved onto the
     * {@link ToPartial} node and the wrapped inner aggregate is left unfiltered. The planner then applies the filter
     * to the inner aggregator in the branch's initial phase (see {@code ToPartial#supplierWithInnerFilter}), so the
     * branch folds only the matching rows into its intermediate state. An unfiltered aggregate takes the simple
     * {@code ToPartial(aggFn, aggFn)} form unchanged.
     */
    private static Expression buildBranchPartial(AggregateFunction resolvedAggFn) {
        if (isIntermediateDecomposable(resolvedAggFn) == false) {
            return resolvedAggFn;
        }
        if (resolvedAggFn.hasFilter() == false) {
            return new ToPartial(resolvedAggFn.source(), resolvedAggFn, resolvedAggFn);
        }
        AggregateFunction unfilteredInner = resolvedAggFn.withFilter(Literal.TRUE);
        return new ToPartial(resolvedAggFn.source(), unfilteredInner, resolvedAggFn.filter(), AggregateFunction.NO_WINDOW, unfilteredInner);
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
     * Returns the combiner aggregate function that merges the per-branch partials referenced by
     * {@code partialRef}:
     * <ul>
     *   <li>COUNT → SUM (sum the per-branch counts)</li>
     *   <li>SUM   → SUM (sum the per-branch sums)</li>
     *   <li>MIN   → MIN (minimum of per-branch minima)</li>
     *   <li>MAX   → MAX (maximum of per-branch maxima)</li>
     *   <li>intermediate-state aggregates → {@link FromPartial} (merge the per-branch {@code PARTIAL_AGG}
     *   states and produce the final value)</li>
     * </ul>
     *
     * <p>The {@link FromPartial} carries {@code aggFn} only to select the aggregator supplier;
     * {@link FromPartial#references()} excludes it, so its (now-absent) UnionAll-level attribute references
     * are not part of the combiner's reference set. Any per-aggregate filter is dropped from the carried
     * {@code aggFn}: the branches already filtered their rows before producing the partial states, so the
     * combiner must stay filterless (and dropping it avoids keeping the filter's UnionAll-level references
     * in the plan tree).
     */
    private static AggregateFunction buildCombinerFn(AggregateFunction aggFn, ReferenceAttribute partialRef) {
        Source src = aggFn.source();
        if (isIntermediateDecomposable(aggFn)) {
            return new FromPartial(src, partialRef, aggFn.withFilter(Literal.TRUE));
        }
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
