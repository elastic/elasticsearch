/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.DocPreserving;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinType;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Replace nested expressions over aggregates with synthetic eval post the aggregation
 * stats a = sum(a) + min(b) by x
 * becomes
 * stats a1 = sum(a), a2 = min(b) by x | eval a = a1 + a2 | keep a, x
 * The rule also considers expressions applied over groups:
 * {@code STATS a = x + 1 BY x} becomes {@code STATS BY x | EVAL a = x + 1 | KEEP a, x}
 * And to combine the two:
 * stats a = x + count(*) by x
 * becomes
 * stats a1 = count(*) by x | eval a = x + a1 | keep a1, x
 * Since the logic is very similar, this rule also handles duplicate aggregate functions to avoid duplicate compute
 * stats a = min(x), b = min(x), c = count(*), d = count() by g
 * becomes
 * stats a = min(x), c = count(*) by g | eval b = a, d = c | keep a, b, c, d, g
 */
public class ReplaceAggregateAggExpressionWithEval extends OptimizerRules.OptimizerRule<Aggregate> {
    private final boolean replaceNestedExpressions;

    public ReplaceAggregateAggExpressionWithEval(boolean replaceNestedExpressions) {
        super(OptimizerRules.TransformDirection.UP);
        this.replaceNestedExpressions = replaceNestedExpressions;
    }

    public ReplaceAggregateAggExpressionWithEval() {
        super(OptimizerRules.TransformDirection.UP);
        this.replaceNestedExpressions = true;
    }

    @Override
    protected LogicalPlan rule(Aggregate aggregate) {
        // an alias map for evaluatable grouping functions
        AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder();
        // a function map for non-evaluatable grouping functions
        Map<GroupingFunction.NonEvaluatableGroupingFunction, Attribute> nonEvalGroupingAttributes = new HashMap<>(
            aggregate.groupings().size()
        );
        Consumer<Alias> collectAlias = a -> {
            if (a.child() instanceof GroupingFunction.NonEvaluatableGroupingFunction groupingFunction) {
                nonEvalGroupingAttributes.put(groupingFunction, a.toAttribute());
            } else {
                aliasesBuilder.put(a.toAttribute(), a.child());
            }
        };
        AttributeSet childAttributes = aggregate.child().outputSet();
        aggregate.forEachExpression(Alias.class, a -> {
            // An output alias that reuses an input attribute's id says nothing about the input values the aggregate functions read.
            if (childAttributes.contains(a.toAttribute()) == false) {
                collectAlias.accept(a);
            }
        });
        collectAliases(aggregate.child(), collectAlias);
        var aliases = aliasesBuilder.build();

        // break down each aggregate into AggregateFunction and/or grouping key
        // preserve the projection at the end
        List<? extends NamedExpression> aggs = aggregate.aggregates();

        // root/naked aggs
        Map<AggregateFunction, Alias> rootAggs = Maps.newLinkedHashMapWithExpectedSize(aggs.size());
        // evals (original expression relying on multiple aggs)
        List<Alias> newEvals = new ArrayList<>();
        List<NamedExpression> newProjections = new ArrayList<>();
        // track the aggregate aggs (including grouping which is not an AggregateFunction)
        List<NamedExpression> newAggs = new ArrayList<>();

        Holder<Boolean> changed = new Holder<>(false);
        int[] counter = new int[] { 0 };

        for (NamedExpression agg : aggs) {
            if (agg instanceof Alias as) {
                // use intermediate variable to mark child as final for lambda use
                Expression child = as.child();

                // common case - handle duplicates
                if (child instanceof AggregateFunction af) {
                    // canonical representation, with resolved aliases
                    AggregateFunction canonical = getCannonical(af, aliases);

                    Alias found = rootAggs.get(canonical);
                    // aggregate is new
                    if (found == null) {
                        rootAggs.put(canonical, as);
                        newAggs.add(as);
                        newProjections.add(as.toAttribute());
                    }
                    // agg already exists - preserve the current alias but point it to the existing agg
                    // thus don't add it to the list of aggs as we don't want duplicated compute
                    else {
                        changed.set(true);
                        newProjections.add(as.replaceChild(found.toAttribute()));
                    }
                }
                // nested expression over aggregate function or groups
                // replace them with reference and move the expression into a follow-up eval
                else if (replaceNestedExpressions) {
                    changed.set(true);
                    // Use transformDown so the outermost aggregate is extracted as a whole. This matters for aggregates that wrap
                    // another aggregate (e.g. SPARKLINE(COUNT(*), ...)): descending into the inner aggregate would lift it out
                    // separately and leave the outer aggregate malformed. Extracting the outer aggregate as a single leaf reference
                    // stops the recursion before it reaches the inner aggregate, leaving it intact for later rules to handle.
                    Expression aggExpression = child.transformDown(AggregateFunction.class, af -> {
                        // canonical representation, with resolved aliases
                        AggregateFunction canonical = getCannonical(af, aliases);
                        Alias alias = rootAggs.get(canonical);
                        if (alias == null) {
                            // create synthetic alias over the found agg function
                            alias = new Alias(af.source(), syntheticName(canonical, child, counter[0]++), af.canonical(), null, true);
                            // and remember it to remove duplicates
                            rootAggs.put(canonical, alias);
                            // add it to the list of aggregates and continue
                            newAggs.add(alias);
                        }
                        // (even when found) return a reference to it
                        return alias.toAttribute();
                    });

                    // replace non-evaluatable grouping functions with their references
                    aggExpression = aggExpression.transformUp(
                        GroupingFunction.NonEvaluatableGroupingFunction.class,
                        nonEvalGroupingAttributes::get
                    );

                    Alias alias = as.replaceChild(aggExpression);
                    newEvals.add(alias);
                    newProjections.add(alias.toAttribute());
                } else {
                    newAggs.add(agg);
                    newProjections.add(agg.toAttribute());
                }
            }
            // not an alias (e.g. grouping field)
            else {
                newAggs.add(agg);
                newProjections.add(agg.toAttribute());
            }
        }

        LogicalPlan plan = aggregate;
        if (changed.get()) {
            Source source = aggregate.source();
            plan = aggregate.with(aggregate.child(), aggregate.groupings(), newAggs);
            if (newEvals.size() > 0) {
                plan = new Eval(source, plan, newEvals);
            }
            // preserve initial projection
            plan = new Project(source, plan, newProjections);
        }

        return plan;
    }

    /**
     * Collects the aliases that hold on every row reaching the aggregate being optimized, the only ones safe for comparing its
     * aggregate functions. The walk only follows {@link DocPreserving} plans that keep their input's values and the left-hand side
     * of joins; any other plan stops it, which at worst computes a duplicate aggregate function twice. A {@link Row} ends the walk
     * with its own fields, which hold on its only row.
     */
    private static void collectAliases(LogicalPlan plan, Consumer<Alias> consumer) {
        if (plan instanceof Row row) {
            row.fields().forEach(consumer);
            return;
        }
        LogicalPlan input = passThroughInput(plan);
        if (input != null) {
            plan.forEachExpression(Alias.class, consumer);
            collectAliases(input, consumer);
        }
    }

    /**
     * Returns the input whose rows pass through {@code plan} with the same values, or {@code null} if there is none. A join keeps
     * the values of its left-hand side even when it is not {@link DocPreserving}, such as {@code LOOKUP JOIN}: it may duplicate or
     * drop left rows, but does not change their values. Its right-hand side never qualifies, since it is null-filled on unmatched
     * rows or, for {@code INLINE STATS}, computed per group, and neither does the left-hand side of a {@code RIGHT} or {@code FULL}
     * join. Neither does the input of a plan that writes new values under one of its input's attributes, see
     * {@link #writesUnderInputAttribute}.
     */
    private static LogicalPlan passThroughInput(LogicalPlan plan) {
        if (plan instanceof Join join) {
            JoinType type = join.config().type();
            return JoinTypes.RIGHT.equals(type) || JoinTypes.FULL.equals(type) ? null : join.left();
        }
        if (plan instanceof DocPreserving docPreserving) {
            LogicalPlan input = docPreserving.preservingInput();
            return writesUnderInputAttribute(plan, input) ? null : input;
        }
        return null;
    }

    /**
     * Whether {@code plan} gives one of its input's attribute ids a new value: as a generated attribute, such as {@code RERANK}
     * scoring into an existing {@code _score}, or as an alias, such as the projection that renames the score of a pushed-down
     * {@code RERANK} back to {@code _score}. Aliases collected at or below such a plan may then mean something else above it.
     */
    private static boolean writesUnderInputAttribute(LogicalPlan plan, LogicalPlan input) {
        AttributeSet inputAttributes = input.outputSet();
        if (plan instanceof GeneratingPlan<?> generatingPlan
            && generatingPlan.generatedAttributes().stream().anyMatch(inputAttributes::contains)) {
            return true;
        }
        return plan.expressions()
            .stream()
            .anyMatch(e -> e.anyMatch(x -> x instanceof Alias alias && inputAttributes.contains(alias.toAttribute())));
    }

    private static AggregateFunction getCannonical(AggregateFunction af, AttributeMap<Expression> aliases) {
        return (AggregateFunction) af.canonical().transformUp(e -> aliases.resolve(e, e));
    }

    private static String syntheticName(Expression expression, Expression af, int counter) {
        return TemporaryNameGenerator.temporaryName(expression, af, counter);
    }
}
