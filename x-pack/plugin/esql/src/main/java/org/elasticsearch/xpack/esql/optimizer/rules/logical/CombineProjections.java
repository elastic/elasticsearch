/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.grouping.Categorize;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public final class CombineProjections extends OptimizerRules.OptimizerRule<UnaryPlan> {

    public CombineProjections() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(UnaryPlan plan) {
        LogicalPlan child = plan.child();

        if (plan instanceof Project project) {
            if (child instanceof Project p) {
                // eliminate lower project but first replace the aliases in the upper one
                project = p.withProjections(combineProjections(project.projections(), p.projections()));
                child = project.child();
                plan = project;
                // don't return the plan since the grandchild (now child) might be an aggregate that could not be folded on the way up
                // e.g. stats c = count(x) | project c, c as x | project x
                // try to apply the rule again opportunistically as another node might be pushed in (a limit might be pushed in)
            }
            // check if the projection eliminates certain aggregates
            // but be mindful of aliases to existing aggregates that we don't want to duplicate to avoid redundant work
            if (child instanceof Aggregate a) {
                var aggs = a.aggregates();
                var newAggs = projectAggregations(project.projections(), aggs);
                // project can be fully removed
                if (newAggs != null) {
                    var newGroups = replacePrunedAliasesUsedInGroupBy(a.groupings(), aggs, newAggs);
                    plan = new Aggregate(a.source(), a.child(), a.aggregateType(), newGroups, newAggs);
                }
            }
            return plan;
        }

        // Agg with underlying Project (group by on sub-queries)
        if (plan instanceof Aggregate a) {
            if (child instanceof Project p) {
                var groupings = a.groupings();
                List<NamedExpression> groupingAttrs = new ArrayList<>(a.groupings().size());
                for (Expression grouping : groupings) {
                    if (grouping instanceof Attribute attribute) {
                        groupingAttrs.add(attribute);
                    } else if (grouping instanceof Alias as && as.child() instanceof Categorize) {
                        groupingAttrs.add(as);
                    } else {
                        // After applying ReplaceAggregateNestedExpressionWithEval,
                        // groupings (except Categorize) can only contain attributes.
                        throw new EsqlIllegalArgumentException("Expected an Attribute, got {}", grouping);
                    }
                }
                plan = new Aggregate(
                    a.source(),
                    p.child(),
                    a.aggregateType(),
                    combineUpperGroupingsAndLowerProjections(groupingAttrs, p.projections()),
                    combineProjections(a.aggregates(), p.projections())
                );
            }
        }

        return plan;
    }

    // variant of #combineProjections specialized for project followed by agg due to the rewrite rules applied on aggregations
    // this method tries to combine the projections by paying attention to:
    // - aggregations that are projected away - remove them
    // - aliases in the project that point to aggregates - keep them in place (to avoid duplicating the aggs)
    private static List<? extends NamedExpression> projectAggregations(
        List<? extends NamedExpression> upperProjection,
        List<? extends NamedExpression> lowerAggregations
    ) {
        AttributeSet seen = new AttributeSet();
        for (NamedExpression upper : upperProjection) {
            Expression unwrapped = Alias.unwrap(upper);
            // projection contains an inner alias (point to an existing fields inside the projection)
            if (seen.contains(unwrapped)) {
                return null;
            }
            seen.add(Expressions.attribute(unwrapped));
        }

        lowerAggregations = combineProjections(upperProjection, lowerAggregations);

        return lowerAggregations;
    }

    // normally only the upper projections should survive but since the lower list might have aliases definitions
    // that might be reused by the upper one, these need to be replaced.
    // for example an alias defined in the lower list might be referred in the upper - without replacing it the alias becomes invalid
    private static List<NamedExpression> combineProjections(List<? extends NamedExpression> upper, List<? extends NamedExpression> lower) {

        // collect named expressions declaration in the lower list
        AttributeMap<NamedExpression> namedExpressions = new AttributeMap<>();
        // while also collecting the alias map for resolving the source (f1 = 1, f2 = f1, etc..)
        AttributeMap<Expression> aliases = new AttributeMap<>();
        for (NamedExpression ne : lower) {
            // record the alias
            aliases.put(ne.toAttribute(), Alias.unwrap(ne));

            // record named expression as is
            if (ne instanceof Alias as) {
                Expression child = as.child();
                namedExpressions.put(ne.toAttribute(), as.replaceChild(aliases.resolve(child, child)));
            }
        }
        List<NamedExpression> replaced = new ArrayList<>();

        // replace any matching attribute with a lower alias (if there's a match)
        // but clean-up non-top aliases at the end
        for (NamedExpression ne : upper) {
            NamedExpression replacedExp = (NamedExpression) ne.transformUp(Attribute.class, a -> namedExpressions.resolve(a, a));
            replaced.add((NamedExpression) trimNonTopLevelAliases(replacedExp));
        }
        return replaced;
    }

    private static List<Expression> combineUpperGroupingsAndLowerProjections(
        List<? extends NamedExpression> upperGroupings,
        List<? extends NamedExpression> lowerProjections
    ) {
        assert upperGroupings.size() <= 1
            || upperGroupings.stream().anyMatch(group -> group.anyMatch(expr -> expr instanceof Categorize)) == false
            : "CombineProjections only tested with a single CATEGORIZE with no additional groups";
        // Collect the alias map for resolving the source (f1 = 1, f2 = f1, etc..)
        AttributeMap<Attribute> aliases = new AttributeMap<>();
        for (NamedExpression ne : lowerProjections) {
            // Record the aliases.
            // Projections are just aliases for attributes, so casting is safe.
            aliases.put(ne.toAttribute(), (Attribute) Alias.unwrap(ne));
        }

        // Propagate any renames from the lower projection into the upper groupings.
        // This can lead to duplicates: e.g.
        // | EVAL x = y | STATS ... BY x, y
        // All substitutions happen before; groupings must be attributes at this point except for CATEGORIZE which will be an alias like
        // `c = CATEGORIZE(attribute)`.
        // Therefore, it is correct to deduplicate based on simple equality (based on names) instead of name ids (Set vs. AttributeSet).
        // TODO: The deduplication based on simple equality will be insufficient in case of multiple CATEGORIZEs, e.g. for
        // `| EVAL x = y | STATS ... BY CATEGORIZE(x), CATEGORIZE(y)`. That will require semantic equality instead.
        LinkedHashSet<NamedExpression> resolvedGroupings = new LinkedHashSet<>();
        for (NamedExpression ne : upperGroupings) {
            NamedExpression transformed = (NamedExpression) ne.transformUp(Attribute.class, a -> aliases.resolve(a, a));
            resolvedGroupings.add(transformed);
        }
        return new ArrayList<>(resolvedGroupings);
    }

    /**
     * Replace grouping alias previously contained in the aggregations that might have been projected away.
     */
    private List<Expression> replacePrunedAliasesUsedInGroupBy(
        List<Expression> groupings,
        List<? extends NamedExpression> oldAggs,
        List<? extends NamedExpression> newAggs
    ) {
        AttributeMap<Expression> removedAliases = new AttributeMap<>();
        AttributeSet currentAliases = new AttributeSet(Expressions.asAttributes(newAggs));

        // record only removed aliases
        for (NamedExpression ne : oldAggs) {
            if (ne instanceof Alias alias) {
                var attr = ne.toAttribute();
                if (currentAliases.contains(attr) == false) {
                    removedAliases.put(attr, alias.child());
                }
            }
        }

        if (removedAliases.isEmpty()) {
            return groupings;
        }

        var newGroupings = new ArrayList<Expression>(groupings.size());
        for (Expression group : groupings) {
            var transformed = group.transformUp(Attribute.class, a -> removedAliases.resolve(a, a));
            if (Expressions.anyMatch(newGroupings, g -> Expressions.equalsAsAttribute(g, transformed)) == false) {
                newGroupings.add(transformed);
            }
        }

        return newGroupings;
    }

    public static Expression trimNonTopLevelAliases(Expression e) {
        return e instanceof Alias a ? a.replaceChild(trimAliases(a.child())) : trimAliases(e);
    }

    private static Expression trimAliases(Expression e) {
        return e.transformDown(Alias.class, Alias::child);
    }
}
