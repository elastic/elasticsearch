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
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingFunction;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;

public final class CombineProjections extends OptimizerRules.OptimizerRule<UnaryPlan> {
    // don't drop groupings from a local plan, as the layout has already been agreed upon
    private final boolean local;

    public CombineProjections(boolean local) {
        super(OptimizerRules.TransformDirection.UP);
        this.local = local;
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
                    plan = a.with(newGroups, newAggs);
                }
            }
            return plan;
        }

        // Agg with underlying Project (group by on sub-queries)
        if (plan instanceof Aggregate a && child instanceof Project p) {
            var groupings = a.groupings();

            // sanity checks
            for (Expression grouping : groupings) {
                if ((grouping instanceof Attribute
                    || grouping instanceof Alias as && as.child() instanceof GroupingFunction.NonEvaluatableGroupingFunction) == false) {
                    // After applying ReplaceAggregateNestedExpressionWithEval,
                    // evaluatable groupings can only contain attributes.
                    throw new EsqlIllegalArgumentException("Expected an attribute or grouping function, got {}", grouping);
                }
            }
            assert groupings.size() <= 1
                || groupings.stream()
                    .anyMatch(group -> group.anyMatch(expr -> expr instanceof GroupingFunction.NonEvaluatableGroupingFunction)) == false
                : "CombineProjections only tested with a single CATEGORIZE with no additional groups";

            // Collect the alias map for resolving the source (f1 = 1, f2 = f1, etc..)
            AttributeMap.Builder<Attribute> aliasesBuilder = AttributeMap.builder();
            for (NamedExpression ne : p.projections()) {
                // Record the aliases.
                // Projections are just aliases for attributes, so casting is safe.
                aliasesBuilder.put(ne.toAttribute(), (Attribute) Alias.unwrap(ne));
            }
            var aliases = aliasesBuilder.build();

            // Propagate any renames from the lower projection into the upper groupings.
            List<Expression> resolvedGroupings = new ArrayList<>();
            for (Expression grouping : groupings) {
                Expression transformed = grouping.transformUp(Attribute.class, as -> aliases.resolve(as, as));
                resolvedGroupings.add(transformed);
            }

            // This can lead to duplicates in the groupings: e.g.
            // | EVAL x = y | STATS ... BY x, y
            if (local == false) {
                // On the coordinator, we can just discard the duplicates.

                // All substitutions happen before; groupings must be attributes at this point except for non-evaluatable groupings which
                // will be an alias like `c = CATEGORIZE(attribute)`.
                // Due to such aliases, we can't use an AttributeSet to deduplicate. But we can use a regular set to deduplicate based on
                // regular equality (i.e. based on names) instead of name ids.
                // TODO: The deduplication based on simple equality will be insufficient in case of multiple non-evaluatable groupings, e.g.
                // for `| EVAL x = y | STATS ... BY CATEGORIZE(x), CATEGORIZE(y)`. That will require semantic equality instead. Also
                // applies in the local case below.
                LinkedHashSet<Expression> deduplicatedResolvedGroupings = new LinkedHashSet<>(resolvedGroupings);
                List<Expression> newGroupings = new ArrayList<>(deduplicatedResolvedGroupings);
                plan = a.with(p.child(), newGroupings, combineProjections(a.aggregates(), p.projections()));
            } else {
                // On the data node, the groupings must be preserved because they affect the physical output (see
                // AbstractPhysicalOperationProviders#intermediateAttributes).
                // In case that propagating the lower projection leads to duplicates in the resolved groupings, we'll leave an Eval in place
                // of the original projection to create new attributes for the duplicate groups.
                HashSet<Expression> seenResolvedGroupings = new HashSet<>(resolvedGroupings.size());
                List<Expression> newGroupings = new ArrayList<>();
                List<Alias> aliasesAgainstDuplication = new ArrayList<>();

                for (int i = 0; i < groupings.size(); i++) {
                    Expression resolvedGrouping = resolvedGroupings.get(i);
                    if (seenResolvedGroupings.add(resolvedGrouping)) {
                        newGroupings.add(resolvedGrouping);
                    } else {
                        // resolving the renames leads to a duplicate here - we need to alias the underlying attribute this refers to.
                        // should really only be 1 attribute, anyway, but going via .references() includes the case of a
                        // GroupingFunction.NonEvaluatableGroupingFunction.
                        Attribute coreAttribute = resolvedGrouping.references().iterator().next();

                        Alias renameAgainstDuplication = new Alias(
                            coreAttribute.source(),
                            TemporaryNameUtils.locallyUniqueTemporaryName(coreAttribute.name()),
                            coreAttribute
                        );
                        aliasesAgainstDuplication.add(renameAgainstDuplication);

                        // propagate the new alias into the new grouping
                        AttributeMap.Builder<Attribute> resolverBuilder = AttributeMap.builder();
                        resolverBuilder.put(coreAttribute, renameAgainstDuplication.toAttribute());
                        resolverBuilder.build();
                        AttributeMap<Attribute> resolver = resolverBuilder.build();

                        newGroupings.add(resolvedGrouping.transformUp(Attribute.class, attr -> resolver.resolve(attr, attr)));
                    }
                }

                LogicalPlan newChild = aliasesAgainstDuplication.isEmpty()
                    ? p.child()
                    : new Eval(p.source(), p.child(), aliasesAgainstDuplication);
                plan = a.with(newChild, newGroupings, combineProjections(a.aggregates(), p.projections()));
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
        AttributeSet.Builder seen = AttributeSet.builder();
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
        AttributeMap.Builder<NamedExpression> namedExpressionsBuilder = AttributeMap.builder();
        // while also collecting the alias map for resolving the source (f1 = 1, f2 = f1, etc..)
        AttributeMap.Builder<Expression> aliasesBuilder = AttributeMap.builder(lower.size());
        for (NamedExpression ne : lower) {
            // record the alias
            aliasesBuilder.put(ne.toAttribute(), Alias.unwrap(ne));

            // record named expression as is
            if (ne instanceof Alias as) {
                Expression child = as.child();
                namedExpressionsBuilder.put(ne.toAttribute(), as.replaceChild(aliasesBuilder.build().resolve(child, child)));
            } else if (ne instanceof ReferenceAttribute ra) {
                namedExpressionsBuilder.put(ra, ra);
            }
        }
        List<NamedExpression> replaced = new ArrayList<>(upper.size());
        var namedExpressions = namedExpressionsBuilder.build();

        // replace any matching attribute with a lower alias (if there's a match)
        // but clean-up non-top aliases at the end
        for (NamedExpression ne : upper) {
            NamedExpression replacedExp = (NamedExpression) ne.transformUp(Attribute.class, a -> namedExpressions.resolve(a, a));
            replaced.add((NamedExpression) trimNonTopLevelAliases(replacedExp));
        }
        return replaced;
    }

    /**
     * Replace grouping alias previously contained in the aggregations that might have been projected away.
     */
    private List<Expression> replacePrunedAliasesUsedInGroupBy(
        List<Expression> groupings,
        List<? extends NamedExpression> oldAggs,
        List<? extends NamedExpression> newAggs
    ) {
        AttributeMap.Builder<Expression> removedAliasesBuilder = AttributeMap.builder();
        AttributeSet currentAliases = AttributeSet.of(Expressions.asAttributes(newAggs));

        // record only removed aliases
        for (NamedExpression ne : oldAggs) {
            if (ne instanceof Alias alias) {
                var attr = ne.toAttribute();
                if (currentAliases.contains(attr) == false) {
                    removedAliasesBuilder.put(attr, alias.child());
                }
            }
        }
        var removedAliases = removedAliasesBuilder.build();

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
