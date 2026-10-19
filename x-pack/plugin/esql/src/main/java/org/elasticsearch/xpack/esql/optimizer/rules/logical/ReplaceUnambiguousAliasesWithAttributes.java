/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesCollapse;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Remove a projection alias only when its binding can be replaced without capturing another column.
 * <p>
 * A substitution starts at the alias definition, follows its output through explicitly supported unary operators,
 * and stops when that output is projected away. At each step the output must be exactly the old output with that
 * substitution: no columns may be hidden, exposed, reordered, or identified with another column. User names are
 * preserved; only synthetic names may change. Operators with other binding rules, including joins and merges,
 * are boundaries. If any step cannot satisfy these conditions, the entire substitution is discarded.
 * <p>
 * This gives an inductive argument: the definition reads the same input value; each supported parent reads the
 * same values under a capture-free substitution and preserves its output layout; outside that binding's scope
 * nothing changes. A global id-to-attribute map cannot provide this guarantee because independent scopes may reuse ids.
 */
public final class ReplaceUnambiguousAliasesWithAttributes extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        if (plan.resolved() == false) {
            return plan;
        }
        Set<NameId> candidates = new LinkedHashSet<>();
        plan.forEachUp(p -> {
            if (isProjection(p)) {
                for (Expression expression : p.expressions()) {
                    if (expression instanceof Alias alias && alias.child() instanceof Attribute) {
                        candidates.add(alias.id());
                    }
                }
            }
        });
        for (NameId id : candidates) {
            Rewrite rewrite = replace(plan, id);
            if (rewrite != null) {
                plan = rewrite.plan();
            }
        }
        return plan;
    }

    /** The replacement is exported only while the original alias is in this subtree's output. */
    private record Rewrite(LogicalPlan plan, @Nullable Attribute replacement) {}

    /** Return null to discard this candidate's entire rewrite, including changes already made below it. */
    @Nullable
    private static Rewrite replace(LogicalPlan plan, NameId id) {
        List<LogicalPlan> children = new ArrayList<>(plan.children().size());
        Attribute input = null;
        boolean changed = false;
        for (LogicalPlan child : plan.children()) {
            Rewrite rewritten = replace(child, id);
            if (rewritten == null) {
                return null;
            }
            if (rewritten.replacement() != null) {
                if (canSubstituteInput(plan) == false) {
                    return null;
                }
                input = rewritten.replacement();
            }
            children.add(rewritten.plan());
            changed |= child != rewritten.plan();
        }
        return substitute(plan, changed ? plan.replaceChildren(children) : plan, id, input);
    }

    @Nullable
    private static Rewrite substitute(LogicalPlan plan, LogicalPlan result, NameId id, @Nullable Attribute input) {
        List<Alias> definitions = new ArrayList<>();
        if (isProjection(plan)) {
            plan.forEachExpression(Alias.class, a -> {
                if (a.id().equals(id)) {
                    definitions.add(a);
                }
            });
        }
        if (input == null && definitions.isEmpty()) {
            return new Rewrite(result, null);
        }

        Attribute value = input;
        for (Alias alias : definitions) {
            if (alias.child() instanceof Attribute == false || plan.expressions().contains(alias) == false) {
                return null;
            }
            Attribute attribute = (Attribute) alias.child();
            if (value != null && attribute.id().equals(id) && alias.name().equals(attribute.name())) {
                // An identity wrapper may repeat a grouping's definition.
                attribute = value;
            } else if (input != null) {
                // A new binding must not reuse the input alias's id.
                return null;
            }
            if (value != null && value.equals(attribute) == false
                || alias.synthetic() == false && alias.name().equals(attribute.name()) == false) {
                return null;
            }
            value = attribute;
        }
        if (definitions.isEmpty() == false && isUnambiguousDefinition(result, id, value) == false) {
            return null;
        }
        Attribute replacement = value;
        if (capturesLocalBinding(plan, id, replacement, definitions)) {
            return null;
        }
        // A reference to the target in this scope would be captured by the substitution. Definitions need their input value.
        if (definitions.isEmpty()
            && replacement.id().equals(id) == false
            && plan.expressions()
                .stream()
                .anyMatch(e -> e.anyMatch(n -> n instanceof NamedExpression ne && ne.id().equals(replacement.id())))) {
            return null;
        }
        // EVAL defines aliases but cannot have their definitions removed, even if their ids are reused in another scope.
        if (definitions.isEmpty()
            && plan.expressions().stream().anyMatch(e -> e.anyMatch(n -> n instanceof Alias a && a.id().equals(id)))) {
            return null;
        }
        result = result.transformExpressionsOnlyUp(Expression.class, e -> {
            if (e instanceof Attribute attribute && attribute.id().equals(id)) {
                // Preserve the input attribute's kind (notably source fields) and this occurrence's nullability.
                return replacement.withNullability(attribute.nullable());
            }
            if (e instanceof Alias alias && definitions.isEmpty() == false && alias.id().equals(id)) {
                return alias.child();
            }
            return e;
        });
        if (preservesOutput(plan.output(), result.output(), id, replacement) == false) {
            return null;
        }
        boolean exported = plan.output().stream().anyMatch(a -> a.id().equals(id));
        return new Rewrite(result, exported ? replacement : null);
    }

    private static boolean capturesLocalBinding(LogicalPlan plan, NameId id, Attribute replacement, List<Alias> definitions) {
        boolean renamed = definitions.stream().anyMatch(a -> a.name().equals(replacement.name()) == false)
            || plan.children()
                .getFirst()
                .output()
                .stream()
                .anyMatch(a -> a.id().equals(id) && a.name().equals(replacement.name()) == false);
        // Grouping bindings matter even when they are not output. Freshness must cover both ids and names.
        return plan.expressions()
            .stream()
            .anyMatch(
                e -> e.anyMatch(
                    n -> n instanceof Alias a
                        && a.id().equals(id) == false
                        && (a.id().equals(replacement.id()) || renamed && a.name().equals(replacement.name()))
                )
            );
    }

    private static boolean preservesOutput(List<Attribute> before, List<Attribute> after, NameId id, Attribute replacement) {
        List<Attribute> expected = new ArrayList<>(before.size());
        for (Attribute attribute : before) {
            if (attribute.id().equals(id)) {
                if (attribute.dataType() != replacement.dataType()
                    || attribute.synthetic() == false && attribute.name().equals(replacement.name()) == false) {
                    return false;
                }
                if (before.stream()
                    .anyMatch(
                        a -> a.id().equals(id) == false && (a.id().equals(replacement.id()) || a.name().equals(replacement.name()))
                    )) {
                    return false;
                }
                expected.add(replacement.withNullability(attribute.nullable()));
            } else {
                expected.add(attribute);
            }
        }
        // Recompute the output from the rewritten node: checking the old layout alone misses name-based shadowing.
        return after.equals(expected);
    }

    private static boolean isUnambiguousDefinition(LogicalPlan plan, NameId id, Attribute value) {
        var input = plan.children().getFirst().output();
        if (input.stream().filter(a -> a.name().equals(value.name())).count() != 1
            || input.stream().noneMatch(a -> a.equals(value))
            || id.equals(value.id()) == false && input.stream().anyMatch(a -> a.id().equals(id))) {
            return false;
        }
        if (id.equals(value.id())) {
            return true;
        }
        // In particular, two grouping identities for a multivalued input must not collapse into one grouping.
        for (Expression expression : plan.expressions()) {
            if (expression instanceof Alias alias && alias.id().equals(id) || expression instanceof Attribute a && a.id().equals(id)) {
                continue;
            }
            if (Alias.unwrap(expression) instanceof Attribute attribute && attribute.id().equals(value.id())) {
                return false;
            }
        }
        return true;
    }

    private static boolean isProjection(LogicalPlan plan) {
        return plan.getClass() == Project.class || plan.getClass() == Aggregate.class || plan.getClass() == TimeSeriesAggregate.class;
    }

    private static boolean canSubstituteInput(LogicalPlan plan) {
        // These operators use attribute bindings for their inputs. Do not infer this property from UnaryPlan or Streaming.
        return isProjection(plan)
            || plan.getClass() == Eval.class
            || plan.getClass() == Filter.class
            || plan.getClass() == OrderBy.class
            || plan.getClass() == Limit.class
            || plan.getClass() == TopN.class
            || plan.getClass() == TimeSeriesCollapse.class;
    }
}
