/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRule;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class AnalyzerRules {

    public abstract static class AnalyzerRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

        // transformUp (post-order) - that is first children and then the node
        // but with a twist; only if the tree is not resolved or analyzed
        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return plan.transformUp(typeToken(), t -> t.analyzed() || skipResolved() && t.resolved() ? t : rule(t));
        }

        protected abstract LogicalPlan rule(SubPlan plan);

        protected boolean skipResolved() {
            return true;
        }
    }

    public abstract static class ParameterizedAnalyzerRule<SubPlan extends LogicalPlan, P> extends ParameterizedRule<
        SubPlan,
        LogicalPlan,
        P> {

        // transformUp (post-order) - that is first children and then the node
        // but with a twist; only if the tree is not resolved or analyzed
        public final LogicalPlan apply(LogicalPlan plan, P context) {
            return plan.transformUp(typeToken(), t -> t.analyzed() || skipResolved() && t.resolved() ? t : rule(t, context));
        }

        protected abstract LogicalPlan rule(SubPlan plan, P context);

        protected boolean skipResolved() {
            return true;
        }
    }

    public abstract static class BaseAnalyzerRule extends AnalyzerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            if (plan.childrenResolved() == false) {
                return plan;
            }
            return doRule(plan);
        }

        protected abstract LogicalPlan doRule(LogicalPlan plan);
    }

    public static List<Attribute> maybeResolveAgainstList(
        UnresolvedAttribute u,
        Collection<Attribute> attrList,
        java.util.function.Function<Attribute, Attribute> fieldInspector
    ) {
        // first take into account the qualified version
        final String qualifier = u.qualifier();
        final String name = u.name();
        final boolean qualified = u.qualifier() != null;

        Predicate<Attribute> predicate = a -> {
            return qualified ? Objects.equals(qualifier, a.qualifiedName()) :
            // if the field is unqualified
            // first check the names directly
                (Objects.equals(name, a.name()))
                    // but also if the qualifier might not be quoted and if there's any ambiguity with nested fields
                    || Objects.equals(name, a.qualifiedName());

        };
        return maybeResolveAgainstList(predicate, () -> u, attrList, false, fieldInspector);
    }

    public static List<Attribute> maybeResolveAgainstList(
        Predicate<Attribute> matcher,
        Supplier<UnresolvedAttribute> unresolved,
        Collection<Attribute> attrList,
        boolean isPattern,
        java.util.function.Function<Attribute, Attribute> fieldInspector
    ) {
        List<Attribute> matches = new ArrayList<>();

        for (Attribute attribute : attrList) {
            if (attribute.synthetic() == false) {
                boolean match = matcher.test(attribute);
                if (match) {
                    matches.add(attribute);
                }
            }
        }

        if (matches.isEmpty()) {
            return matches;
        }

        UnresolvedAttribute ua = unresolved.get();
        // found exact match or multiple if pattern
        if (matches.size() == 1 || isPattern) {
            // NB: only add the location if the match is univocal; b/c otherwise adding the location will overwrite any preexisting one
            matches.replaceAll(e -> fieldInspector.apply(e));
            return matches;
        }

        // report ambiguity
        List<String> refs = matches.stream().sorted((a, b) -> {
            int lineDiff = a.sourceLocation().getLineNumber() - b.sourceLocation().getLineNumber();
            int colDiff = a.sourceLocation().getColumnNumber() - b.sourceLocation().getColumnNumber();
            return lineDiff != 0 ? lineDiff : (colDiff != 0 ? colDiff : a.qualifiedName().compareTo(b.qualifiedName()));
        })
            .map(
                a -> "line "
                    + a.sourceLocation().toString().substring(1)
                    + " ["
                    + (a.qualifier() != null ? "\"" + a.qualifier() + "\".\"" + a.name() + "\"" : a.name())
                    + "]"
            )
            .toList();

        throw new IllegalStateException("Reference [" + ua.qualifiedName() + "] is ambiguous; " + "matches any of " + refs);
    }
}
