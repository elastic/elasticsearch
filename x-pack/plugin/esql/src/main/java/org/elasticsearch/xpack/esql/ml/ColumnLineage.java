/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ml;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedStar;
import org.elasticsearch.xpack.esql.expression.UnresolvedNamePattern;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Drop;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Keep;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Rename;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

import java.util.List;

/** Traces a column of the parsed, unresolved plan down to the expression or source field that defines it. */
final class ColumnLineage {

    private ColumnLineage() {}

    /** A position in the plan: {@code plan}, of which only the first {@code visibleAliases} aliases are visible if it is an EVAL. */
    record Scope(LogicalPlan plan, int visibleAliases) {
        static Scope below(LogicalPlan plan) {
            return new Scope(plan, Integer.MAX_VALUE);
        }
    }

    sealed interface Origin permits SourceField, Computed, Unknown {}

    /** The column is the source field {@code name}: nothing between it and the source command defines it. */
    record SourceField(String name) implements Origin {}

    /** The column is computed by {@code expression}, whose own columns are visible at {@code scope}. */
    record Computed(Expression expression, Scope scope) implements Origin {}

    /** The definition cannot be traced: a merge, a removed column, or a boundary the caller does not cross. */
    enum Unknown implements Origin {
        INSTANCE
    }

    /** The source field {@code column} carries in the output of {@code plan}, or {@code null} if it is computed or not traceable. */
    @Nullable
    static String sourceFieldOf(LogicalPlan plan, String column) {
        return origin(column, Scope.below(plan), true) instanceof SourceField field ? field.name() : null;
    }

    /**
     * Follows {@code column} down from {@code scope} through EVAL, RENAME, KEEP, DROP and STATS, past attribute-to-attribute
     * aliases, to the expression or source field defining it.
     *
     * @param crossJoins whether to continue through the left side of joins and through INLINE STATS
     */
    static Origin origin(String column, Scope scope, boolean crossJoins) {
        LogicalPlan node = scope.plan();
        int visibleAliases = scope.visibleAliases();
        while (true) {
            switch (node) {
                case Eval eval -> {
                    List<Alias> fields = eval.fields();
                    // The last alias of a name wins, and a later alias may refer to an earlier one of the same EVAL.
                    for (int i = Math.min(visibleAliases, fields.size()) - 1; i >= 0; i--) {
                        Alias alias = fields.get(i);
                        if (alias.name().equals(column)) {
                            if (alias.child() instanceof UnresolvedAttribute attribute) {
                                column = attribute.name();
                            } else {
                                return new Computed(alias.child(), new Scope(eval, i));
                            }
                        }
                    }
                    node = eval.child();
                }
                case Rename rename -> {
                    for (Alias renaming : rename.renamings().reversed()) {
                        if (renaming.name().equals(column) && renaming.child() instanceof UnresolvedAttribute attribute) {
                            column = attribute.name();
                        }
                    }
                    node = rename.child();
                }
                case Keep keep -> {
                    if (matchesAny(keep.projections(), column) == false) {
                        return Unknown.INSTANCE;
                    }
                    node = keep.child();
                }
                case Drop drop -> {
                    if (matchesAny(drop.removals(), column)) {
                        return Unknown.INSTANCE;
                    }
                    node = drop.child();
                }
                case InlineStats inlineStats -> {
                    if (crossJoins == false || inlineStats.child() instanceof Aggregate == false) {
                        return Unknown.INSTANCE;
                    }
                    Aggregate aggregate = (Aggregate) inlineStats.child();
                    // INLINE STATS keeps every input column and overwrites only those it defines.
                    Expression definition = definition(aggregate, column);
                    if (definition != null && definition instanceof UnresolvedAttribute == false) {
                        return new Computed(definition, Scope.below(aggregate.child()));
                    }
                    column = definition == null ? column : ((UnresolvedAttribute) definition).name();
                    node = aggregate.child();
                }
                case Aggregate aggregate -> {
                    Expression definition = definition(aggregate, column);
                    if (definition == null) {
                        return Unknown.INSTANCE;
                    }
                    if (definition instanceof UnresolvedAttribute == false) {
                        return new Computed(definition, Scope.below(aggregate.child()));
                    }
                    column = ((UnresolvedAttribute) definition).name();
                    node = aggregate.child();
                }
                case UnaryPlan unary -> node = unary.child();
                case Join join -> {
                    if (crossJoins == false) {
                        return Unknown.INSTANCE;
                    }
                    node = join.left();
                }
                case UnresolvedRelation ignored -> {
                    return new SourceField(column);
                }
                // Merges (FORK, FROM with subqueries) and other sources such as ROW.
                default -> {
                    return Unknown.INSTANCE;
                }
            }
            visibleAliases = Integer.MAX_VALUE;
        }
    }

    /** The unaliased expression of the STATS grouping or aggregate named {@code column}, or {@code null} if there is none. */
    @Nullable
    private static Expression definition(Aggregate aggregate, String column) {
        for (Expression grouping : aggregate.groupings()) {
            if (grouping instanceof NamedExpression named && named.name().equals(column)) {
                return Alias.unwrap(grouping);
            }
        }
        for (NamedExpression aggregateExpression : aggregate.aggregates()) {
            if (aggregateExpression instanceof Alias alias && alias.name().equals(column)) {
                return alias.child();
            }
        }
        return null;
    }

    /** Whether a KEEP or DROP name, pattern or {@code *} in {@code patterns} matches {@code column}. */
    static boolean matchesAny(List<? extends NamedExpression> patterns, String column) {
        return patterns.stream().anyMatch(pattern -> switch (pattern) {
            case UnresolvedStar ignored -> true;
            case UnresolvedNamePattern namePattern -> namePattern.match(column);
            case UnresolvedAttribute attribute -> attribute.name().equals(column);
            default -> false;
        });
    }
}
