/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.promql;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PackDims;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TimeSeriesAggregate;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.promql.UnresolvedPromqlAttribute;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Names every {@code _timeseries} reference of a translated PromQL plan after the column it reads.
 * <p>
 * Every metadata column is the source's {@code _timeseries} with some labels left out: operators only ever remove labels
 * from metadata and promote any label they add or rewrite. The set of labels left out therefore identifies the column.
 * Translation states only each operator's own removals, relative to its input ({@link UnresolvedPromqlAttribute#unset()});
 * this rule adds up the removals from the source to each reference and names it after the total, e.g.
 * {@code _timeseries$dst$pod}, so that {@link PruneUnresolvedPromqlAttributeRefs} matches references to columns by name.
 * Leaving labels out requires typed {@code _timeseries}; keyword metadata is unsupported for non-empty unset.
 */
public final class PathTraceUnresolvedPromqlAttributeRefs extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return new Pass().follow(plan).plan();
    }

    private static final class Pass {
        /** Every metadata reference followed, by id: one reference read in several places follows once. */
        private final Map<NameId, UnresolvedPromqlAttribute> followed = new HashMap<>();

        /** Followed plan plus the unset of its time-series metadata (composed toward the source). */
        private record Followed(LogicalPlan plan, @Nullable Set<String> unset) {
            Followed(LogicalPlan plan, @Nullable UnresolvedPromqlAttribute timeSeriesMetadata) {
                this(plan, timeSeriesMetadata == null ? null : timeSeriesMetadata.unset());
            }
        }

        /**
         * Follows the metadata references of {@code node} and below. An operator's statement of its metadata output (an
         * Eval of a reference) goes once followed.
         */
        private Followed follow(LogicalPlan node) {
            List<Followed> children = node.children().stream().map(this::follow).toList();
            List<LogicalPlan> plans = children.stream().map(Followed::plan).toList();
            LogicalPlan rebuilt = plans.equals(node.children()) ? node : node.replaceChildren(plans);
            // Union intersects unset so a name-dropping branch cannot strip labels the other still holds.
            Followed input;
            if (rebuilt instanceof UnionAll && children.isEmpty() == false) {
                input = unionMetadata(rebuilt, children);
            } else {
                input = children.isEmpty() ? null : children.getFirst();
            }
            Set<String> below = input == null || input.unset() == null ? Set.of() : input.unset();
            LogicalPlan plan = rebuilt.transformExpressionsOnly(
                UnresolvedPromqlAttribute.class,
                ref -> ref.isMetadata() ? follow(ref, below) : ref
            );
            if (plan instanceof Eval eval) {
                var stated = eval.fields().stream().filter(field -> isMetadata(field.child())).toList();
                if (stated.isEmpty() == false) {
                    var fields = eval.fields().stream().filter(field -> isMetadata(field.child()) == false).toList();
                    LogicalPlan rest = fields.isEmpty() ? eval.child() : new Eval(eval.source(), eval.child(), fields);
                    return new Followed(rest, (UnresolvedPromqlAttribute) stated.getLast().child());
                }
            }
            return metadataOf(plan, input);
        }

        /** Intersect unset across branches; a metadata-less branch does not constrain the others. */
        private static Followed unionMetadata(LogicalPlan union, List<Followed> children) {
            Set<String> unset = null;
            for (Followed child : children) {
                if (child.unset() == null) {
                    continue;
                }
                if (unset == null) {
                    unset = new LinkedHashSet<>(child.unset());
                } else {
                    unset.retainAll(child.unset());
                }
            }
            return new Followed(union, unset);
        }

        /** Accumulate unset from source through ref. */
        private UnresolvedPromqlAttribute follow(UnresolvedPromqlAttribute ref, Set<String> below) {
            return followed.computeIfAbsent(ref.id(), id -> {
                var unset = new LinkedHashSet<>(below);
                unset.addAll(ref.unset());
                return ref.againstSource(unset);
            });
        }

        /**
         * What the metadata of {@code plan} leaves out of the source's series: an aggregate's is its key, or its input's when
         * it keeps one row per series; a projection's is the one it projects; any other operator passes its input's. Null
         * when the plan has none, its rows grouped by promoted labels only.
         */
        private static Followed metadataOf(LogicalPlan plan, @Nullable Followed input) {
            Followed passed = input == null ? new Followed(plan, Set.of()) : new Followed(plan, input.unset());
            return switch (plan) {
                case Aggregate aggregate -> {
                    UnresolvedPromqlAttribute key = metadataAmong(aggregate.groupings());
                    if (key == null && aggregate.child() instanceof PackDims pack) {
                        key = metadataAmong(pack.dims());
                    }
                    if (key != null) {
                        yield new Followed(plan, key);
                    }
                    boolean perSeries = aggregate instanceof TimeSeriesAggregate
                        && aggregate.aggregates().getFirst() instanceof Alias value
                        && PruneUnresolvedPromqlAttributeRefs.hasAcrossSeriesAggregate(value.child()) == false;
                    yield perSeries ? passed : new Followed(plan, (Set<String>) null);
                }
                case Project project -> new Followed(plan, metadataAmong(project.projections()));
                default -> passed;
            };
        }

        private static @Nullable UnresolvedPromqlAttribute metadataAmong(List<? extends Expression> expressions) {
            for (Expression expression : expressions) {
                Expression read = expression instanceof Alias alias ? alias.child() : expression;
                if (read instanceof UnresolvedPromqlAttribute ref && ref.isMetadata()) {
                    return ref;
                }
            }
            return null;
        }

        private static boolean isMetadata(Expression expression) {
            return expression instanceof UnresolvedPromqlAttribute ref && ref.isMetadata();
        }
    }
}
