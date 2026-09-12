/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * The {@code FORK} command: an n-ary {@link MergePlan} where each child is a sub plan, e.g.
 * {@code FORK [WHERE content:"fox" ] [WHERE content:"dog"] }
 */
public final class Fork extends MergePlan implements TelemetryAware {

    public static final String FORK_FIELD = "_fork";

    public Fork(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children, output);
    }

    @Override
    public Fork replaceChildren(List<LogicalPlan> newChildren) {
        return new Fork(source(), newChildren, output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Fork::new, children(), output());
    }

    @Override
    public Fork replaceSubPlans(List<LogicalPlan> subPlans) {
        return new Fork(source(), subPlans, output());
    }

    @Override
    public Fork replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new Fork(source(), subPlans, output);
    }

    @Override
    public Fork refreshOutput() {
        return new Fork(source(), children(), refreshedOutput());
    }

    @Override
    public int hashCode() {
        return Objects.hash(Fork.class, output(), children());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Fork other = (Fork) o;

        return Objects.equals(output(), other.output()) && Objects.equals(children(), other.children());
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return Fork::checkFork;
    }

    private static void checkFork(LogicalPlan plan, Failures failures) {
        checkNonEmpty(plan, failures);
        if (plan instanceof Fork == false) {
            return;
        }
        Fork fork = (Fork) plan;
        checkMaxBranches(fork, failures);

        checkForUnseparatedFork(fork, false, failures);

        Map<String, DataType> outputTypes = fork.output().stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));

        fork.children().forEach(subPlan -> {
            for (Attribute attr : subPlan.output()) {
                var expected = outputTypes.get(attr.name());

                // Union-type resolution can introduce synthetic conversion attributes after this FORK's output was resolved. They are
                // carried through the branch projections so the conversion can be extracted, but are intentionally absent from the
                // user-visible FORK output and removed by the union-types cleanup rule.
                if (expected == null && attr.synthetic()) {
                    continue;
                }

                // If the FORK output has an UNSUPPORTED data type, we know there is no conflict.
                // We only assign an UNSUPPORTED attribute in the FORK output when there exists no attribute with the
                // same name and supported data type in any of the FORK branches.
                if (expected == DataType.UNSUPPORTED) {
                    continue;
                }

                var actual = attr.dataType();
                if (actual != expected) {
                    failures.add(
                        Failure.fail(
                            attr,
                            "Column [{}] has conflicting data types in FORK branches: [{}] and [{}]",
                            attr.name(),
                            actual,
                            expected
                        )
                    );
                }
            }
        });
    }

    /**
     * The {@code FORK} command's per-node branch cap. Lives at post-analysis verification rather than
     * the constructor so that compaction passes get a chance to reduce the count first. {@link UnionAll}
     * and {@link ViewUnionAll} are not subject to this cap; they are bounded by the query-wide
     * {@code max_query_branches} / {@code max_query_branch_levels} pragmas.
     */
    private static void checkMaxBranches(Fork fork, Failures failures) {
        int branches = fork.children().size();
        if (exceedsMaxBranches(branches)) {
            failures.add(Failure.fail(fork, "FORK supports up to {} branches, got: {}", MAX_BRANCHES, branches));
        }
    }

    /**
     * Rejects two user-written FORKs on the same uninterrupted pipeline path. A {@link UnionAll} is a real merge boundary, whether it
     * came from user subqueries, a view, an external dataset, or federation, so each of its branches starts a new FORK segment. The right
     * side of an {@link AbstractSubqueryJoin} is an independently executed query scope and is verified by its own FORK node.
     */
    private static void checkForUnseparatedFork(LogicalPlan plan, boolean forkSeen, Failures failures) {
        if (plan instanceof UnionAll unionAll) {
            for (LogicalPlan child : unionAll.children()) {
                checkForUnseparatedFork(child, false, failures);
            }
            return;
        }
        if (plan instanceof AbstractSubqueryJoin join) {
            checkForUnseparatedFork(join.left(), forkSeen, failures);
            return;
        }
        boolean seen = forkSeen;
        if (plan.getClass() == Fork.class) {
            if (forkSeen) {
                failures.add(Failure.fail(plan, "Only a single FORK command is supported, but found multiple"));
                return;
            }
            seen = true;
        }
        for (LogicalPlan child : plan.children()) {
            checkForUnseparatedFork(child, seen, failures);
        }
    }
}
