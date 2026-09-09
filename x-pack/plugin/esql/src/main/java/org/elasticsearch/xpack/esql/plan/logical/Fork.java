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
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;

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
        checkBranchCount(plan, failures);
        if (plan instanceof Fork == false) {
            return;
        }
        Fork fork = (Fork) plan;

        forEachMergePlanSkippingSubqueries(fork, other -> {
            if (other == fork) {
                return;
            }

            failures.add(
                Failure.fail(
                    other,
                    other instanceof UnionAll
                        ? "FORK after subquery is not supported"
                        : "Only a single FORK command is supported, but found multiple"
                )
            );
        });

        Map<String, Attribute> mergedOutput = fork.output().stream().collect(Collectors.toMap(Attribute::name, attr -> attr));

        fork.children().forEach(subPlan -> {
            for (Attribute attr : subPlan.output()) {
                var merged = mergedOutput.get(attr.name());

                // If the FORK output has an UNSUPPORTED data type, we know there is no conflict.
                // We only assign an UNSUPPORTED attribute in the FORK output when there exists no attribute with the
                // same name and supported data type in any of the FORK branches.
                //
                // Likewise, a branch that does not produce the column at all had it filled with nulls to line the branches up.
                // Those rows carry no values, so there is nothing for a sibling's declarations to disagree with.
                if (merged == null || merged.dataType() == DataType.UNSUPPORTED || producesOnlyNull(subPlan, attr)) {
                    continue;
                }

                var conflict = Expressions.checkForMergeConflict(attr, merged);
                if (conflict != null) {
                    failures.add(
                        Failure.fail(
                            attr,
                            "Column [{}] has conflicting {} in FORK branches: [{}] and [{}]",
                            attr.name(),
                            conflict.property(),
                            conflict.branchValue(),
                            conflict.mergedValue()
                        )
                    );
                }
            }
        });
    }

    /**
     * Whether {@code attr}, a column of {@code branch}'s output, holds nothing but nulls. Branch alignment fills a
     * column a branch lacks this way, so that every branch outputs the same names; a column written as an explicit
     * {@code EVAL x = null} is indistinguishable and equally empty, so both are treated alike.
     * <p>
     * Matched on the attribute's id rather than its name: a branch may assign the name more than once, and only the
     * assignment this attribute came from decides what the branch outputs. Matching by name would let an assignment
     * a later one shadows answer for the column.
     */
    private static boolean producesOnlyNull(LogicalPlan branch, Attribute attr) {
        Holder<Boolean> onlyNull = new Holder<>(false);
        branch.forEachDown(Eval.class, eval -> {
            for (Alias field : eval.fields()) {
                if (field.id().equals(attr.id())) {
                    onlyNull.set(Expressions.isGuaranteedNull(field.child()));
                }
            }
        });
        return onlyNull.get();
    }
}
