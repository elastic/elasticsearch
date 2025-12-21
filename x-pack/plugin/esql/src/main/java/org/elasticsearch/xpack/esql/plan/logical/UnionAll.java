/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class UnionAll extends Fork implements PostOptimizationPlanVerificationAware {

    public UnionAll(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children, output);
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new UnionAll(source(), newChildren, output());
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, UnionAll::new, children(), output());
    }

    @Override
    public UnionAll replaceSubPlans(List<LogicalPlan> subPlans) {
        return new UnionAll(source(), subPlans, output());
    }

    @Override
    public Fork replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new UnionAll(source(), subPlans, output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(UnionAll.class, children());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnionAll other = (UnionAll) o;

        return Objects.equals(children(), other.children());
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return UnionAll::checkUnionAll;
    }

    private static void checkUnionAll(LogicalPlan plan, Failures failures) {
        // Check that all UnionAll branches have compatible data types for each column
        if (plan instanceof UnionAll unionAll) {
            Map<String, DataType> outputTypes = unionAll.output().stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));

            unionAll.children().forEach(subPlan -> {
                for (Attribute attr : subPlan.output()) {
                    var expected = outputTypes.get(attr.name());

                    // UnionAll with unsupported types should not be allowed, otherwise runtime couldn't handle it
                    // Verifier checkUnresolvedAttributes should have caught it already, this check is similar to Fork
                    if (expected == DataType.UNSUPPORTED) {
                        continue;
                    }

                    var actual = attr.dataType();
                    if (actual != expected) {
                        failures.add(
                            Failure.fail(
                                attr,
                                "Column [{}] has conflicting data types in subqueries: [{}] and [{}]",
                                attr.name(),
                                actual,
                                expected
                            )
                        );
                    }
                }
            });
        }

        // Check InlineStats is not in the parent plan of UnionAll, as Limit is not allowed in the child plans of InlineStats.
        // Refer to Verifier.checkLimitBeforeInlineStats for details, provide a clear error message for subqueries here.
        if (plan instanceof InlineStats inlineStats) {
            Holder<UnionAll> inlineStatsDescendantUnionAll = new Holder<>();
            inlineStats.forEachDownMayReturnEarly((p, breakEarly) -> {
                if (p instanceof UnionAll unionAll) {
                    inlineStatsDescendantUnionAll.set(unionAll);
                    breakEarly.set(true);
                    return;
                }
            });

            if (inlineStatsDescendantUnionAll.get() != null) {
                failures.add(
                    Failure.fail(
                        inlineStatsDescendantUnionAll.get(),
                        "INLINE STATS after subquery is not supported, "
                            + "as INLINE STATS cannot be used after an explicit or implicit LIMIT command"
                    )
                );
            }
        }
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postOptimizationPlanVerification() {
        return UnionAll::checkNestedUnionAlls;
    }

    /**
     * Defer the check for nested UnionAlls until after logical planner as some of the nested subqueries can be flattened
     * by logical planner in the future.
     */
    private static void checkNestedUnionAlls(LogicalPlan logicalPlan, Failures failures) {
        if (logicalPlan instanceof UnionAll unionAll) {
            unionAll.forEachDown(Fork.class, otherForkOrUnionAll -> {
                if (unionAll == otherForkOrUnionAll) {
                    return;
                }
                failures.add(
                    Failure.fail(
                        otherForkOrUnionAll,
                        otherForkOrUnionAll instanceof UnionAll
                            ? "Nested subqueries are not supported"
                            : "FORK inside subquery is not supported"
                    )
                );
            });
        }
    }
}
