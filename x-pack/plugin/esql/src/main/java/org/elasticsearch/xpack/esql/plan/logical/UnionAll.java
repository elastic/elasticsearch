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
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
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
    public UnionAll replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new UnionAll(source(), subPlans, output);
    }

    @Override
    public UnionAll refreshOutput() {
        return new UnionAll(source(), children(), refreshedOutput());
    }

    /**
     * Override of {@link Fork#pruneEmptyBranches(Predicate)} that returns a {@link UnionAll}
     * (rather than letting the base implementation produce whatever {@link #replaceChildren}
     * would). Mirrors the base behaviour otherwise: single-survivor wrappers are preserved
     * (callers that want to collapse to the lone child do so explicitly).
     */
    @Override
    public LogicalPlan pruneEmptyBranches(Predicate<LogicalPlan> isEmpty) {
        List<LogicalPlan> kept = new ArrayList<>(children().size());
        for (LogicalPlan child : children()) {
            if (isEmpty.test(child) == false) {
                kept.add(child);
            }
        }
        if (kept.size() == children().size()) {
            return this;
        }
        return new UnionAll(source(), kept, output());
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
        Fork.checkBranchCount(plan, failures);
        // Check that all UnionAll branches have compatible data types for each column
        if (plan instanceof UnionAll unionAll) {
            Map<String, DataType> outputTypes = unionAll.output().stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));

            unionAll.children().forEach(subPlan -> {
                for (Attribute attr : subPlan.output()) {
                    var expected = outputTypes.get(attr.name());

                    // UnionAll with unsupported types should not be allowed, otherwise runtime couldn't handle it
                    // Verifier checkUnresolvedAttributes should have caught it already, this check is similar to Fork
                    if (expected == null || expected == DataType.UNSUPPORTED) {
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
            unionAll.forEachDown(Fork.class, nested -> {
                if (unionAll == nested) {
                    return;
                }
                failures.add(nestedUnionAllFailure(nested));
            });
        }
    }

    /**
     * Builds the verification {@link Failure} for a {@link Fork}/{@link UnionAll} found nested below another {@link UnionAll} at
     * post-optimization.
     * <p>
     * A {@link ViewUnionAll} is never written by the user: it is added when a {@code FROM} pattern resolves, during view resolution, to
     * more than one source where at least one is a view — for example a wildcard matching a view together with a concrete index, a pattern
     * matching several views, or a view whose body references multiple sources. In every one of those cases the pattern (or view) expands
     * to a union of multiple sources, so the generic "Nested subqueries are not supported" wording is misleading - the query the user
     * wrote contains no nested subquery. We describe the real cause instead and quote the offending {@code FROM} clause (from
     * {@link #sourceText()}, truncated to {@link Node#TO_STRING_MAX_WIDTH}) so the user can locate it. A plain {@link UnionAll} is a
     * genuine user-written (or dataset-expanded) nested subquery, and a bare {@link Fork} is a {@code FORK} inside a subquery.
     */
    private static Failure nestedUnionAllFailure(LogicalPlan nested) {
        if (nested instanceof ViewUnionAll) {
            String sourceText = nested.sourceText();
            String source = sourceText.length() > Node.TO_STRING_MAX_WIDTH
                ? sourceText.substring(0, Node.TO_STRING_MAX_WIDTH) + "..."
                : sourceText;
            return Failure.fail(
                nested,
                "a pattern that expands to multiple sources, [{}], cannot be combined with subqueries"
                    + "; replace it with a single source in the FROM command",
                source
            );
        }
        if (nested instanceof UnionAll) {
            return Failure.fail(nested, "Nested subqueries are not supported");
        }
        return Failure.fail(nested, "FORK inside subquery is not supported");
    }
}
