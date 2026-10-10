/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
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
        if (plan instanceof Fork == false) {
            return;
        }
        Fork fork = (Fork) plan;

        checkForUnseparatedFork(fork, false, failures);

        Map<String, Attribute> mergedOutput = fork.output().stream().collect(Collectors.toMap(Attribute::name, attr -> attr));

        fork.children().forEach(subPlan -> {
            Predicate<Attribute> onlyNull = producesOnlyNull(subPlan);
            for (Attribute attr : subPlan.output()) {
                var merged = mergedOutput.get(attr.name());

                // If the FORK output has an UNSUPPORTED data type, we know there is no conflict.
                // We only assign an UNSUPPORTED attribute in the FORK output when there exists no attribute with the
                // same name and supported data type in any of the FORK branches.
                //
                // Likewise, a branch that does not produce the column at all had it filled with nulls to line the branches up.
                // Those rows carry no values, so there is nothing for a sibling's declarations to disagree with.
                //
                // Union-type resolution can also introduce synthetic conversion attributes after this FORK's output was
                // resolved. They are carried through the branch projections so the conversion can be extracted, but are
                // intentionally absent from the user-visible FORK output and removed by the union-types cleanup rule.
                if (merged == null || merged.dataType() == DataType.UNSUPPORTED || onlyNull.test(attr)) {
                    continue;
                }

                var conflict = checkForMergeConflict(attr, merged);
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
     * Rejects a {@link Fork} whose direct children exceed {@code max_branch_count_per_merge} cluster setting or pragma.
     */
    @Override
    void checkBranchCount(LogicalPlan plan, Failures failures, QueryPragmas pragmas, EsqlFlags flags) {
        super.checkBranchCount(plan, failures, pragmas, flags);
        int maxBranches = pragmas.maxBranchCountPerMerge(flags.maxBranchCountPerMerge());
        String limitSource = pragmas.maxBranchCountPerMergeLimitSource(EsqlFlags.ESQL_MAX_BRANCH_COUNT_PER_MERGE.getKey());
        if (plan.children().size() > maxBranches) {
            String sourceText = plan.sourceText();
            String errorMessage = sourceText.length() > Node.TO_STRING_MAX_WIDTH
                ? sourceText.substring(0, Node.TO_STRING_MAX_WIDTH) + "..."
                : sourceText;
            failures.add(
                Failure.fail(
                    plan,
                    "{} resolved to {} branches, exceeding the limit of {} set by the {}. Reduce the number of branches,"
                        + " split the query, or change the setting",
                    errorMessage,
                    plan.children().size(),
                    maxBranches,
                    limitSource
                )
            );
        }
    }

    /**
     * Whether this pipeline segment contains a {@link Fork} that is not already under a {@link MergePlan}. FORKs inside a {@link UnionAll},
     * {@link ViewUnionAll}, or another {@link Fork} are a different merge segment and do not count. The right side of an
     * {@link AbstractSubqueryJoin} is an independently executed query and is also skipped. Used to keep a one-child {@link UnionAll} or
     * {@link ViewUnionAll} around a single {@code FROM (...)} or inlined view so {@link #checkForUnseparatedFork} can treat that scope as a
     * boundary rather than consecutive FORKs on one pipeline.
     */
    public static boolean containsFork(LogicalPlan plan) {
        if (plan instanceof Fork) {
            return true;
        }
        if (plan instanceof MergePlan) {
            return false;
        }
        if (plan instanceof AbstractSubqueryJoin join) {
            return containsFork(join.left());
        }
        for (LogicalPlan child : plan.children()) {
            if (containsFork(child)) {
                return true;
            }
        }
        return false;
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

    /**
     * A property that two same-named attributes disagree on, and so cannot be merged into one output column.
     *
     * @param property plural name of the property, for a user-facing message
     * @param branchValue the value on the attribute being merged in
     * @param mergedValue the value the merged output carries
     */
    record MergeConflict(String property, String branchValue, String mergedValue) {}

    /**
     * Why {@code branch} cannot be merged into {@code merged}, or {@code null} when it can.
     * <p>
     * {@link Expressions#toReferenceAttributesPreservingIds} keeps one attribute per column name, so a branch
     * disagreeing on any property that changes how the column's values are read would have its rows read as if it
     * had declared the merged one. {@link #checkFork} reports the conflict; this decides what counts as one, so
     * that adding a text-column property does not scatter the comparison through the check itself.
     */
    @Nullable
    static MergeConflict checkForMergeConflict(Attribute branch, Attribute merged) {
        if (branch.dataType() != merged.dataType()) {
            return new MergeConflict("data types", String.valueOf(branch.dataType()), String.valueOf(merged.dataType()));
        }
        // Declaring nothing is declaring the standard analyzer, so it still disagrees with a sibling that names a
        // different one: the merged column can carry only one, and the other branch's values would be analyzed with
        // an analyzer they never declared. A column with no values to analyze - one branch alignment filled with
        // nulls - is skipped by the caller rather than weakening the comparison here.
        String branchAnalyzer = analyzerOrStandard(branch);
        String mergedAnalyzer = analyzerOrStandard(merged);
        if (branchAnalyzer.equals(mergedAnalyzer) == false) {
            return new MergeConflict("values analyzers", branchAnalyzer, mergedAnalyzer);
        }
        return null;
    }

    private static String analyzerOrStandard(Attribute attr) {
        String declared = AnalyzedTextExpression.valuesAnalyzerOf(attr);
        return declared == null ? AnalyzedTextExpression.STANDARD_ANALYZER : declared;
    }

    /**
     * Whether a column of {@code branch}'s output holds nothing but nulls. Branch alignment fills a column a branch
     * lacks this way, so that every branch outputs the same names; a column written as an explicit
     * {@code EVAL x = null} is indistinguishable and equally empty, so both are treated alike.
     * <p>
     * Matched on the attribute's id rather than its name: a branch may assign the name more than once, and only the
     * assignment this attribute came from decides what the branch outputs. Matching by name would let an assignment
     * a later one shadows answer for the column.
     * <p>
     * Walks {@code branch} once, so build one predicate per branch and test every column of a wide branch against it.
     */
    public static Predicate<Attribute> producesOnlyNull(LogicalPlan branch) {
        Map<NameId, Boolean> onlyNull = new HashMap<>();
        branch.forEachDown(Eval.class, eval -> {
            for (Alias field : eval.fields()) {
                onlyNull.put(field.id(), Expressions.isGuaranteedNull(field.child()));
            }
        });
        return attr -> onlyNull.getOrDefault(attr.id(), false);
    }
}
