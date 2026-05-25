/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.analysis.Analyzer.NO_FIELDS;
import static org.elasticsearch.xpack.esql.core.expression.Expressions.toReferenceAttributesPreservingIds;

/**
 * A Fork is a n-ary {@code Plan} where each child is a sub plan, e.g.
 * {@code FORK [WHERE content:"fox" ] [WHERE content:"dog"] }
 */
public class Fork extends LogicalPlan implements PostAnalysisPlanVerificationAware, TelemetryAware, ExecutesOn.Coordinator {

    public static final String FORK_FIELD = "_fork";
    public static final int MAX_BRANCHES = 8;
    private final List<Attribute> output;

    public Fork(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children);
        this.output = output;
    }

    /**
     * Branch-count predicate shared by {@link Fork}'s constructor and any caller that wants to fail
     * earlier with a more user-facing message. Returns {@code true} if {@code count} would exceed the
     * branch cap. Centralizes the comparison so the cap can move in one place.
     */
    public static boolean exceedsMaxBranches(int count) {
        return count > MAX_BRANCHES;
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new Fork(source(), newChildren, output);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public boolean expressionsResolved() {
        if (children().stream().allMatch(LogicalPlan::resolved) == false) {
            return false;
        }

        if (children().stream()
            .anyMatch(p -> p.outputSet().names().contains(Analyzer.NO_FIELDS_NAME) || output.size() != p.output().size())) {
            return false;
        }

        // Here we check if all sub plans output the same column names.
        // If they don't then FORK was not resolved.
        List<String> firstOutputNames = children().getFirst().output().stream().map(Attribute::name).toList();
        Holder<Boolean> resolved = new Holder<>(true);
        children().stream().skip(1).forEach(subPlan -> {
            List<String> names = subPlan.output().stream().map(Attribute::name).toList();
            if (names.equals(firstOutputNames) == false) {
                resolved.set(false);
            }
        });

        return resolved.get();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Fork::new, children(), output);
    }

    public Fork replaceSubPlans(List<LogicalPlan> subPlans) {
        return new Fork(source(), subPlans, output);
    }

    public Fork replaceSubPlansAndOutput(List<LogicalPlan> subPlans, List<Attribute> output) {
        return new Fork(source(), subPlans, output);
    }

    public Fork refreshOutput() {
        return new Fork(source(), children(), refreshedOutput());
    }

    /**
     * Drop branches whose root the {@code isEmpty} predicate considers empty. Each
     * {@link Fork} subclass with structural invariants beyond the positional children list
     * (notably {@link ViewUnionAll}, which carries a named-subqueries map) overrides this method
     * to preserve those invariants.
     * <p>
     * Behaviour:
     * <ul>
     *   <li>nothing pruned → returns {@code this} (cheap no-op);</li>
     *   <li>at least one branch pruned → returns {@code replaceChildren(survivors)} with the
     *       remaining children — this includes the all-empty case, which produces a Fork (or
     *       subclass) with zero children. The caller is expected either to short-circuit the
     *       all-empty case before calling (e.g. {@code PruneEmptyForkBranches} replaces with
     *       a {@code LocalRelation} when every branch reduces to empty) or to let the
     *       analyzer's verifier surface the empty-Fork state via {@link #checkBranchCount}.</li>
     * </ul>
     * Single-survivor collapse semantics — a {@link UnionAll}/{@link ViewUnionAll} with one
     * branch left is equivalent to that branch — are not part of this primitive; callers that
     * want that collapse do it explicitly (see {@code ViewCompaction.stripViewShadowRelations}).
     * A {@link Fork} with a single branch is still a {@link Fork} per FORK syntax.
     */
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
        return replaceChildren(kept);
    }

    protected List<Attribute> refreshedOutput() {
        return toReferenceAttributesPreservingIds(outputUnion(children()), this.output());
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public static List<Attribute> outputUnion(List<LogicalPlan> subplans) {
        List<Attribute> output = new ArrayList<>();
        Set<String> names = new HashSet<>();
        // these are attribute names we know should have an UNSUPPORTED data type in the FORK output
        Set<String> unsupportedAttributesNames = outputUnsupportedAttributeNames(subplans);

        for (var subPlan : subplans) {
            for (var attr : subPlan.output()) {
                // When we have multiple attributes with the same name, the ones that have a supported data type take priority.
                // We only add an attribute with an unsupported data type if we know that in the output of the rest of the FORK branches
                // there exists no attribute with the same name and with a supported data type.
                if (attr.dataType() == DataType.UNSUPPORTED && unsupportedAttributesNames.contains(attr.name()) == false) {
                    continue;
                }

                if (names.contains(attr.name()) == false && attr != NO_FIELDS.getFirst()) {
                    names.add(attr.name());
                    output.add(attr);
                }
            }
        }
        return output;
    }

    /**
     * Returns a list of attribute names that will need to have the @{code UNSUPPORTED} data type in FORK output.
     * These are attributes that are either {@code UNSUPPORTED} or missing in each FORK branch.
     * If two branches have the same attribute name, but only in one of them the data type is {@code UNSUPPORTED}, this constitutes
     * data type conflict, and so this attribute name will not be returned by this function.
     * Data type conflicts are later on checked in {@code postAnalysisPlanVerification}.
     */
    public static Set<String> outputUnsupportedAttributeNames(List<LogicalPlan> subplans) {
        Set<String> unsupportedAttributes = new HashSet<>();
        Set<String> names = new HashSet<>();

        for (var subPlan : subplans) {
            for (var attr : subPlan.output()) {
                var attrName = attr.name();
                if (unsupportedAttributes.contains(attrName) == false
                    && attr.dataType() == DataType.UNSUPPORTED
                    && names.contains(attrName) == false) {
                    unsupportedAttributes.add(attrName);
                } else if (unsupportedAttributes.contains(attrName) && attr.dataType() != DataType.UNSUPPORTED) {
                    unsupportedAttributes.remove(attrName);
                }
                names.add(attrName);
            }
        }

        return unsupportedAttributes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Fork.class, output, children());
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

        return Objects.equals(output, other.output) && Objects.equals(children(), other.children());
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return Fork::checkFork;
    }

    /**
     * Branch-count bounds shared by all {@link Fork} subclasses (Fork, UnionAll, ViewUnionAll).
     * Lives at post-analysis verification rather than the Fork constructor so that compaction
     * passes (e.g. ViewCompaction) get a chance to reduce the count first. Called from both
     * {@code Fork::checkFork} and {@code UnionAll::checkUnionAll} since each subclass dispatches
     * to its own {@link #postAnalysisPlanVerification()} override.
     * <p>
     * The lower bound (≥ 1 branch) catches invalid plans where {@link #pruneEmptyBranches}
     * removed every branch — e.g. a CCS subquery whose {@code IndexResolution} came back
     * {@code EMPTY_SUBQUERY} for every sibling. The {@code PruneEmptyForkBranches} optimizer
     * rule short-circuits this case to a {@code LocalRelation}; rules that don't (the analyzer's
     * {@code PruneEmptyUnionAllBranch}, {@code ViewCompaction.stripViewShadowRelations}) rely on
     * this check to surface the bad state with a clear message rather than letting an empty
     * {@code Fork}/{@code UnionAll} propagate silently.
     */
    static void checkBranchCount(LogicalPlan plan, Failures failures) {
        if (plan instanceof Fork fork) {
            int size = fork.children().size();
            if (exceedsMaxBranches(size)) {
                failures.add(Failure.fail(fork, "FORK supports up to {} branches, got: {}", MAX_BRANCHES, size));
            } else if (size == 0) {
                failures.add(Failure.fail(fork, "{} requires at least one branch", fork.getClass().getSimpleName()));
            }
        }
    }

    private static void checkFork(LogicalPlan plan, Failures failures) {
        checkBranchCount(plan, failures);
        if (plan instanceof Fork == false || plan instanceof UnionAll) {
            return;
        }
        Fork fork = (Fork) plan;

        fork.forEachDown(Fork.class, otherFork -> {
            if (fork == otherFork) {
                return;
            }

            failures.add(
                Failure.fail(
                    otherFork,
                    otherFork instanceof UnionAll
                        ? "FORK after subquery is not supported"
                        : "Only a single FORK command is supported, but found multiple"
                )
            );
        });

        Map<String, DataType> outputTypes = fork.output().stream().collect(Collectors.toMap(Attribute::name, Attribute::dataType));

        fork.children().forEach(subPlan -> {
            for (Attribute attr : subPlan.output()) {
                var expected = outputTypes.get(attr.name());

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
}
