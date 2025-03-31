/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * A Fork is a {@code Plan} with one child, but holds several logical subplans, e.g.
 * {@code FORK [WHERE content:"fox" ] [WHERE content:"dog"] }
 */
public class Fork extends UnaryPlan implements SurrogateLogicalPlan, PostAnalysisPlanVerificationAware {
    public static final String FORK_FIELD = "_fork";

    private final List<LogicalPlan> subPlans;
    List<Attribute> lazyOutput;

    public Fork(Source source, LogicalPlan child, List<LogicalPlan> subPlans) {
        super(source, child);
        if (subPlans.size() < 2) {
            throw new IllegalArgumentException("requires more than two subqueries, got:" + subPlans.size());
        }
        this.subPlans = subPlans;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public List<LogicalPlan> subPlans() {
        return subPlans;
    }

    @Override
    public LogicalPlan surrogate() {
        var newChildren = subPlans.stream().map(p -> surrogateSubPlan(child(), p)).toList();
        return new Merge(source(), newChildren);
    }

    @Override
    public boolean expressionsResolved() {
        if (child().resolved() && subPlans.stream().allMatch(LogicalPlan::resolved) == false) {
            return false;
        }

        // Here we check if all sub plans output the same column names.
        // If they don't then FORK was not resolved.
        Set<String> firstOutputNames = subPlans.getFirst().outputSet().names();
        Holder<Boolean> resolved = new Holder<>(true);
        subPlans.stream().skip(1).forEach(subPlan -> {
            Set<String> names = subPlan.outputSet().names();
            if (names.equals(firstOutputNames) == false) {
                resolved.set(false);
            }
        });

        return resolved.get();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Fork::new, child(), subPlans);
    }

    static List<LogicalPlan> replaceEmptyStubs(List<LogicalPlan> subQueries, LogicalPlan newChild) {
        List<Attribute> attributes = List.copyOf(newChild.output());
        return subQueries.stream().map(subquery -> {
            var newStub = new StubRelation(subquery.source(), attributes);
            return subquery.transformUp(StubRelation.class, stubRelation -> newStub);
        }).toList();
    }

    @Override
    public Fork replaceChild(LogicalPlan newChild) {
        var newSubQueries = replaceEmptyStubs(subPlans, newChild);
        return new Fork(source(), newChild, newSubQueries);
    }

    /**
     * Returns the surrogate subplan
     */
    private LogicalPlan surrogateSubPlan(LogicalPlan source, LogicalPlan subplan) {
        // Replaces the stubbed source with the actual source.
        LogicalPlan stubbed = subplan.transformUp(StubRelation.class, stubRelation -> source);

        // align the output
        List<Attribute> subplanOutput = new ArrayList<>();

        for (Attribute mainAttr : output()) {
            for (Attribute subAttr : subplan.output()) {
                if (mainAttr.name().equals(subAttr.name())) {
                    subplanOutput.add(subAttr);
                }
            }
        }

        return new Keep(source(), stubbed, subplanOutput);
    }

    public Fork replaceSubPlans(List<LogicalPlan> subPlans) {
        return new Fork(source(), child(), subPlans);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = lazyOutput();
        }
        return lazyOutput;
    }

    private List<Attribute> lazyOutput() {
        List<Attribute> output = new ArrayList<>();
        Set<String> names = new HashSet<>();

        for (var subPlan : subPlans) {
            for (var attr : subPlan.output()) {
                if (names.contains(attr.name()) == false) {
                    names.add(attr.name());
                    output.add(attr);
                }
            }
        }
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), subPlans);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        Fork other = (Fork) o;
        return Objects.equals(subPlans, other.subPlans);
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

        Map<String, DataType> outputTypes = fork.subPlans()
            .getFirst()
            .output()
            .stream()
            .collect(Collectors.toMap(Attribute::name, Attribute::dataType));

        fork.subPlans().stream().skip(1).forEach(subPlan -> {
            for (Attribute attr : subPlan.output()) {
                var actual = attr.dataType();
                var expected = outputTypes.get(attr.name());
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
