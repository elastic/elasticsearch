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
import java.util.stream.Collectors;

/**
 * A Fork is a n-ary {@code Plan} where each child is a sub plan, e.g.
 * {@code FORK [WHERE content:"fox" ] [WHERE content:"dog"] }
 */
public class Fork extends LogicalPlan implements PostAnalysisPlanVerificationAware {

    public static final String FORK_FIELD = "_fork";
    List<Attribute> lazyOutput;

    public Fork(Source source, List<LogicalPlan> children) {
        super(source, children);
        if (children.size() < 2) {
            throw new IllegalArgumentException("requires more than two subqueries, got:" + children.size());
        }
    }

    @Override
    public LogicalPlan replaceChildren(List<LogicalPlan> newChildren) {
        return new Fork(source(), newChildren);
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

        if (children().stream().anyMatch(p -> p.outputSet().names().contains(Analyzer.NO_FIELDS_NAME))) {
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
        return NodeInfo.create(this, Fork::new, children());
    }

    public Fork replaceSubPlans(List<LogicalPlan> subPlans) {
        return new Fork(source(), subPlans);
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

        for (var subPlan : children()) {
            for (var attr : subPlan.output()) {
                if (names.contains(attr.name()) == false && attr.name().equals(Analyzer.NO_FIELDS_NAME) == false) {
                    names.add(attr.name());
                    output.add(attr);
                }
            }
        }
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(Fork.class, children());
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

        return Objects.equals(children(), other.children());
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

        fork.forEachDown(Fork.class, otherFork -> {
            if (fork == otherFork) {
                return;
            }

            failures.add(Failure.fail(otherFork, "Only a single FORK command is allowed, but found multiple"));
        });

        Map<String, DataType> outputTypes = fork.children()
            .getFirst()
            .output()
            .stream()
            .collect(Collectors.toMap(Attribute::name, Attribute::dataType));

        fork.children().stream().skip(1).forEach(subPlan -> {
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
