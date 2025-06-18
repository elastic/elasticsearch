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
    private final List<Attribute> output;

    public Fork(Source source, List<LogicalPlan> children, List<Attribute> output) {
        super(source, children);
        if (children.size() < 2) {
            throw new IllegalArgumentException("requires more than two subqueries, got:" + children.size());
        }
        this.output = output;
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

                if (names.contains(attr.name()) == false && attr.name().equals(Analyzer.NO_FIELDS_NAME) == false) {
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
                } else if (unsupportedAttributes.contains(attrName) == true && attr.dataType() != DataType.UNSUPPORTED) {
                    unsupportedAttributes.remove(attrName);
                }
                names.add(attrName);
            }
        }

        return unsupportedAttributes;
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
