/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.operator;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.BinaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public abstract sealed class VectorBinaryOperator extends BinaryPlan implements PromqlPlan permits VectorBinarySet, VectorBinaryComparison,
    VectorBinaryArithmetic {

    private final VectorMatch match;
    private final boolean dropMetricName;
    private final BinaryOp binaryOp;
    private List<Attribute> output;

    /**
     * Underlying binary operation (e.g. +, -, *, /, etc.) being performed
     * on the actual values of the vectors.
     */
    public interface BinaryOp {
        String name();

        ScalarFunctionFactory asFunction();
    }

    public interface ScalarFunctionFactory {
        Function create(Source source, Expression left, Expression right, Configuration configuration);
    }

    protected VectorBinaryOperator(
        Source source,
        LogicalPlan left,
        LogicalPlan right,
        VectorMatch match,
        boolean dropMetricName,
        BinaryOp binaryOp
    ) {
        super(source, left, right);
        this.match = match;
        this.dropMetricName = dropMetricName;
        this.binaryOp = binaryOp;
    }

    public VectorMatch match() {
        return match;
    }

    public boolean dropMetricName() {
        return dropMetricName;
    }

    public BinaryOp binaryOp() {
        return binaryOp;
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = computeOutputAttributes();
        }
        return output;
    }

    private List<Attribute> computeOutputAttributes() {
        // Between an instant vector and a scalar,
        // the operator is applied to the value of every data sample in the vector.
        // Therefore, we're returning any grouping attributes (like those created for by (...) and _timeseries) from the vector.
        // If both the left and right are a scalar, this works, too
        // as both outputs will be empty (scalars don't have any grouping attributes).
        if (PromqlPlan.returnsScalar(left())) {
            return right().output();
        }
        if (PromqlPlan.returnsScalar(right())) {
            return left().output();
        }
        Set<String> outputLabels;
        List<Attribute> leftAttrs = left().output();
        List<Attribute> rightAttrs = right().output();
        Set<String> leftLabels = extractLabelNames(leftAttrs);
        Set<String> rightLabels = extractLabelNames(rightAttrs);
        if (matchFilter() == VectorMatch.Filter.ON) {
            outputLabels = new HashSet<>(match.filterLabels());
        } else if (matchFilter() == VectorMatch.Filter.IGNORING) {
            outputLabels = new HashSet<>(leftLabels);
            outputLabels.addAll(rightLabels);
            outputLabels.removeAll(match.filterLabels());
        } else if (leftLabels.equals(rightLabels)) {
            return leftAttrs;
        } else {
            // If there's a mismatch in labels that is not handled by ON or IGNORING,
            // the query result is an empty set by definition as non-matching series are dropped.
            return List.of();
        }

        if (dropMetricName) {
            outputLabels.remove(LabelMatcher.NAME);
        }

        List<Attribute> result = new ArrayList<>();
        for (String label : outputLabels) {
            Attribute attr = findAttribute(label, leftAttrs, rightAttrs);
            if (attr != null) {
                result.add(attr);
            }
        }

        return result;
    }

    private VectorMatch.Filter matchFilter() {
        if (match != null) {
            return match.filter();
        }
        return VectorMatch.Filter.NONE;
    }

    private Set<String> extractLabelNames(List<Attribute> attrs) {
        Set<String> labels = new HashSet<>();
        for (Attribute attr : attrs) {
            String name = attr.name();
            if (name.equals("value") == false) {
                labels.add(name);
            }
        }
        return labels;
    }

    private Attribute findAttribute(String name, List<Attribute> left, List<Attribute> right) {
        for (Attribute attr : left) {
            if (attr.name().equals(name)) {
                return attr;
            }
        }
        for (Attribute attr : right) {
            if (attr.name().equals(name)) {
                return attr;
            }
        }
        return null;
    }

    @Override
    public abstract VectorBinaryOperator replaceChildren(LogicalPlan newLeft, LogicalPlan newRight);

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o)) {
            VectorBinaryOperator that = (VectorBinaryOperator) o;
            return dropMetricName == that.dropMetricName && Objects.equals(match, that.match) && Objects.equals(binaryOp, that.binaryOp);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), match, dropMetricName, binaryOp);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("PromQL plans should not be serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PromQL plans should not be serialized");
    }

    @Override
    public PromqlDataType returnType() {
        return PromqlDataType.INSTANT_VECTOR;
    }
}
