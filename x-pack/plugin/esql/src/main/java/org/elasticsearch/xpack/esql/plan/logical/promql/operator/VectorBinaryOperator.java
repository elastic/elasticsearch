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
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
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

import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType.SCALAR;
import static org.elasticsearch.xpack.esql.plan.logical.promql.PromqlPlan.getType;

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
        this.match = Objects.requireNonNull(match, "match must be VectorMatch.NONE rather than null");
        this.dropMetricName = dropMetricName;
        this.binaryOp = binaryOp;
    }

    /** The declared vector matching; {@link VectorMatch#NONE} - never null - when the operator declares none. */
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
        // Labels the translation guarantees as result columns even when no operand declares them as attributes (an
        // operand can be opaque - its identity packed into `_timeseries` - and the translation materializes or
        // null-fills the columns): the match keys named by on(...) and the group modifier labels. Ones that resolve
        // against neither operand are declared as synthesized references.
        Set<String> guaranteed = new HashSet<>();
        List<Attribute> leftAttrs = left().output();
        List<Attribute> rightAttrs = right().output();
        Set<String> leftLabels = extractLabelNames(leftAttrs);
        Set<String> rightLabels = extractLabelNames(rightAttrs);
        if (match.grouping() == VectorMatch.Joining.LEFT) {
            // group_left keeps the left ("many") label set and copies only explicitly included labels from the right.
            outputLabels = new HashSet<>(leftLabels);
            outputLabels.addAll(match.groupingLabels());
            guaranteed.addAll(match.groupingLabels());
            if (match.filter() == VectorMatch.Filter.ON) {
                outputLabels.addAll(match.filterLabels());
                guaranteed.addAll(match.filterLabels());
            }
        } else if (match.grouping() == VectorMatch.Joining.RIGHT) {
            // group_right keeps the right ("many") label set and copies only explicitly included labels from the left.
            outputLabels = new HashSet<>(rightLabels);
            outputLabels.addAll(match.groupingLabels());
            guaranteed.addAll(match.groupingLabels());
            if (match.filter() == VectorMatch.Filter.ON) {
                outputLabels.addAll(match.filterLabels());
                guaranteed.addAll(match.filterLabels());
            }
        } else if (match.filter() == VectorMatch.Filter.ON) {
            outputLabels = new HashSet<>(match.filterLabels());
            guaranteed.addAll(match.filterLabels());
        } else if (match.filter() == VectorMatch.Filter.IGNORING) {
            outputLabels = new HashSet<>(leftLabels);
            outputLabels.removeAll(match.filterLabels());
        } else if (leftLabels.equals(rightLabels)) {
            return leftAttrs;
        } else {
            // Default matching between different label sets: a pair matches only where the labels one side lacks are
            // absent on the other side too (a Prometheus signature has no entry for an absent label), and like every
            // one-to-one match the result carries the left operand's labels.
            outputLabels = new HashSet<>(leftLabels);
        }

        if (dropMetricName) {
            outputLabels.remove(LabelMatcher.NAME);
        }

        List<Attribute> result = new ArrayList<>();
        for (String label : outputLabels) {
            Attribute attr = findAttribute(label, leftAttrs, rightAttrs);
            if (attr != null) {
                result.add(attr);
            } else if (guaranteed.contains(label)) {
                result.add(new ReferenceAttribute(source(), label, DataType.KEYWORD));
            }
        }

        return result;
    }

    /**
     * Whether both operands declare concrete label sets that differ by name. Prometheus default matching compares each
     * pair's actual label sets, so such operands cannot fold into one aggregate over shared grouping keys; they
     * translate as a join, which yields the empty vector unless the differing labels are null on both sides.
     */
    public boolean hasMismatchedLabelSets() {
        if (match != VectorMatch.NONE || PromqlPlan.returnsScalar(left()) || PromqlPlan.returnsScalar(right())) {
            return false;
        }
        List<Attribute> leftAttrs = left().output();
        List<Attribute> rightAttrs = right().output();
        if (hasPackedLabels(leftAttrs) || hasPackedLabels(rightAttrs)) {
            return false;
        }
        return extractLabelNames(leftAttrs).equals(extractLabelNames(rightAttrs)) == false;
    }

    /** Whether the operand carries a packed {@code _timeseries} column, i.e. does not name every label it exposes. */
    private static boolean hasPackedLabels(List<Attribute> attrs) {
        return attrs.stream().anyMatch(attribute -> MetadataAttribute.isTimeSeriesAttributeName(attribute.name()));
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
        PromqlDataType leftType = getType(left());
        PromqlDataType rightType = getType(right());
        // scalar op scalar → scalar; otherwise → vector
        if (leftType == SCALAR && rightType == SCALAR) {
            return SCALAR;
        }
        return PromqlDataType.INSTANT_VECTOR;
    }

    @Override
    public boolean isIdentityTransparent() {
        // Matches and merges two operands' series identities: a relabel below either operand feeds this boundary.
        return false;
    }
}
