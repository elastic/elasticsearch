/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.ChangePointOperator;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.Partition;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * Plan that detects change points in a list of values. See also:
 * <ul>
 * <li> {@link org.elasticsearch.compute.operator.ChangePointOperator}
 * <li> {@link org.elasticsearch.xpack.ml.aggs.changepoint.ChangePointDetector}
 * </ul>
 *
 * ChangePoint should always run on the coordinating node after the data is collected
 * in sorted order by key. This is enforced by adding OrderBy in the surrogate plan.
 * Furthermore, ChangePoint should be called with at most 1000 data points. That's
 * enforced by the Limit in the surrogate plan.
 */
public class ChangePoint extends UnaryPlan implements SurrogateLogicalPlan, PostAnalysisVerificationAware, LicenseAware {

    private final Attribute value;
    private final Attribute key;
    private final List<Attribute> partition;
    private final Attribute targetType;
    private final Attribute targetPvalue;

    private List<Attribute> output;

    public ChangePoint(
        Source source,
        LogicalPlan child,
        Attribute value,
        Attribute key,
        List<Attribute> partition,
        Attribute targetType,
        Attribute targetPvalue
    ) {
        super(source, child);
        this.value = value;
        this.key = key;
        this.partition = partition;
        this.targetType = targetType;
        this.targetPvalue = targetPvalue;
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
    protected NodeInfo<ChangePoint> info() {
        return NodeInfo.create(this, ChangePoint::new, child(), value, key, partition, targetType, targetPvalue);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new ChangePoint(source(), newChild, value, key, partition, targetType, targetPvalue);
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = NamedExpressions.mergeOutputAttributes(List.of(targetType, targetPvalue), child().output());
        }
        return output;
    }

    public Attribute value() {
        return value;
    }

    public Attribute key() {
        return key;
    }

    public List<Attribute> partition() {
        return partition;
    }

    public Attribute targetType() {
        return targetType;
    }

    public Attribute targetPvalue() {
        return targetPvalue;
    }

    @Override
    protected AttributeSet computeReferences() {
        return Expressions.references(List.of(key, value));
    }

    @Override
    public boolean expressionsResolved() {
        return value.resolved() && key.resolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value, key, partition, targetType, targetPvalue);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other)
            && Objects.equals(value, ((ChangePoint) other).value)
            && Objects.equals(key, ((ChangePoint) other).key)
            && Objects.equals(partition, ((ChangePoint) other).partition)
            && Objects.equals(targetType, ((ChangePoint) other).targetType)
            && Objects.equals(targetPvalue, ((ChangePoint) other).targetPvalue);
    }

    private Order order() {
        return new Order(source(), key, Order.OrderDirection.ASC, Order.NullsPosition.ANY);
    }

    @Override
    public LogicalPlan surrogate() {
        TopN topN = new TopN(
            source(),
            child(),
            partition.stream().map(p -> new Partition(source(), p)).toList(),
            List.of(order()),
            // The Limit of N+1 data points is necessary to generate a possible warning
            new Literal(Source.EMPTY, ChangePointOperator.INPUT_VALUE_COUNT_LIMIT + 1, DataType.INTEGER)
        );
        return new ChangePoint(source(), topN, value, key, partition, targetType, targetPvalue);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        // Key must be sortable
        Order order = order();
        if (DataType.isSortable(order.dataType()) == false) {
            failures.add(fail(this, "change point key [" + key.name() + "] must be sortable"));
        }
        // Value must be a number
        if (value.dataType().isNumeric() == false) {
            failures.add(fail(this, "change point value [" + value.name() + "] must be numeric"));
        }
    }

    @Override
    public boolean licenseCheck(XPackLicenseState state) {
        return MachineLearning.CHANGE_POINT_AGG_FEATURE.check(state);
    }
}
