/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.ChangePointOperator;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class ChangePoint extends UnaryPlan implements GeneratingPlan<ChangePoint>, SurrogateLogicalPlan, PostAnalysisVerificationAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "ChangePoint",
        ChangePoint::new
    );

    private final NamedExpression value;
    private final NamedExpression key;
    private final Attribute targetType;
    private final Attribute targetPvalue;

    private List<Attribute> output;

    public ChangePoint(
        Source source,
        LogicalPlan child,
        NamedExpression value,
        NamedExpression key,
        Attribute targetType,
        Attribute targetPvalue
    ) {
        super(source, child);
        this.value = value;
        this.key = key;
        this.targetType = targetType;
        this.targetPvalue = targetPvalue;
    }

    private ChangePoint(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(NamedExpression.class),
            in.readNamedWriteable(NamedExpression.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("ChangePoint should run on the coordinating node, so never be serialized.");
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<ChangePoint> info() {
        return NodeInfo.create(this, ChangePoint::new, child(), value, key, targetType, targetPvalue);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new ChangePoint(source(), newChild, value, key, targetType, targetPvalue);
    }

    @Override
    public List<Attribute> output() {
        if (output == null) {
            output = NamedExpressions.mergeOutputAttributes(List.of(targetType, targetPvalue), child().output());
        }
        return output;
    }

    public NamedExpression value() {
        return value;
    }

    public NamedExpression key() {
        return key;
    }

    public Attribute targetType() {
        return targetType;
    }

    public Attribute targetPvalue() {
        return targetPvalue;
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return List.of(targetType, targetPvalue);
    }

    @Override
    public ChangePoint withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);
        Attribute newTargetType;
        if (targetType.name().equals(newNames.get(0))) {
            newTargetType = targetType;
        } else {
            newTargetType = targetType.withName(newNames.get(0)).withId(new NameId());
        }
        Attribute newTargetPvalue;
        if (targetPvalue.name().equals(newNames.get(1))) {
            newTargetPvalue = targetPvalue;
        } else {
            newTargetPvalue = targetPvalue.withName(newNames.get(1)).withId(new NameId());
        }
        return new ChangePoint(source(), child(), value, key, newTargetType, newTargetPvalue);
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
        return Objects.hash(value, key, targetType, targetPvalue, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ChangePoint other = (ChangePoint) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(value, other.value)
            && Objects.equals(key, other.key)
            && Objects.equals(targetType, other.targetType)
            && Objects.equals(targetPvalue, other.targetPvalue);
    }

    private Order order() {
        return new Order(source(), key, Order.OrderDirection.ASC, Order.NullsPosition.ANY);
    }

    @Override
    public LogicalPlan surrogate() {
        // ChangePoint should always run on the coordinating node after the data is collected
        // in sorted order. This is enforced by adding OrderBy here.
        // Furthermore, ChangePoint should be called with at most 1000 data points. That's
        // enforced by the Limits here. The first Limit of N+1 data points is necessary to
        // generate a possible warning, the second Limit of N is to truncate the output.
        OrderBy orderBy = new OrderBy(source(), child(), List.of(order()));
        Limit limit = new Limit(
            source(),
            new Literal(Source.EMPTY, ChangePointOperator.INPUT_VALUE_COUNT_LIMIT + 1, DataType.INTEGER),
            orderBy
        );
        ChangePoint changePoint = new ChangePoint(source(), limit, value, key, targetType, targetPvalue);
        return new Limit(source(), new Literal(Source.EMPTY, ChangePointOperator.INPUT_VALUE_COUNT_LIMIT, DataType.INTEGER), changePoint);
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
}
