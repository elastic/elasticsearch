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
import org.elasticsearch.xpack.esql.SupportsObservabilityTier;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.SupportsObservabilityTier.ObservabilityTier.COMPLETE;
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
@SupportsObservabilityTier(tier = COMPLETE)
public class ChangePoint extends UnaryPlan
    implements
        SurrogateLogicalPlan,
        PostAnalysisVerificationAware,
        SortPreserving,
        LicenseAware,
        ExecutesOn.Coordinator {

    private final Attribute value;
    private final Attribute key;
    private final Attribute targetType;
    private final Attribute targetPvalue;
    private final List<Expression> groupings;

    private List<Attribute> output;

    public ChangePoint(
        Source source,
        LogicalPlan child,
        Attribute value,
        Attribute key,
        Attribute targetType,
        Attribute targetPvalue,
        List<Expression> groupings
    ) {
        super(source, child);
        this.value = value;
        this.key = key;
        this.targetType = targetType;
        this.targetPvalue = targetPvalue;
        this.groupings = groupings;
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
        return NodeInfo.create(this, ChangePoint::new, child(), value, key, targetType, targetPvalue, groupings);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new ChangePoint(source(), newChild, value, key, targetType, targetPvalue, groupings);
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

    public Attribute targetType() {
        return targetType;
    }

    public Attribute targetPvalue() {
        return targetPvalue;
    }

    public List<Expression> groupings() {
        return groupings;
    }

    @Override
    protected AttributeSet computeReferences() {
        return Expressions.references(List.of(key, value)).combine(Expressions.references(groupings));
    }

    @Override
    public boolean expressionsResolved() {
        return value.resolved() && key.resolved() && Resolvables.resolved(groupings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value, key, targetType, targetPvalue, groupings);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other)
            && Objects.equals(value, ((ChangePoint) other).value)
            && Objects.equals(key, ((ChangePoint) other).key)
            && Objects.equals(targetType, ((ChangePoint) other).targetType)
            && Objects.equals(targetPvalue, ((ChangePoint) other).targetPvalue)
            && Objects.equals(groupings, ((ChangePoint) other).groupings);
    }

    private List<Order> orders() {
        var keyOrder = new Order(source(), key, Order.OrderDirection.ASC, Order.NullsPosition.LAST);
        if (groupings.isEmpty()) {
            return List.of(keyOrder);
        }
        var orders = new ArrayList<Order>(groupings.size() + 1);
        for (Expression grouping : groupings) {
            orders.add(new Order(source(), grouping, Order.OrderDirection.ASC, Order.NullsPosition.LAST));
        }
        orders.add(keyOrder);
        return orders;
    }

    @Override
    public LogicalPlan surrogate() {
        OrderBy orderBy = new OrderBy(source(), child(), orders());
        Literal limitPlusOne = new Literal(Source.EMPTY, ChangePointOperator.INPUT_VALUE_COUNT_LIMIT + 1, DataType.INTEGER);
        Literal limitExact = new Literal(Source.EMPTY, ChangePointOperator.INPUT_VALUE_COUNT_LIMIT, DataType.INTEGER);
        // The first Limit of N+1 data points is necessary to generate a possible warning.
        LogicalPlan limited = groupings.isEmpty()
            ? new Limit(source(), limitPlusOne, orderBy)
            : new LimitBy(source(), limitPlusOne, orderBy, groupings);
        ChangePoint changePoint = new ChangePoint(source(), limited, value, key, targetType, targetPvalue, groupings);
        // The second Limit of N data points is to truncate the output.
        return groupings.isEmpty()
            ? new Limit(source(), limitExact, changePoint)
            : new LimitBy(source(), limitExact, changePoint, groupings);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        // Key must be sortable
        DataType type = key.dataType();
        if (DataType.isSortable(type) == false) {
            failures.add(fail(key, "CHANGE_POINT only supports sortable keys, found expression [{}] type [{}]", key.sourceText(), type));
        }
        // All groupings must be sortable
        for (Expression grouping : groupings) {
            type = grouping.dataType();
            if (DataType.isSortable(type) == false) {
                failures.add(
                    fail(
                        grouping,
                        "CHANGE_POINT grouping only supports sortable values, found expression [{}] type [{}]",
                        grouping.sourceText(),
                        type
                    )
                );
            }
        }
        // Value must be a number
        type = value.dataType();
        if (DataType.isNullOrNumeric(type) == false) {
            failures.add(
                fail(value, "CHANGE_POINT only supports numeric values, found expression [{}] type [{}]", value.sourceText(), type)
            );
        }
    }

    @Override
    public boolean licenseCheck(XPackLicenseState state) {
        return MachineLearning.CHANGE_POINT_AGG_FEATURE.check(state);
    }
}
