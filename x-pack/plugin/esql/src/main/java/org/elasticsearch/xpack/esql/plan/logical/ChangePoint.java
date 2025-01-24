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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ChangePoint extends UnaryPlan implements GeneratingPlan<ChangePoint> {
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
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(value);
        out.writeNamedWriteable(key);
        out.writeNamedWriteable(targetType);
        out.writeNamedWriteable(targetPvalue);
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
    public String commandName() {
        return "CHANGE_POINT";
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
            && Objects.equals(targetType, value)
            && Objects.equals(targetType, other.targetType)
            && Objects.equals(targetPvalue, other.targetPvalue);
    }
}
