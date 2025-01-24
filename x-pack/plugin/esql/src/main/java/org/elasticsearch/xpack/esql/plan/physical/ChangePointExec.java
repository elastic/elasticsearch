/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ChangePointExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "ChangePointExec",
        ChangePointExec::new
    );

    private final NamedExpression value;
    private final NamedExpression key;
    private final Attribute targetType;
    private final Attribute targetPvalue;

    private List<Attribute> output;

    public ChangePointExec(
        Source source,
        PhysicalPlan child,
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

    private ChangePointExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
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
    protected NodeInfo<? extends ChangePointExec> info() {
        return NodeInfo.create(this, ChangePointExec::new, child(), value, key, targetType, targetPvalue);
    }

    @Override
    public ChangePointExec replaceChild(PhysicalPlan newChild) {
        return new ChangePointExec(source(), newChild, value, key, targetType, targetPvalue);
    }

    @Override
    protected AttributeSet computeReferences() {
        return key.references().combine(value.references());
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
        ChangePointExec other = (ChangePointExec) obj;
        return Objects.equals(child(), other.child())
            && Objects.equals(value, other.value)
            && Objects.equals(key, other.key)
            && Objects.equals(targetType, value)
            && Objects.equals(targetType, other.targetType)
            && Objects.equals(targetPvalue, other.targetPvalue);
    }
}
