/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class ChangePointExec extends UnaryExec {

    private final Attribute value;
    private final Attribute key;
    private final List<Attribute> partition;
    private final Attribute targetType;
    private final Attribute targetPvalue;

    private List<Attribute> output;

    public ChangePointExec(
        Source source,
        PhysicalPlan child,
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
    protected NodeInfo<? extends ChangePointExec> info() {
        return NodeInfo.create(this, ChangePointExec::new, child(), value, key, partition, targetType, targetPvalue);
    }

    @Override
    public ChangePointExec replaceChild(PhysicalPlan newChild) {
        return new ChangePointExec(source(), newChild, value, key, partition, targetType, targetPvalue);
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), value, key, partition, targetType, targetPvalue);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other)
            && Objects.equals(value, ((ChangePointExec) other).value)
            && Objects.equals(key, ((ChangePointExec) other).key)
            && Objects.equals(partition, ((ChangePointExec) other).partition)
            && Objects.equals(targetType, ((ChangePointExec) other).targetType)
            && Objects.equals(targetPvalue, ((ChangePointExec) other).targetPvalue);
    }
}
