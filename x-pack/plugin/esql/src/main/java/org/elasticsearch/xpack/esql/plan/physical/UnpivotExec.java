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
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.plan.logical.Unpivot.calculateOutput;

public class UnpivotExec extends UnaryExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "UnpivotExec",
        UnpivotExec::new
    );

    private final List<NamedExpression> sourceColumns;
    private final Attribute keyColumn;
    private final Attribute valueColumn;

    private final List<Attribute> output;

    public UnpivotExec(Source source, PhysicalPlan child, List<NamedExpression> sourceColumns, Attribute keyColumn, Attribute valueColumn) {
        super(source, child);
        this.sourceColumns = sourceColumns;
        this.keyColumn = keyColumn;
        this.valueColumn = valueColumn;
        this.output = calculateOutput(child.output(), sourceColumns, keyColumn, valueColumn);
    }

    private UnpivotExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            in.readNamedWriteable(Attribute.class),
            in.readNamedWriteable(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(sourceColumns);
        out.writeNamedWriteable(keyColumn);
        out.writeNamedWriteable(valueColumn);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected AttributeSet computeReferences() {
        AttributeSet result = new AttributeSet();
        for (NamedExpression sourceColumn : sourceColumns) {
            result.addAll(sourceColumn.references());
        }
        return result;
    }

    @Override
    protected NodeInfo<UnpivotExec> info() {
        return NodeInfo.create(this, UnpivotExec::new, child(), sourceColumns, keyColumn, valueColumn);
    }

    @Override
    public UnpivotExec replaceChild(PhysicalPlan newChild) {
        return new UnpivotExec(source(), newChild, sourceColumns, keyColumn, valueColumn);
    }

    public Attribute keyColumn() {
        return keyColumn;
    }

    public Attribute valueColumn() {
        return valueColumn;
    }

    public List<NamedExpression> sourceColumns() {
        return sourceColumns;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), sourceColumns, keyColumn, valueColumn);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnpivotExec other = (UnpivotExec) obj;

        return Objects.equals(sourceColumns, other.sourceColumns)
            && Objects.equals(child(), other.child())
            && Objects.equals(keyColumn, other.keyColumn)
            && Objects.equals(valueColumn, other.valueColumn);
    }
}
