/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@code Filter} is a type of Plan that performs filtering of results. In
 * {@code SELECT x FROM y WHERE z ..} the "WHERE" clause is a Filter. A
 * {@code Filter} has a "condition" Expression that does the filtering.
 */
public class Collect extends UnaryPlan implements TelemetryAware, SortAgnostic {
    private final List<Attribute> outputAttributes;
    private final Literal index;
    private final List<NamedExpression> idFields;

    public Collect(Source source, LogicalPlan child, Literal index, List<NamedExpression> idFields) {
        this(
            source,
            child,
            List.of(
                new ReferenceAttribute(source, "rows_saved", DataType.LONG),
                new ReferenceAttribute(source, "rows_created", DataType.LONG),
                new ReferenceAttribute(source, "rows_updated", DataType.LONG),
                new ReferenceAttribute(source, "rows_noop", DataType.LONG),
                new ReferenceAttribute(source, "bytes_saved", DataType.LONG),
                new ReferenceAttribute(source, "bulk_took_millis", DataType.LONG),
                new ReferenceAttribute(source, "bulk_ingest_took_millis", DataType.LONG)
            ),
            index,
            idFields
        );
    }

    private Collect(Source source, LogicalPlan child, List<Attribute> outputAttributes, Literal index, List<NamedExpression> idFields) {
        super(source, child);
        this.outputAttributes = outputAttributes;
        this.index = index;
        this.idFields = idFields;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected NodeInfo<Collect> info() {
        return NodeInfo.create(this, Collect::new, child(), outputAttributes, index, idFields);
    }

    @Override
    public Collect replaceChild(LogicalPlan newChild) {
        return new Collect(source(), newChild, outputAttributes, index, idFields);
    }

    public Literal index() {
        return index;
    }

    public List<NamedExpression> idFields() {
        return idFields;
    }

    @Override
    protected AttributeSet computeReferences() {
        // NOCOMMIT this is busted for `FROM foo | COLLECT`
        return child().outputSet();
    }

    @Override
    public List<Attribute> output() {
        return outputAttributes;
    }

    @Override
    public String telemetryLabel() {
        return "COLLECT";
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(idFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, idFields, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Collect other = (Collect) obj;
        return index.equals(other.index) && idFields.equals(other.idFields) && Objects.equals(child(), other.child());
    }
}
