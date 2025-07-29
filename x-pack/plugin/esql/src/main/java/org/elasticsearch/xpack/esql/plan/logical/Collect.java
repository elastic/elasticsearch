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
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@code Filter} is a type of Plan that performs filtering of results. In
 * {@code SELECT x FROM y WHERE z ..} the "WHERE" clause is a Filter. A
 * {@code Filter} has a "condition" Expression that does the filtering.
 */
public class Collect extends UnaryPlan implements TelemetryAware, SortAgnostic {
    private final ReferenceAttribute rowsEmittedAttribute;
    private final Literal index;

    public Collect(Source source, LogicalPlan child, ReferenceAttribute rowsEmittedAttribute, Literal index) {
        super(source, child);
        this.rowsEmittedAttribute = rowsEmittedAttribute;
        this.index = index;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected NodeInfo<Collect> info() {
        return NodeInfo.create(this, Collect::new, child(), rowsEmittedAttribute, index);
    }

    @Override
    public Collect replaceChild(LogicalPlan newChild) {
        return new Collect(source(), newChild, rowsEmittedAttribute, index);
    }

    public ReferenceAttribute rowsEmittedAttribute() {
        return rowsEmittedAttribute;
    }

    public Literal index() {
        return index;
    }

    @Override
    protected AttributeSet computeReferences() {
        return child().outputSet();
    }

    @Override
    public List<Attribute> output() {
        return List.of(rowsEmittedAttribute);
    }

    @Override
    public String telemetryLabel() {
        return "COLLECT";
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, child());
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

        return Objects.equals(index, other.index) && Objects.equals(child(), other.child());
    }
}
