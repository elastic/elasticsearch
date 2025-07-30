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
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class CollectExec extends UnaryExec {
    private final ReferenceAttribute rowsEmittedAttribute;
    private final Literal index;
    private final List<NamedExpression> idFields;

    public CollectExec(
        Source source,
        PhysicalPlan child,
        ReferenceAttribute rowsEmittedAttribute,
        Literal index,
        List<NamedExpression> idFields
    ) {
        super(source, child);
        this.rowsEmittedAttribute = rowsEmittedAttribute;
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
    protected NodeInfo<CollectExec> info() {
        return NodeInfo.create(this, CollectExec::new, child(), rowsEmittedAttribute, index, idFields);
    }

    @Override
    public CollectExec replaceChild(PhysicalPlan newChild) {
        return new CollectExec(source(), newChild, rowsEmittedAttribute, index, idFields);
    }

    public Literal index() {
        return index;
    }

    public List<NamedExpression> idFields() {
        return idFields;
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

        CollectExec other = (CollectExec) obj;
        return index.equals(other.index) && idFields.equals(other.idFields) && child().equals(other.child());
    }
}
