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

import java.io.IOException;
import java.util.Objects;

public class BulkLookupMvFilterExec extends UnaryExec {
    private final Attribute field;

    public BulkLookupMvFilterExec(Source source, PhysicalPlan child, Attribute field) {
        super(source, child);
        this.field = field;
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
    protected NodeInfo<BulkLookupMvFilterExec> info() {
        return NodeInfo.create(this, BulkLookupMvFilterExec::new, child(), field);
    }

    @Override
    public BulkLookupMvFilterExec replaceChild(PhysicalPlan newChild) {
        return new BulkLookupMvFilterExec(source(), newChild, field);
    }

    @Override
    protected AttributeSet computeReferences() {
        return field.references();
    }

    public Attribute field() {
        return field;
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BulkLookupMvFilterExec other = (BulkLookupMvFilterExec) obj;
        return Objects.equals(field, other.field) && Objects.equals(child(), other.child());
    }
}
