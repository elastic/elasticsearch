/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class MergeExec extends PhysicalPlan {

    private final List<Attribute> output;

    public MergeExec(Source source, List<PhysicalPlan> children, List<Attribute> output) {
        super(source, children);
        this.output = output;
    }

    /**
     * Extracts the children as a list of suppliers. All children must be LocalSourceExec.
     */
    public List<LocalSupplier> suppliers() {
        return children().stream().map(LocalSourceExec.class::cast).map(LocalSourceExec::supplier).toList();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public PhysicalPlan replaceChildren(List<PhysicalPlan> newChildren) {
        return new MergeExec(source(), newChildren, output());
    }

    @Override
    protected NodeInfo<MergeExec> info() {
        return NodeInfo.create(this, MergeExec::new, children(), output);
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(children());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MergeExec other = (MergeExec) o;
        return Objects.equals(this.children(), other.children());
    }
}
