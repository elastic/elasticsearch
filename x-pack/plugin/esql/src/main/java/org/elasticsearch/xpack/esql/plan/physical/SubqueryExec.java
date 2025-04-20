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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Objects;

/**
 * Physical plan representing a subquery, meaning a section of the plan that needs to be executed independently.
 */
public class SubqueryExec extends UnaryExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "SubqueryExec",
        SubqueryExec::new
    );

    public SubqueryExec(Source source, PhysicalPlan child) {
        super(source, child);
    }

    private SubqueryExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(PhysicalPlan.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
    }

    @Override
    public SubqueryExec replaceChild(PhysicalPlan newChild) {
        return new SubqueryExec(source(), newChild);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, SubqueryExec::new, child());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        SubqueryExec that = (SubqueryExec) o;
        return Objects.equals(child(), that.child());
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
