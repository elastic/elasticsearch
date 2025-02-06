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
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class MergeExec extends LeafExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "MergeExec",
        MergeExec::new
    );

    private final List<? extends PhysicalPlan> physSubPlans;
    private final List<Attribute> output;

    public MergeExec(Source source, List<? extends PhysicalPlan> physSubPlans, List<Attribute> output) {
        super(source);
        this.physSubPlans = physSubPlans;
        this.output = output;
    }

    public MergeExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in));
        this.physSubPlans = null; // in.readCollectionAsList(PhysicalPlan::new);
        // this.right = in.readNamedWriteable(PhysicalPlan.class);
        this.output = in.readNamedWriteableCollectionAsList(Attribute.class);
    }

    public List<LocalSupplier> suppliers() {
        return physSubPlans.stream()
            .filter(p -> LocalSourceExec.class.isAssignableFrom(p.getClass()))
            .map(LocalSourceExec.class::cast)
            .map(LocalSourceExec::supplier)
            .toList();
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeCollection(physSubPlans);
        out.writeNamedWriteableCollection(output);
    }

    @Override
    protected NodeInfo<MergeExec> info() {
        return NodeInfo.create(this, MergeExec::new, physSubPlans, output);
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public List<? extends PhysicalPlan> subPlans() {
        return physSubPlans;
    }

    @Override
    public int hashCode() {
        return Objects.hash(source().hashCode(), physSubPlans);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MergeExec other = (MergeExec) o;
        return Objects.equals(this.source(), other.source()) && Objects.equals(this.physSubPlans, other.physSubPlans);
    }
}
