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

public class LocalSourceExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "LocalSourceExec",
        LocalSourceExec::new
    );

    private final List<Attribute> output;
    private final LocalSupplier supplier;

    public LocalSourceExec(Source source, List<Attribute> output, LocalSupplier supplier) {
        super(source);
        this.output = output;
        this.supplier = supplier;
    }

    public LocalSourceExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in));
        this.output = in.readNamedWriteableCollectionAsList(Attribute.class);
        this.supplier = LocalSupplier.readFrom((PlanStreamInput) in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(output);
        supplier.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public LocalSupplier supplier() {
        return supplier;
    }

    @Override
    protected NodeInfo<LocalSourceExec> info() {
        return NodeInfo.create(this, LocalSourceExec::new, output, supplier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var other = (LocalSourceExec) o;
        return Objects.equals(supplier, other.supplier) && Objects.equals(output, other.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, supplier);
    }
}
