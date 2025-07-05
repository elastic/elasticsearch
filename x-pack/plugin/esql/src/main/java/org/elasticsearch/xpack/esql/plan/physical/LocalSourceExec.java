/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.ImmediateLocalSupplier;
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
        if (in.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOCAL_RELATION_WITH_NEW_BLOCKS)) {
            this.supplier = in.readNamedWriteable(LocalSupplier.class);
        } else {
            this.supplier = readLegacyLocalSupplierFrom((PlanStreamInput) in);
        }
    }

    /**
     * Legacy {@link LocalSupplier} deserialization for code that didn't use {@link org.elasticsearch.common.io.stream.NamedWriteable}s
     * and the {@link LocalSupplier} had only one implementation (the {@link ImmediateLocalSupplier}).
     *
     * @param in
     * @return
     * @throws IOException
     */
    public static LocalSupplier readLegacyLocalSupplierFrom(PlanStreamInput in) throws IOException {
        Block[] blocks = in.readCachedBlockArray();
        return blocks.length == 0 ? EmptyLocalSupplier.EMPTY : LocalSupplier.of(blocks);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteableCollection(output);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_LOCAL_RELATION_WITH_NEW_BLOCKS)) {
            out.writeNamedWriteable(supplier);
        } else {
            if (supplier == EmptyLocalSupplier.EMPTY) {
                out.writeVInt(0);
            } else {// here we can only have an ImmediateLocalSupplier as this was the only implementation apart from EMPTY
                ((ImmediateLocalSupplier) supplier).writeTo(out);
            }
        }
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
