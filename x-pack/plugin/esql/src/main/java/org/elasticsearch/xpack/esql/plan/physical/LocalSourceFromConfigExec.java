/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class LocalSourceFromConfigExec extends LeafExec {

    private final List<Attribute> output;
    private final LocalRelation.FromConfig relation;

    public LocalSourceFromConfigExec(Source source, List<Attribute> output, LocalRelation.FromConfig relation) {
        super(source);
        this.output = output;
        this.relation = relation;
    }

    public void writeTo(PlanStreamOutput out) throws IOException {
        out.writeSource(source());
        out.writeCollection(output, (o, v) -> ((PlanStreamOutput) o).writeAttribute(v));
        ((LocalSupplier.))
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    public LocalSupplier supplier() {
        return supplier;
    }

    @Override
    protected NodeInfo<LocalSourceFromConfigExec> info() {
        return NodeInfo.create(this, LocalSourceFromConfigExec::new, output, supplier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        var other = (LocalSourceFromConfigExec) o;
        return Objects.equals(supplier, other.supplier) && Objects.equals(output, other.output);
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, supplier);
    }
}
