/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class LocalSourceExec extends LeafExec {

    private final List<Attribute> output;
    private final LocalSupplier supplier;

    public LocalSourceExec(Source source, List<Attribute> output, LocalSupplier supplier) {
        super(source);
        this.output = output;
        this.supplier = supplier;
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
