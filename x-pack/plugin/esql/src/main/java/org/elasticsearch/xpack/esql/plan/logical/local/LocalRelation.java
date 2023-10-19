/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class LocalRelation extends LeafPlan {

    private final List<Attribute> output;
    private final LocalSupplier supplier;

    public LocalRelation(Source source, List<Attribute> output, LocalSupplier supplier) {
        super(source);
        this.output = output;
        this.supplier = supplier;
    }

    @Override
    protected NodeInfo<LocalRelation> info() {
        return NodeInfo.create(this, LocalRelation::new, output, supplier);
    }

    public LocalSupplier supplier() {
        return supplier;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    public int hashCode() {
        return Objects.hash(output, supplier);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        LocalRelation other = (LocalRelation) obj;
        return Objects.equals(supplier, other.supplier) && Objects.equals(output, other.output);
    }

}
