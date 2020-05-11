/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.sql.plan.logical.Pivot;

import java.util.List;
import java.util.Objects;

public class PivotExec extends UnaryExec implements Unexecutable {

    private final Pivot pivot;

    public PivotExec(Source source, PhysicalPlan child, Pivot pivot) {
        super(source, child);
        this.pivot = pivot;
    }

    @Override
    protected NodeInfo<PivotExec> info() {
        return NodeInfo.create(this, PivotExec::new, child(), pivot);
    }

    @Override
    protected PivotExec replaceChild(PhysicalPlan newChild) {
        return new PivotExec(source(), newChild, pivot);
    }

    @Override
    public List<Attribute> output() {
        return pivot.output();
    }

    public Pivot pivot() {
        return pivot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(pivot, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PivotExec other = (PivotExec) obj;

        return Objects.equals(pivot, other.pivot)
                && Objects.equals(child(), other.child());
    }
}
