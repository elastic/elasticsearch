/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public abstract class UnaryExec extends PhysicalPlan {

    private final PhysicalPlan child;

    protected UnaryExec(Source source, PhysicalPlan child) {
        super(source, Collections.singletonList(child));
        this.child = child;
    }

    @Override
    public final PhysicalPlan replaceChildren(List<PhysicalPlan> newChildren) {
        return replaceChild(newChildren.get(0));
    }

    public abstract UnaryExec replaceChild(PhysicalPlan newChild);

    public PhysicalPlan child() {
        return child;
    }

    @Override
    public List<Attribute> output() {
        return child.output();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        UnaryExec other = (UnaryExec) obj;

        return Objects.equals(child, other.child);
    }

}
