/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public abstract class BinaryExec extends PhysicalPlan {

    private final PhysicalPlan left, right;

    protected BinaryExec(Source source, PhysicalPlan left, PhysicalPlan right) {
        super(source, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    @Override
    public final BinaryExec replaceChildren(List<PhysicalPlan> newChildren) {
        return replaceChildren(newChildren.get(0), newChildren.get(1));
    }

    protected abstract BinaryExec replaceChildren(PhysicalPlan newLeft, PhysicalPlan newRight);

    public PhysicalPlan left() {
        return left;
    }

    public PhysicalPlan right() {
        return right;
    }

    public abstract AttributeSet leftReferences();

    public abstract AttributeSet rightReferences();

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(left);
        out.writeNamedWriteable(right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BinaryExec other = (BinaryExec) obj;
        return Objects.equals(left, other.left) && Objects.equals(right, other.right);
    }
}
