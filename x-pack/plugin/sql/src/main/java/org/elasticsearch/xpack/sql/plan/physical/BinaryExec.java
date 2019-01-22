/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.xpack.sql.tree.Source;

abstract class BinaryExec extends PhysicalPlan {

    private final PhysicalPlan left, right;

    protected BinaryExec(Source source, PhysicalPlan left, PhysicalPlan right) {
        super(source, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    @Override
    public final BinaryExec replaceChildren(List<PhysicalPlan> newChildren) {
        if (newChildren.size() != 2) {
            throw new IllegalArgumentException("expected [2] children but received [" + newChildren.size() + "]");
        }
        return replaceChildren(newChildren.get(0), newChildren.get(1));
    }
    protected abstract BinaryExec replaceChildren(PhysicalPlan newLeft, PhysicalPlan newRight);

    public PhysicalPlan left() {
        return left;
    }

    public PhysicalPlan right() {
        return right;
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
        return Objects.equals(left, other.left)
                && Objects.equals(right, other.right);
    }
}
