/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.plan.logical;

import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.Objects;

public abstract class BinaryPlan extends LogicalPlan {

    private final LogicalPlan left, right;

    protected BinaryPlan(Source source, LogicalPlan left, LogicalPlan right) {
        super(source, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

    public LogicalPlan left() {
        return left;
    }

    public LogicalPlan right() {
        return right;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BinaryPlan other = (BinaryPlan) obj;

        return Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }

    @Override
    public int hashCode() {
        return Objects.hash(left, right);
    }

}
