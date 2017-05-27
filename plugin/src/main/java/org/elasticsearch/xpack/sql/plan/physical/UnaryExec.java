/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.xpack.sql.expression.Attribute;

abstract class UnaryExec extends PhysicalPlan {

    private final PhysicalPlan child;

    UnaryExec(PhysicalPlan child) {
        super(child.location(), Collections.singletonList(child));
        this.child = child;
    }

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
