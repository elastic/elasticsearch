/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.tree.Location;

public abstract class UnaryPlan extends LogicalPlan {

    private final LogicalPlan child;

    UnaryPlan(Location location, LogicalPlan child) {
        super(location, Collections.singletonList(child));
        this.child = child;
    }

    public LogicalPlan child() {
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

        UnaryPlan other = (UnaryPlan) obj;

        return Objects.equals(child, other.child);
    }
}
