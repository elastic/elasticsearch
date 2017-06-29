/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.physical;

import java.util.Arrays;
import java.util.Objects;

import org.elasticsearch.xpack.sql.tree.Location;

abstract class BinaryExec extends PhysicalPlan {

    private final PhysicalPlan left, right;

    protected BinaryExec(Location location, PhysicalPlan left, PhysicalPlan right) {
        super(location, Arrays.asList(left, right));
        this.left = left;
        this.right = right;
    }

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
