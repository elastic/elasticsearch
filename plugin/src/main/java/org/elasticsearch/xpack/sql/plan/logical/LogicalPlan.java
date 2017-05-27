/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import java.util.List;

import org.elasticsearch.xpack.sql.capabilities.Resolvable;
import org.elasticsearch.xpack.sql.capabilities.Resolvables;
import org.elasticsearch.xpack.sql.plan.QueryPlan;
import org.elasticsearch.xpack.sql.tree.Location;

public abstract class LogicalPlan extends QueryPlan<LogicalPlan> implements Resolvable {

    private boolean analyzed = false;
    private boolean optimized = false;
    private Boolean lazyChildrenResolved = null;
    private Boolean lazyResolved = null;

    public LogicalPlan(Location location, List<LogicalPlan> children) {
        super(location, children);
    }

    public boolean analyzed() {
        return analyzed;
    }

    public void setAnalyzed() {
        analyzed = true;
    }

    public boolean optimized() {
        return optimized;
    }

    public void setOptimized() {
        optimized = true;
    }

    public final boolean childrenResolved() {
        if (lazyChildrenResolved == null) {
            lazyChildrenResolved = Boolean.valueOf(Resolvables.resolved(children()));
        }
        return lazyChildrenResolved;
    }

    @Override
    public boolean resolved() {
        if (lazyResolved == null) {
            lazyResolved = expressionsResolved() && childrenResolved();
        }
        return lazyResolved;
    }

    public abstract boolean expressionsResolved();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

}
