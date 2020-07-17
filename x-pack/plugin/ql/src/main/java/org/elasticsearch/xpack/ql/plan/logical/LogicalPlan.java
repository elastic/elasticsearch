/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvable;
import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.plan.QueryPlan;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

/**
 * A LogicalPlan is <b>what</b> (not the "how") a user told us they want to do.
 * For example, a logical plan in English would be: "I want to get from DEN to SFO".
 */
public abstract class LogicalPlan extends QueryPlan<LogicalPlan> implements Resolvable {

    /**
     * Order is important in the enum; any values should be added at the end.
     */
    public enum Stage {
        PARSED,
        PRE_ANALYZED,
        ANALYZED,
        OPTIMIZED;
    }

    private Stage stage = Stage.PARSED;
    private Boolean lazyChildrenResolved = null;
    private Boolean lazyResolved = null;

    public LogicalPlan(Source source, List<LogicalPlan> children) {
        super(source, children);
    }

    public boolean preAnalyzed() {
        return stage.ordinal() >= Stage.PRE_ANALYZED.ordinal();
    }

    public void setPreAnalyzed() {
        stage = Stage.PRE_ANALYZED;
    }

    public boolean analyzed() {
        return stage.ordinal() >= Stage.ANALYZED.ordinal();
    }

    public void setAnalyzed() {
        stage = Stage.ANALYZED;
    }

    public boolean optimized() {
        return stage.ordinal() >= Stage.OPTIMIZED.ordinal();
    }

    public void setOptimized() {
        stage = Stage.OPTIMIZED;
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
