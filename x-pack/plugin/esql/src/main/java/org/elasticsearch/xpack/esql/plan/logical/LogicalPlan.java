/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvable;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.plan.QueryPlan;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.io.IOException;
import java.util.List;

/**
 * A LogicalPlan is <b>what</b> (not the "how") a user told us they want to do.
 * For example, a logical plan in English would be: "I want to get from DEN to SFO".
 */
public abstract class LogicalPlan extends QueryPlan<LogicalPlan> implements Resolvable {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(LocalRelation.ENTRY, Lookup.ENTRY, Join.ENTRY, TopN.ENTRY);
    }

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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // TODO remove when all PhysicalPlans are migrated to NamedWriteable
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
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
