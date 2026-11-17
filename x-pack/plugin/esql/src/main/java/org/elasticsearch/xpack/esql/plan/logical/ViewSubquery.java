/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.xpack.esql.core.expression.RelationClass;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * A {@link NamedSubquery} that carries the view name it was resolved from.
 * <p>
 * The type itself states "this branch is a view": view resolution and compaction test for it, so a plain
 * {@link NamedSubquery} (a named {@code LET} binding) is never mistaken for one. After view resolution, a
 * post-processing pass converts {@link UnionAll} nodes containing {@code ViewSubquery} children into {@link BranchPlan}.
 * <p>
 * This class should only be used during query re-writing and not survive analysis, so it needs no serialization of its
 * own.
 */
public class ViewSubquery extends NamedSubquery {

    public ViewSubquery(Source source, LogicalPlan subqueryPlan, String name) {
        super(source, subqueryPlan, name);
    }

    @Override
    public RelationClass relationClass() {
        return RelationClass.VIEW;
    }

    @Override
    protected NodeInfo<ViewSubquery> info() {
        return NodeInfo.create(this, ViewSubquery::new, child(), name());
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new ViewSubquery(source(), newChild, name());
    }
}
