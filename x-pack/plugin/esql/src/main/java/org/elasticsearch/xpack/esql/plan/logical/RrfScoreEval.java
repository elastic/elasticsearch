/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class RrfScoreEval extends UnaryPlan {
    private final Attribute forkAttr;
    private final Attribute scoreAttr;

    public RrfScoreEval(Source source, LogicalPlan child, Attribute scoreAttr, Attribute forkAttr) {
        super(source, child);
        this.scoreAttr = scoreAttr;
        this.forkAttr = forkAttr;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, RrfScoreEval::new, child(), scoreAttr, forkAttr);
    }

    @Override
    public boolean expressionsResolved() {
        return scoreAttr.resolved() && forkAttr.resolved();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new RrfScoreEval(source(), newChild, scoreAttr, forkAttr);
    }

    public Attribute scoreAttribute() {
        return scoreAttr;
    }

    public Attribute forkAttribute() {
        return forkAttr;
    }
}
