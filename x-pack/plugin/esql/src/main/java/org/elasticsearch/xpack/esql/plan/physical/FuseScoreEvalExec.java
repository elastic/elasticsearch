/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.fuse.FuseConfig;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class FuseScoreEvalExec extends UnaryExec {
    private final Attribute scoreAttr;
    private final Attribute discriminatorAttr;
    private final FuseConfig fuseConfig;

    public FuseScoreEvalExec(Source source, PhysicalPlan child, Attribute scoreAttr, Attribute discriminatorAttr, FuseConfig fuseConfig) {
        super(source, child);
        this.scoreAttr = scoreAttr;
        this.discriminatorAttr = discriminatorAttr;
        this.fuseConfig = fuseConfig;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, FuseScoreEvalExec::new, child(), scoreAttr, discriminatorAttr, fuseConfig);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FuseScoreEvalExec(source(), newChild, scoreAttr, discriminatorAttr, fuseConfig);
    }

    public Attribute score() {
        return scoreAttr;
    }

    public Attribute discriminator() {
        return discriminatorAttr;
    }

    public FuseConfig fuseConfig() {
        return fuseConfig;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(scoreAttr, discriminatorAttr);
    }
}
