/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.fuse.FuseConfig;

import java.io.IOException;

public class FuseScoreEvalExec extends UnaryExec {
    private final Attribute score;
    private final Attribute discriminator;
    private final FuseConfig config;

    public FuseScoreEvalExec(Source source, PhysicalPlan child, Attribute score, Attribute discriminator, FuseConfig config) {
        super(source, child);
        this.score = score;
        this.discriminator = discriminator;
        this.config = config;
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new FuseScoreEvalExec(source(), newChild, score, discriminator, config);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, FuseScoreEvalExec::new, child(), score, discriminator, config);
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    public Attribute score() {
        return score;
    }

    public Attribute discriminator() {
        return discriminator;
    }

    public FuseConfig config() {
        return config;
    }
}
