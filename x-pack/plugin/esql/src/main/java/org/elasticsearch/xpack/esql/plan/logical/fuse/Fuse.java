/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.fuse;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;

public class Fuse extends UnaryPlan implements TelemetryAware {
    private final Attribute score;
    private final Attribute discriminator;
    private final List<NamedExpression> groupings;
    private final FuseType fuseType;
    private final MapExpression options;

    public enum FuseType {
        RRF,
        LINEAR
    };

    public Fuse(
        Source source,
        LogicalPlan child,
        Attribute score,
        Attribute discriminator,
        List<NamedExpression> groupings,
        FuseType fuseType,
        MapExpression options
    ) {
        super(source, child);
        this.score = score;
        this.discriminator = discriminator;
        this.groupings = groupings;
        this.fuseType = fuseType;
        this.options = options;

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
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Fuse::new, child(), score, discriminator, groupings, fuseType, options);
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Fuse(source(), newChild, score, discriminator, groupings, fuseType, options);
    }

    public List<NamedExpression> groupings() {
        return groupings;
    }

    public Attribute discriminator() {
        return discriminator;
    }

    public Attribute score() {
        return score;
    }

    public FuseType fuseType() {
        return fuseType;
    }

    public MapExpression options() {
        return options;
    }

    @Override
    public boolean expressionsResolved() {
        return score.resolved() && discriminator.resolved() && groupings.stream().allMatch(Expression::resolved);
    }
}
