/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.fuse;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;

public class FuseScoreEval extends UnaryPlan implements LicenseAware {

    private final Attribute discriminator;
    private final Attribute score;
    private final FuseConfig config;

    public FuseScoreEval(Source source, LogicalPlan child, Attribute score, Attribute discriminator, FuseConfig config) {
        super(source, child);

        this.discriminator = discriminator;
        this.score = score;
        this.config = config;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new FuseScoreEval(source(), newChild, score, discriminator, config);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, FuseScoreEval::new, child(), score, discriminator, config);
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
    public boolean expressionsResolved() {
        return score.resolved() && discriminator.resolved();
    }

    @Override
    public boolean licenseCheck(XPackLicenseState state) {
        return state.isAllowedByLicense(License.OperationMode.ENTERPRISE);
    }

    public Attribute discriminator() {
        return discriminator;
    }

    public Attribute score() {
        return score;
    }

    public FuseConfig config() {
        return config;
    }
}
