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
import java.util.Objects;

public class FuseScoreEval extends UnaryPlan implements LicenseAware {
    private final Attribute discriminatorAttr;
    private final Attribute scoreAttr;

    public FuseScoreEval(Source source, LogicalPlan child, Attribute scoreAttr, Attribute discriminatorAttr) {
        super(source, child);
        this.scoreAttr = scoreAttr;
        this.discriminatorAttr = discriminatorAttr;
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
        return NodeInfo.create(this, FuseScoreEval::new, child(), scoreAttr, discriminatorAttr);
    }

    @Override
    public boolean expressionsResolved() {
        return scoreAttr.resolved() && discriminatorAttr.resolved();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new FuseScoreEval(source(), newChild, scoreAttr, discriminatorAttr);
    }

    public Attribute score() {
        return scoreAttr;
    }

    public Attribute discriminator() {
        return discriminatorAttr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), scoreAttr, discriminatorAttr);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FuseScoreEval rrf = (FuseScoreEval) obj;
        return child().equals(rrf.child()) && scoreAttr.equals(rrf.score()) && discriminatorAttr.equals(discriminator());
    }

    @Override
    public boolean licenseCheck(XPackLicenseState state) {
        return state.isAllowedByLicense(License.OperationMode.ENTERPRISE);
    }
}
