/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

public class RrfScoreEval extends UnaryPlan implements PostAnalysisVerificationAware, LicenseAware {
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

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (this.child() instanceof Fork == false) {
            failures.add(
                fail(
                    this,
                    "Invalid use of RRF. RRF can only be used after FORK, but found {}",
                    child().sourceText().split(" ")[0].toUpperCase(Locale.ROOT)
                )
            );
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), scoreAttr, forkAttr);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RrfScoreEval rrf = (RrfScoreEval) obj;
        return child().equals(rrf.child()) && scoreAttr.equals(rrf.scoreAttribute()) && forkAttr.equals(forkAttribute());
    }

    @Override
    public boolean licenseCheck(XPackLicenseState state) {
        return state.isAllowedByLicense(License.OperationMode.ENTERPRISE);
    }
}
