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
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class FuseScoreEval extends UnaryPlan implements LicenseAware, PostAnalysisVerificationAware {
    private final Attribute discriminatorAttr;
    private final Attribute scoreAttr;
    private final MapExpression options;

    public FuseScoreEval(Source source, LogicalPlan child, Attribute scoreAttr, Attribute discriminatorAttr, MapExpression options) {
        super(source, child);
        this.scoreAttr = scoreAttr;
        this.discriminatorAttr = discriminatorAttr;
        this.options = options;
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
        return NodeInfo.create(this, FuseScoreEval::new, child(), scoreAttr, discriminatorAttr, options);
    }

    @Override
    public boolean expressionsResolved() {
        return scoreAttr.resolved() && discriminatorAttr.resolved();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new FuseScoreEval(source(), newChild, scoreAttr, discriminatorAttr, options);
    }

    public Attribute score() {
        return scoreAttr;
    }

    public Attribute discriminator() {
        return discriminatorAttr;
    }

    public FuseConfig fuseConfig() {
        if (options == null) {
            return RrfConfig.DEFAULT_CONFIG;
        }
        Double rankConstant = RrfConfig.DEFAULT_RANK_CONSTANT;
        Expression rankConstantExp = options.keyFoldedMap().get(RrfConfig.RANK_CONSTANT);
        if (rankConstantExp != null) {
            rankConstant = ((Number) rankConstantExp.fold(FoldContext.small())).doubleValue();
        }

        Map<String, Double> weights = new HashMap<>();
        Expression weightsExp = options.keyFoldedMap().get(FuseConfig.WEIGHTS);
        if (weightsExp != null) {
            for (Map.Entry<String, Expression> entry : ((MapExpression) weightsExp).keyFoldedMap().entrySet()) {
                weights.put(entry.getKey(), ((Number) entry.getValue().fold(FoldContext.small())).doubleValue());
            }
        }

        return new RrfConfig(rankConstant, weights);
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

    @Override
    public void postAnalysisVerification(Failures failures) {
        if (options == null) {
            return;
        }

        options.keyFoldedMap().forEach((key, value) -> {
            if (key.equals(RrfConfig.RANK_CONSTANT)) {
                validatePositiveNumber(failures, value, key);
            } else if (key.equals(FuseConfig.WEIGHTS)) {
                validateWeights(value, failures);
            } else {
                failures.add(new Failure(this, "unknown option [" + key + "] in [" + this.sourceText() + "]"));
            }
        });
    }

    private void validateWeights(Expression weights, Failures failures) {
        if ((weights instanceof MapExpression) == false) {
            failures.add(new Failure(this, "expected weights to be a MapExpression, got [" + weights.sourceText() + "]"));
            return;
        }
        ((MapExpression) weights).keyFoldedMap().forEach((key, value) -> { validatePositiveNumber(failures, value, "weight"); });
    }

    private void validatePositiveNumber(Failures failures, Expression value, String name) {
        if ((value instanceof Literal) == false) {
            failures.add(new Failure(this, "expected " + name + " to be a literal, got [" + value.sourceText() + "]"));
        }

        if (value.dataType().isNumeric() == false) {
            failures.add(new Failure(this, "expected " + name + " to be numeric, got [" + value.sourceText() + "]"));
            return;
        }
        Number numericValue = (Number) value.fold(FoldContext.small());
        if (numericValue != null && numericValue.doubleValue() <= 0) {
            failures.add(new Failure(this, "expected " + name + " to be positive, got [" + value.sourceText() + "]"));
        }
    }
}
