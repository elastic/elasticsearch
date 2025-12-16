/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.fuse;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.operator.fuse.FuseConfig;
import org.elasticsearch.compute.operator.fuse.LinearConfig;
import org.elasticsearch.compute.operator.fuse.RrfConfig;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Values;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class FuseScoreEval extends UnaryPlan
    implements
        LicenseAware,
        PostAnalysisVerificationAware,
        ExecutesOn.Coordinator,
        PipelineBreaker {
    private final Attribute discriminatorAttr;
    private final Attribute scoreAttr;
    private final Fuse.FuseType fuseType;
    private final MapExpression options;

    public FuseScoreEval(
        Source source,
        LogicalPlan child,
        Attribute scoreAttr,
        Attribute discriminatorAttr,
        Fuse.FuseType fuseType,
        MapExpression options
    ) {
        super(source, child);
        this.scoreAttr = scoreAttr;
        this.discriminatorAttr = discriminatorAttr;
        this.fuseType = fuseType;
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
        return NodeInfo.create(this, FuseScoreEval::new, child(), scoreAttr, discriminatorAttr, fuseType, options);
    }

    @Override
    public boolean expressionsResolved() {
        return scoreAttr.resolved() && discriminatorAttr.resolved();
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new FuseScoreEval(source(), newChild, scoreAttr, discriminatorAttr, fuseType, options);
    }

    public Attribute score() {
        return scoreAttr;
    }

    public Attribute discriminator() {
        return discriminatorAttr;
    }

    public FuseConfig fuseConfig() {
        return switch (fuseType) {
            case RRF -> rrfFuseConfig();
            case LINEAR -> linearFuseConfig();
        };
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), scoreAttr, discriminatorAttr, fuseType, options);
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
        validateInput(failures);
        validatePipelineBreakerBeforeFuse(failures);
        if (options == null) {
            return;
        }

        switch (fuseType) {
            case LINEAR -> validateLinearOptions(failures);
            case RRF -> validateRrfOptions(failures);
        }
    }

    private void validateInput(Failures failures) {
        // Since we use STATS BY to merge rows together, we need to make sure that all columns can be used in STATS BY.
        // When the input of FUSE contains unsupported columns, we don't want to fail with a STATS BY validation error,
        // but with an error specific to FUSE.
        Expression aggFilter = new Literal(source(), true, DataType.BOOLEAN);

        for (Attribute attr : child().output()) {
            var valuesAgg = new Values(source(), attr, aggFilter, AggregateFunction.NO_WINDOW);

            if (valuesAgg.resolved() == false) {
                failures.add(
                    new Failure(
                        this,
                        "cannot use [" + attr.name() + "] as an input of FUSE. Consider using [DROP " + attr.name() + "] before FUSE."
                    )
                );
            }
        }
    }

    private void validatePipelineBreakerBeforeFuse(Failures failures) {
        var myself = this;
        Holder<Boolean> hasLimitedInput = new Holder<>(false);
        this.forEachUp(LogicalPlan.class, plan -> {
            if (plan == myself) {
                return;
            }

            if (plan instanceof PipelineBreaker) {
                hasLimitedInput.set(true);
            }
        });

        if (hasLimitedInput.get() == false) {
            failures.add(new Failure(this, "FUSE can only be used on a limited number of rows. Consider adding a LIMIT before FUSE."));
        }
    }

    private void validateRrfOptions(Failures failures) {
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

    private void validateLinearOptions(Failures failures) {
        options.keyFoldedMap().forEach((key, value) -> {
            if (key.equals(LinearConfig.NORMALIZER)) {
                if ((value instanceof Literal) == false) {
                    failures.add(new Failure(this, "expected " + key + " to be a literal, got [" + value.sourceText() + "]"));
                    return;
                }
                if (value.dataType() != DataType.KEYWORD) {
                    failures.add(new Failure(this, "expected " + key + " to be a string, got [" + value.sourceText() + "]"));
                    return;
                }
                String stringValue = BytesRefs.toString(value.fold(FoldContext.small())).toUpperCase(Locale.ROOT);
                if (Arrays.stream(LinearConfig.Normalizer.values()).noneMatch(s -> s.name().equals(stringValue))) {
                    failures.add(new Failure(this, "[" + value.sourceText() + "] is not a valid normalizer"));
                } else if (LinearConfig.Normalizer.valueOf(stringValue) == LinearConfig.Normalizer.L2_NORM
                    && EsqlCapabilities.Cap.FUSE_L2_NORM.isEnabled() == false) {
                        failures.add(new Failure(this, "[" + value.sourceText() + "] is not a valid normalizer"));
                    }
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

    private FuseConfig rrfFuseConfig() {
        if (options == null) {
            return RrfConfig.DEFAULT_CONFIG;
        }
        Double rankConstant = RrfConfig.DEFAULT_RANK_CONSTANT;
        Expression rankConstantExp = options.keyFoldedMap().get(RrfConfig.RANK_CONSTANT);
        if (rankConstantExp != null) {
            rankConstant = ((Number) rankConstantExp.fold(FoldContext.small())).doubleValue();
        }
        return new RrfConfig(rankConstant, configWeights());
    }

    private FuseConfig linearFuseConfig() {
        if (options == null) {
            return LinearConfig.DEFAULT_CONFIG;
        }

        LinearConfig.Normalizer normalizer = LinearConfig.Normalizer.NONE;
        Expression normalizerExp = options.keyFoldedMap().get(LinearConfig.NORMALIZER);
        if (normalizerExp != null) {
            normalizer = LinearConfig.Normalizer.valueOf(
                BytesRefs.toString(normalizerExp.fold(FoldContext.small())).toUpperCase(Locale.ROOT)
            );
        }
        return new LinearConfig(normalizer, configWeights());
    }

    private Map<String, Double> configWeights() {
        Map<String, Double> weights = new HashMap<>();
        Expression weightsExp = options.keyFoldedMap().get(FuseConfig.WEIGHTS);
        if (weightsExp != null) {
            for (Map.Entry<String, Expression> entry : ((MapExpression) weightsExp).keyFoldedMap().entrySet()) {
                weights.put(entry.getKey(), ((Number) entry.getValue().fold(FoldContext.small())).doubleValue());
            }
        }
        return weights;
    }
}
