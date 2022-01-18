/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.AbstractObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Parameters used by both {@link Classification} and {@link Regression} analyses.
 */
public class BoostedTreeParams implements ToXContentFragment, Writeable {

    static final String NAME = "boosted_tree_params";

    public static final ParseField LAMBDA = new ParseField("lambda");
    public static final ParseField GAMMA = new ParseField("gamma");
    public static final ParseField ETA = new ParseField("eta");
    public static final ParseField MAX_TREES = new ParseField("max_trees", "maximum_number_trees");
    public static final ParseField FEATURE_BAG_FRACTION = new ParseField("feature_bag_fraction");
    public static final ParseField NUM_TOP_FEATURE_IMPORTANCE_VALUES = new ParseField("num_top_feature_importance_values");
    public static final ParseField ALPHA = new ParseField("alpha");
    public static final ParseField ETA_GROWTH_RATE_PER_TREE = new ParseField("eta_growth_rate_per_tree");
    public static final ParseField SOFT_TREE_DEPTH_LIMIT = new ParseField("soft_tree_depth_limit");
    public static final ParseField SOFT_TREE_DEPTH_TOLERANCE = new ParseField("soft_tree_depth_tolerance");
    public static final ParseField DOWNSAMPLE_FACTOR = new ParseField("downsample_factor");
    public static final ParseField MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER = new ParseField(
        "max_optimization_rounds_per_hyperparameter"
    );

    static void declareFields(AbstractObjectParser<?, Void> parser) {
        parser.declareDouble(optionalConstructorArg(), LAMBDA);
        parser.declareDouble(optionalConstructorArg(), GAMMA);
        parser.declareDouble(optionalConstructorArg(), ETA);
        parser.declareInt(optionalConstructorArg(), MAX_TREES);
        parser.declareDouble(optionalConstructorArg(), FEATURE_BAG_FRACTION);
        parser.declareInt(optionalConstructorArg(), NUM_TOP_FEATURE_IMPORTANCE_VALUES);
        parser.declareDouble(optionalConstructorArg(), ALPHA);
        parser.declareDouble(optionalConstructorArg(), ETA_GROWTH_RATE_PER_TREE);
        parser.declareDouble(optionalConstructorArg(), SOFT_TREE_DEPTH_LIMIT);
        parser.declareDouble(optionalConstructorArg(), SOFT_TREE_DEPTH_TOLERANCE);
        parser.declareDouble(optionalConstructorArg(), DOWNSAMPLE_FACTOR);
        parser.declareInt(optionalConstructorArg(), MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER);
    }

    private final Double lambda;
    private final Double gamma;
    private final Double eta;
    private final Integer maxTrees;
    private final Double featureBagFraction;
    private final Integer numTopFeatureImportanceValues;
    private final Double alpha;
    private final Double etaGrowthRatePerTree;
    private final Double softTreeDepthLimit;
    private final Double softTreeDepthTolerance;
    private final Double downsampleFactor;
    private final Integer maxOptimizationRoundsPerHyperparameter;

    public BoostedTreeParams(
        @Nullable Double lambda,
        @Nullable Double gamma,
        @Nullable Double eta,
        @Nullable Integer maxTrees,
        @Nullable Double featureBagFraction,
        @Nullable Integer numTopFeatureImportanceValues,
        @Nullable Double alpha,
        @Nullable Double etaGrowthRatePerTree,
        @Nullable Double softTreeDepthLimit,
        @Nullable Double softTreeDepthTolerance,
        @Nullable Double downsampleFactor,
        @Nullable Integer maxOptimizationRoundsPerHyperparameter
    ) {
        if (lambda != null && lambda < 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a non-negative double", LAMBDA.getPreferredName());
        }
        if (gamma != null && gamma < 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a non-negative double", GAMMA.getPreferredName());
        }
        if (eta != null && (eta < 0.001 || eta > 1)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in [0.001, 1]", ETA.getPreferredName());
        }
        if (maxTrees != null && (maxTrees <= 0 || maxTrees > 2000)) {
            throw ExceptionsHelper.badRequestException("[{}] must be an integer in [1, 2000]", MAX_TREES.getPreferredName());
        }
        if (featureBagFraction != null && (featureBagFraction <= 0 || featureBagFraction > 1.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in (0, 1]", FEATURE_BAG_FRACTION.getPreferredName());
        }
        if (numTopFeatureImportanceValues != null && numTopFeatureImportanceValues < 0) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be a non-negative integer",
                NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName()
            );
        }
        if (alpha != null && alpha < 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a non-negative double", ALPHA.getPreferredName());
        }
        if (etaGrowthRatePerTree != null && (etaGrowthRatePerTree < 0.5 || etaGrowthRatePerTree > 2.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in [0.5, 2.0]", ETA_GROWTH_RATE_PER_TREE.getPreferredName());
        }
        if (softTreeDepthLimit != null && softTreeDepthLimit < 0) {
            throw ExceptionsHelper.badRequestException("[{}] must be a non-negative double", SOFT_TREE_DEPTH_LIMIT.getPreferredName());
        }
        if (softTreeDepthTolerance != null && softTreeDepthTolerance < 0.01) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be a double greater than or equal to 0.01",
                SOFT_TREE_DEPTH_TOLERANCE.getPreferredName()
            );
        }
        if (downsampleFactor != null && (downsampleFactor <= 0 || downsampleFactor > 1.0)) {
            throw ExceptionsHelper.badRequestException("[{}] must be a double in (0, 1]", DOWNSAMPLE_FACTOR.getPreferredName());
        }
        if (maxOptimizationRoundsPerHyperparameter != null
            && (maxOptimizationRoundsPerHyperparameter < 0 || maxOptimizationRoundsPerHyperparameter > 20)) {
            throw ExceptionsHelper.badRequestException(
                "[{}] must be an integer in [0, 20]",
                MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER.getPreferredName()
            );
        }
        this.lambda = lambda;
        this.gamma = gamma;
        this.eta = eta;
        this.maxTrees = maxTrees;
        this.featureBagFraction = featureBagFraction;
        this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;
        this.alpha = alpha;
        this.etaGrowthRatePerTree = etaGrowthRatePerTree;
        this.softTreeDepthLimit = softTreeDepthLimit;
        this.softTreeDepthTolerance = softTreeDepthTolerance;
        this.downsampleFactor = downsampleFactor;
        this.maxOptimizationRoundsPerHyperparameter = maxOptimizationRoundsPerHyperparameter;
    }

    BoostedTreeParams(StreamInput in) throws IOException {
        lambda = in.readOptionalDouble();
        gamma = in.readOptionalDouble();
        eta = in.readOptionalDouble();
        maxTrees = in.readOptionalVInt();
        featureBagFraction = in.readOptionalDouble();
        numTopFeatureImportanceValues = in.readOptionalInt();
        alpha = in.readOptionalDouble();
        etaGrowthRatePerTree = in.readOptionalDouble();
        softTreeDepthLimit = in.readOptionalDouble();
        softTreeDepthTolerance = in.readOptionalDouble();
        downsampleFactor = in.readOptionalDouble();
        maxOptimizationRoundsPerHyperparameter = in.readOptionalVInt();
    }

    public Double getLambda() {
        return lambda;
    }

    public Double getGamma() {
        return gamma;
    }

    public Double getEta() {
        return eta;
    }

    public Integer getMaxTrees() {
        return maxTrees;
    }

    public Double getFeatureBagFraction() {
        return featureBagFraction;
    }

    public Integer getNumTopFeatureImportanceValues() {
        return numTopFeatureImportanceValues;
    }

    public Double getAlpha() {
        return alpha;
    }

    public Double getEtaGrowthRatePerTree() {
        return etaGrowthRatePerTree;
    }

    public Double getSoftTreeDepthLimit() {
        return softTreeDepthLimit;
    }

    public Double getSoftTreeDepthTolerance() {
        return softTreeDepthTolerance;
    }

    public Double getDownsampleFactor() {
        return downsampleFactor;
    }

    public Integer getMaxOptimizationRoundsPerHyperparameter() {
        return maxOptimizationRoundsPerHyperparameter;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalDouble(lambda);
        out.writeOptionalDouble(gamma);
        out.writeOptionalDouble(eta);
        out.writeOptionalVInt(maxTrees);
        out.writeOptionalDouble(featureBagFraction);
        out.writeOptionalInt(numTopFeatureImportanceValues);
        out.writeOptionalDouble(alpha);
        out.writeOptionalDouble(etaGrowthRatePerTree);
        out.writeOptionalDouble(softTreeDepthLimit);
        out.writeOptionalDouble(softTreeDepthTolerance);
        out.writeOptionalDouble(downsampleFactor);
        out.writeOptionalVInt(maxOptimizationRoundsPerHyperparameter);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (alpha != null) {
            builder.field(ALPHA.getPreferredName(), alpha);
        }
        if (lambda != null) {
            builder.field(LAMBDA.getPreferredName(), lambda);
        }
        if (gamma != null) {
            builder.field(GAMMA.getPreferredName(), gamma);
        }
        if (eta != null) {
            builder.field(ETA.getPreferredName(), eta);
        }
        if (etaGrowthRatePerTree != null) {
            builder.field(ETA_GROWTH_RATE_PER_TREE.getPreferredName(), etaGrowthRatePerTree);
        }
        if (maxTrees != null) {
            builder.field(MAX_TREES.getPreferredName(), maxTrees);
        }
        if (featureBagFraction != null) {
            builder.field(FEATURE_BAG_FRACTION.getPreferredName(), featureBagFraction);
        }
        if (numTopFeatureImportanceValues != null) {
            builder.field(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), numTopFeatureImportanceValues);
        }
        if (softTreeDepthLimit != null) {
            builder.field(SOFT_TREE_DEPTH_LIMIT.getPreferredName(), softTreeDepthLimit);
        }
        if (softTreeDepthTolerance != null) {
            builder.field(SOFT_TREE_DEPTH_TOLERANCE.getPreferredName(), softTreeDepthTolerance);
        }
        if (downsampleFactor != null) {
            builder.field(DOWNSAMPLE_FACTOR.getPreferredName(), downsampleFactor);
        }
        if (maxOptimizationRoundsPerHyperparameter != null) {
            builder.field(MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER.getPreferredName(), maxOptimizationRoundsPerHyperparameter);
        }
        return builder;
    }

    Map<String, Object> getParams() {
        Map<String, Object> params = new HashMap<>();
        if (lambda != null) {
            params.put(LAMBDA.getPreferredName(), lambda);
        }
        if (gamma != null) {
            params.put(GAMMA.getPreferredName(), gamma);
        }
        if (eta != null) {
            params.put(ETA.getPreferredName(), eta);
        }
        if (maxTrees != null) {
            params.put(MAX_TREES.getPreferredName(), maxTrees);
        }
        if (featureBagFraction != null) {
            params.put(FEATURE_BAG_FRACTION.getPreferredName(), featureBagFraction);
        }
        if (numTopFeatureImportanceValues != null) {
            params.put(NUM_TOP_FEATURE_IMPORTANCE_VALUES.getPreferredName(), numTopFeatureImportanceValues);
        }
        if (alpha != null) {
            params.put(ALPHA.getPreferredName(), alpha);
        }
        if (etaGrowthRatePerTree != null) {
            params.put(ETA_GROWTH_RATE_PER_TREE.getPreferredName(), etaGrowthRatePerTree);
        }
        if (softTreeDepthLimit != null) {
            params.put(SOFT_TREE_DEPTH_LIMIT.getPreferredName(), softTreeDepthLimit);
        }
        if (softTreeDepthTolerance != null) {
            params.put(SOFT_TREE_DEPTH_TOLERANCE.getPreferredName(), softTreeDepthTolerance);
        }
        if (downsampleFactor != null) {
            params.put(DOWNSAMPLE_FACTOR.getPreferredName(), downsampleFactor);
        }
        if (maxOptimizationRoundsPerHyperparameter != null) {
            params.put(MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER.getPreferredName(), maxOptimizationRoundsPerHyperparameter);
        }
        return params;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BoostedTreeParams that = (BoostedTreeParams) o;
        return Objects.equals(lambda, that.lambda)
            && Objects.equals(gamma, that.gamma)
            && Objects.equals(eta, that.eta)
            && Objects.equals(maxTrees, that.maxTrees)
            && Objects.equals(featureBagFraction, that.featureBagFraction)
            && Objects.equals(numTopFeatureImportanceValues, that.numTopFeatureImportanceValues)
            && Objects.equals(alpha, that.alpha)
            && Objects.equals(etaGrowthRatePerTree, that.etaGrowthRatePerTree)
            && Objects.equals(softTreeDepthLimit, that.softTreeDepthLimit)
            && Objects.equals(softTreeDepthTolerance, that.softTreeDepthTolerance)
            && Objects.equals(downsampleFactor, that.downsampleFactor)
            && Objects.equals(maxOptimizationRoundsPerHyperparameter, that.maxOptimizationRoundsPerHyperparameter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            lambda,
            gamma,
            eta,
            maxTrees,
            featureBagFraction,
            numTopFeatureImportanceValues,
            alpha,
            etaGrowthRatePerTree,
            softTreeDepthLimit,
            softTreeDepthTolerance,
            downsampleFactor,
            maxOptimizationRoundsPerHyperparameter
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Double lambda;
        private Double gamma;
        private Double eta;
        private Integer maxTrees;
        private Double featureBagFraction;
        private Integer numTopFeatureImportanceValues;
        private Double alpha;
        private Double etaGrowthRatePerTree;
        private Double softTreeDepthLimit;
        private Double softTreeDepthTolerance;
        private Double downsampleFactor;
        private Integer maxOptimizationRoundsPerHyperparameter;

        private Builder() {}

        Builder(BoostedTreeParams params) {
            this.lambda = params.lambda;
            this.gamma = params.gamma;
            this.eta = params.eta;
            this.maxTrees = params.maxTrees;
            this.featureBagFraction = params.featureBagFraction;
            this.numTopFeatureImportanceValues = params.numTopFeatureImportanceValues;
            this.alpha = params.alpha;
            this.etaGrowthRatePerTree = params.etaGrowthRatePerTree;
            this.softTreeDepthLimit = params.softTreeDepthLimit;
            this.softTreeDepthTolerance = params.softTreeDepthTolerance;
            this.downsampleFactor = params.downsampleFactor;
            this.maxOptimizationRoundsPerHyperparameter = params.maxOptimizationRoundsPerHyperparameter;
        }

        public Builder setLambda(Double lambda) {
            this.lambda = lambda;
            return this;
        }

        public Builder setGamma(Double gamma) {
            this.gamma = gamma;
            return this;
        }

        public Builder setEta(Double eta) {
            this.eta = eta;
            return this;
        }

        public Builder setMaxTrees(Integer maxTrees) {
            this.maxTrees = maxTrees;
            return this;
        }

        public Builder setFeatureBagFraction(Double featureBagFraction) {
            this.featureBagFraction = featureBagFraction;
            return this;
        }

        public Builder setNumTopFeatureImportanceValues(Integer numTopFeatureImportanceValues) {
            this.numTopFeatureImportanceValues = numTopFeatureImportanceValues;
            return this;
        }

        public Builder setAlpha(Double alpha) {
            this.alpha = alpha;
            return this;
        }

        public Builder setEtaGrowthRatePerTree(Double etaGrowthRatePerTree) {
            this.etaGrowthRatePerTree = etaGrowthRatePerTree;
            return this;
        }

        public Builder setSoftTreeDepthLimit(Double softTreeDepthLimit) {
            this.softTreeDepthLimit = softTreeDepthLimit;
            return this;
        }

        public Builder setSoftTreeDepthTolerance(Double softTreeDepthTolerance) {
            this.softTreeDepthTolerance = softTreeDepthTolerance;
            return this;
        }

        public Builder setDownsampleFactor(Double downsampleFactor) {
            this.downsampleFactor = downsampleFactor;
            return this;
        }

        public Builder setMaxOptimizationRoundsPerHyperparameter(Integer maxOptimizationRoundsPerHyperparameter) {
            this.maxOptimizationRoundsPerHyperparameter = maxOptimizationRoundsPerHyperparameter;
            return this;
        }

        public BoostedTreeParams build() {
            return new BoostedTreeParams(
                lambda,
                gamma,
                eta,
                maxTrees,
                featureBagFraction,
                numTopFeatureImportanceValues,
                alpha,
                etaGrowthRatePerTree,
                softTreeDepthLimit,
                softTreeDepthTolerance,
                downsampleFactor,
                maxOptimizationRoundsPerHyperparameter
            );
        }
    }
}
