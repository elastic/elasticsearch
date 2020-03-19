/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml.dataframe.stats.regression;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class Hyperparameters implements ToXContentObject {

    public static final ParseField DOWNSAMPLE_FACTOR = new ParseField("downsample_factor");
    public static final ParseField ETA = new ParseField("eta");
    public static final ParseField ETA_GROWTH_RATE_PER_TREE = new ParseField("eta_growth_rate_per_tree");
    public static final ParseField FEATURE_BAG_FRACTION = new ParseField("feature_bag_fraction");
    public static final ParseField MAX_ATTEMPTS_TO_ADD_TREE = new ParseField("max_attempts_to_add_tree");
    public static final ParseField MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER = new ParseField(
        "max_optimization_rounds_per_hyperparameter");
    public static final ParseField MAX_TREES = new ParseField("max_trees");
    public static final ParseField NUM_FOLDS = new ParseField("num_folds");
    public static final ParseField NUM_SPLITS_PER_FEATURE = new ParseField("num_splits_per_feature");
    public static final ParseField REGULARIZATION_DEPTH_PENALTY_MULTIPLIER = new ParseField("regularization_depth_penalty_multiplier");
    public static final ParseField REGULARIZATION_LEAF_WEIGHT_PENALTY_MULTIPLIER
        = new ParseField("regularization_leaf_weight_penalty_multiplier");
    public static final ParseField REGULARIZATION_SOFT_TREE_DEPTH_LIMIT = new ParseField("regularization_soft_tree_depth_limit");
    public static final ParseField REGULARIZATION_SOFT_TREE_DEPTH_TOLERANCE = new ParseField("regularization_soft_tree_depth_tolerance");
    public static final ParseField REGULARIZATION_TREE_SIZE_PENALTY_MULTIPLIER =
        new ParseField("regularization_tree_size_penalty_multiplier");

    public static ConstructingObjectParser<Hyperparameters, Void> PARSER = new ConstructingObjectParser<>("regression_hyperparameters",
        true,
        a -> new Hyperparameters(
            (Double) a[0],
            (Double) a[1],
            (Double) a[2],
            (Double) a[3],
            (Integer) a[4],
            (Integer) a[5],
            (Integer) a[6],
            (Integer) a[7],
            (Integer) a[8],
            (Double) a[9],
            (Double) a[10],
            (Double) a[11],
            (Double) a[12],
            (Double) a[13]
        ));

    static {
        PARSER.declareDouble(optionalConstructorArg(), DOWNSAMPLE_FACTOR);
        PARSER.declareDouble(optionalConstructorArg(), ETA);
        PARSER.declareDouble(optionalConstructorArg(), ETA_GROWTH_RATE_PER_TREE);
        PARSER.declareDouble(optionalConstructorArg(), FEATURE_BAG_FRACTION);
        PARSER.declareInt(optionalConstructorArg(), MAX_ATTEMPTS_TO_ADD_TREE);
        PARSER.declareInt(optionalConstructorArg(), MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER);
        PARSER.declareInt(optionalConstructorArg(), MAX_TREES);
        PARSER.declareInt(optionalConstructorArg(), NUM_FOLDS);
        PARSER.declareInt(optionalConstructorArg(), NUM_SPLITS_PER_FEATURE);
        PARSER.declareDouble(optionalConstructorArg(), REGULARIZATION_DEPTH_PENALTY_MULTIPLIER);
        PARSER.declareDouble(optionalConstructorArg(), REGULARIZATION_LEAF_WEIGHT_PENALTY_MULTIPLIER);
        PARSER.declareDouble(optionalConstructorArg(), REGULARIZATION_SOFT_TREE_DEPTH_LIMIT);
        PARSER.declareDouble(optionalConstructorArg(), REGULARIZATION_SOFT_TREE_DEPTH_TOLERANCE);
        PARSER.declareDouble(optionalConstructorArg(), REGULARIZATION_TREE_SIZE_PENALTY_MULTIPLIER);
    }

    private final Double downsampleFactor;
    private final Double eta;
    private final Double etaGrowthRatePerTree;
    private final Double featureBagFraction;
    private final Integer maxAttemptsToAddTree;
    private final Integer maxOptimizationRoundsPerHyperparameter;
    private final Integer maxTrees;
    private final Integer numFolds;
    private final Integer numSplitsPerFeature;
    private final Double regularizationDepthPenaltyMultiplier;
    private final Double regularizationLeafWeightPenaltyMultiplier;
    private final Double regularizationSoftTreeDepthLimit;
    private final Double regularizationSoftTreeDepthTolerance;
    private final Double regularizationTreeSizePenaltyMultiplier;

    public Hyperparameters(Double downsampleFactor,
                           Double eta,
                           Double etaGrowthRatePerTree,
                           Double featureBagFraction,
                           Integer maxAttemptsToAddTree,
                           Integer maxOptimizationRoundsPerHyperparameter,
                           Integer maxTrees,
                           Integer numFolds,
                           Integer numSplitsPerFeature,
                           Double regularizationDepthPenaltyMultiplier,
                           Double regularizationLeafWeightPenaltyMultiplier,
                           Double regularizationSoftTreeDepthLimit,
                           Double regularizationSoftTreeDepthTolerance,
                           Double regularizationTreeSizePenaltyMultiplier) {
        this.downsampleFactor = downsampleFactor;
        this.eta = eta;
        this.etaGrowthRatePerTree = etaGrowthRatePerTree;
        this.featureBagFraction = featureBagFraction;
        this.maxAttemptsToAddTree = maxAttemptsToAddTree;
        this.maxOptimizationRoundsPerHyperparameter = maxOptimizationRoundsPerHyperparameter;
        this.maxTrees = maxTrees;
        this.numFolds = numFolds;
        this.numSplitsPerFeature = numSplitsPerFeature;
        this.regularizationDepthPenaltyMultiplier = regularizationDepthPenaltyMultiplier;
        this.regularizationLeafWeightPenaltyMultiplier = regularizationLeafWeightPenaltyMultiplier;
        this.regularizationSoftTreeDepthLimit = regularizationSoftTreeDepthLimit;
        this.regularizationSoftTreeDepthTolerance = regularizationSoftTreeDepthTolerance;
        this.regularizationTreeSizePenaltyMultiplier = regularizationTreeSizePenaltyMultiplier;
    }

    public Double getDownsampleFactor() {
        return downsampleFactor;
    }

    public Double getEta() {
        return eta;
    }

    public Double getEtaGrowthRatePerTree() {
        return etaGrowthRatePerTree;
    }

    public Double getFeatureBagFraction() {
        return featureBagFraction;
    }

    public Integer getMaxAttemptsToAddTree() {
        return maxAttemptsToAddTree;
    }

    public Integer getMaxOptimizationRoundsPerHyperparameter() {
        return maxOptimizationRoundsPerHyperparameter;
    }

    public Integer getMaxTrees() {
        return maxTrees;
    }

    public Integer getNumFolds() {
        return numFolds;
    }

    public Integer getNumSplitsPerFeature() {
        return numSplitsPerFeature;
    }

    public Double getRegularizationDepthPenaltyMultiplier() {
        return regularizationDepthPenaltyMultiplier;
    }

    public Double getRegularizationLeafWeightPenaltyMultiplier() {
        return regularizationLeafWeightPenaltyMultiplier;
    }

    public Double getRegularizationSoftTreeDepthLimit() {
        return regularizationSoftTreeDepthLimit;
    }

    public Double getRegularizationSoftTreeDepthTolerance() {
        return regularizationSoftTreeDepthTolerance;
    }

    public Double getRegularizationTreeSizePenaltyMultiplier() {
        return regularizationTreeSizePenaltyMultiplier;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (downsampleFactor != null) {
            builder.field(DOWNSAMPLE_FACTOR.getPreferredName(), downsampleFactor);
        }
        if (eta != null) {
            builder.field(ETA.getPreferredName(), eta);
        }
        if (etaGrowthRatePerTree != null) {
            builder.field(ETA_GROWTH_RATE_PER_TREE.getPreferredName(), etaGrowthRatePerTree);
        }
        if (featureBagFraction != null) {
            builder.field(FEATURE_BAG_FRACTION.getPreferredName(), featureBagFraction);
        }
        if (maxAttemptsToAddTree != null) {
            builder.field(MAX_ATTEMPTS_TO_ADD_TREE.getPreferredName(), maxAttemptsToAddTree);
        }
        if (maxOptimizationRoundsPerHyperparameter != null) {
            builder.field(MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER.getPreferredName(), maxOptimizationRoundsPerHyperparameter);
        }
        if (maxTrees != null) {
            builder.field(MAX_TREES.getPreferredName(), maxTrees);
        }
        if (numFolds != null) {
            builder.field(NUM_FOLDS.getPreferredName(), numFolds);
        }
        if (numSplitsPerFeature != null) {
            builder.field(NUM_SPLITS_PER_FEATURE.getPreferredName(), numSplitsPerFeature);
        }
        if (regularizationDepthPenaltyMultiplier != null) {
            builder.field(REGULARIZATION_DEPTH_PENALTY_MULTIPLIER.getPreferredName(), regularizationDepthPenaltyMultiplier);
        }
        if (regularizationLeafWeightPenaltyMultiplier != null) {
            builder.field(REGULARIZATION_LEAF_WEIGHT_PENALTY_MULTIPLIER.getPreferredName(), regularizationLeafWeightPenaltyMultiplier);
        }
        if (regularizationSoftTreeDepthLimit != null) {
            builder.field(REGULARIZATION_SOFT_TREE_DEPTH_LIMIT.getPreferredName(), regularizationSoftTreeDepthLimit);
        }
        if (regularizationSoftTreeDepthTolerance != null) {
            builder.field(REGULARIZATION_SOFT_TREE_DEPTH_TOLERANCE.getPreferredName(), regularizationSoftTreeDepthTolerance);
        }
        if (regularizationTreeSizePenaltyMultiplier != null) {
            builder.field(REGULARIZATION_TREE_SIZE_PENALTY_MULTIPLIER.getPreferredName(), regularizationTreeSizePenaltyMultiplier);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Hyperparameters that = (Hyperparameters) o;
        return Objects.equals(downsampleFactor, that.downsampleFactor)
            && Objects.equals(eta, that.eta)
            && Objects.equals(etaGrowthRatePerTree, that.etaGrowthRatePerTree)
            && Objects.equals(featureBagFraction, that.featureBagFraction)
            && Objects.equals(maxAttemptsToAddTree, that.maxAttemptsToAddTree)
            && Objects.equals(maxOptimizationRoundsPerHyperparameter, that.maxOptimizationRoundsPerHyperparameter)
            && Objects.equals(maxTrees, that.maxTrees)
            && Objects.equals(numFolds, that.numFolds)
            && Objects.equals(numSplitsPerFeature, that.numSplitsPerFeature)
            && Objects.equals(regularizationDepthPenaltyMultiplier, that.regularizationDepthPenaltyMultiplier)
            && Objects.equals(regularizationLeafWeightPenaltyMultiplier, that.regularizationLeafWeightPenaltyMultiplier)
            && Objects.equals(regularizationSoftTreeDepthLimit, that.regularizationSoftTreeDepthLimit)
            && Objects.equals(regularizationSoftTreeDepthTolerance, that.regularizationSoftTreeDepthTolerance)
            && Objects.equals(regularizationTreeSizePenaltyMultiplier, that.regularizationTreeSizePenaltyMultiplier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            downsampleFactor,
            eta,
            etaGrowthRatePerTree,
            featureBagFraction,
            maxAttemptsToAddTree,
            maxOptimizationRoundsPerHyperparameter,
            maxTrees,
            numFolds,
            numSplitsPerFeature,
            regularizationDepthPenaltyMultiplier,
            regularizationLeafWeightPenaltyMultiplier,
            regularizationSoftTreeDepthLimit,
            regularizationSoftTreeDepthTolerance,
            regularizationTreeSizePenaltyMultiplier
        );
    }
}
