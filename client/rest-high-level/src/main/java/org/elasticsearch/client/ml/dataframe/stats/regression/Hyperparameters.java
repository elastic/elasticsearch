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

    public static final ParseField ALPHA = new ParseField("alpha");
    public static final ParseField DOWNSAMPLE_FACTOR = new ParseField("downsample_factor");
    public static final ParseField ETA = new ParseField("eta");
    public static final ParseField ETA_GROWTH_RATE_PER_TREE = new ParseField("eta_growth_rate_per_tree");
    public static final ParseField FEATURE_BAG_FRACTION = new ParseField("feature_bag_fraction");
    public static final ParseField GAMMA = new ParseField("gamma");
    public static final ParseField LAMBDA = new ParseField("lambda");
    public static final ParseField MAX_ATTEMPTS_TO_ADD_TREE = new ParseField("max_attempts_to_add_tree");
    public static final ParseField MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER = new ParseField(
        "max_optimization_rounds_per_hyperparameter");
    public static final ParseField MAX_TREES = new ParseField("max_trees");
    public static final ParseField NUM_FOLDS = new ParseField("num_folds");
    public static final ParseField NUM_SPLITS_PER_FEATURE = new ParseField("num_splits_per_feature");
    public static final ParseField SOFT_TREE_DEPTH_LIMIT = new ParseField("soft_tree_depth_limit");
    public static final ParseField SOFT_TREE_DEPTH_TOLERANCE = new ParseField("soft_tree_depth_tolerance");

    public static ConstructingObjectParser<Hyperparameters, Void> PARSER = new ConstructingObjectParser<>("regression_hyperparameters",
        true,
        a -> new Hyperparameters(
            (Double) a[0],
            (Double) a[1],
            (Double) a[2],
            (Double) a[3],
            (Double) a[4],
            (Double) a[5],
            (Double) a[6],
            (Integer) a[7],
            (Integer) a[8],
            (Integer) a[9],
            (Integer) a[10],
            (Integer) a[11],
            (Double) a[12],
            (Double) a[13]
        ));

    static {
        PARSER.declareDouble(optionalConstructorArg(), ALPHA);
        PARSER.declareDouble(optionalConstructorArg(), DOWNSAMPLE_FACTOR);
        PARSER.declareDouble(optionalConstructorArg(), ETA);
        PARSER.declareDouble(optionalConstructorArg(), ETA_GROWTH_RATE_PER_TREE);
        PARSER.declareDouble(optionalConstructorArg(), FEATURE_BAG_FRACTION);
        PARSER.declareDouble(optionalConstructorArg(), GAMMA);
        PARSER.declareDouble(optionalConstructorArg(), LAMBDA);
        PARSER.declareInt(optionalConstructorArg(), MAX_ATTEMPTS_TO_ADD_TREE);
        PARSER.declareInt(optionalConstructorArg(), MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER);
        PARSER.declareInt(optionalConstructorArg(), MAX_TREES);
        PARSER.declareInt(optionalConstructorArg(), NUM_FOLDS);
        PARSER.declareInt(optionalConstructorArg(), NUM_SPLITS_PER_FEATURE);
        PARSER.declareDouble(optionalConstructorArg(), SOFT_TREE_DEPTH_LIMIT);
        PARSER.declareDouble(optionalConstructorArg(), SOFT_TREE_DEPTH_TOLERANCE);
    }

    private final Double alpha;
    private final Double downsampleFactor;
    private final Double eta;
    private final Double etaGrowthRatePerTree;
    private final Double featureBagFraction;
    private final Double gamma;
    private final Double lambda;
    private final Integer maxAttemptsToAddTree;
    private final Integer maxOptimizationRoundsPerHyperparameter;
    private final Integer maxTrees;
    private final Integer numFolds;
    private final Integer numSplitsPerFeature;
    private final Double softTreeDepthLimit;
    private final Double softTreeDepthTolerance;

    public Hyperparameters(Double alpha,
                           Double downsampleFactor,
                           Double eta,
                           Double etaGrowthRatePerTree,
                           Double featureBagFraction,
                           Double gamma,
                           Double lambda,
                           Integer maxAttemptsToAddTree,
                           Integer maxOptimizationRoundsPerHyperparameter,
                           Integer maxTrees,
                           Integer numFolds,
                           Integer numSplitsPerFeature,
                           Double softTreeDepthLimit,
                           Double softTreeDepthTolerance) {
        this.alpha = alpha;
        this.downsampleFactor = downsampleFactor;
        this.eta = eta;
        this.etaGrowthRatePerTree = etaGrowthRatePerTree;
        this.featureBagFraction = featureBagFraction;
        this.gamma = gamma;
        this.lambda = lambda;
        this.maxAttemptsToAddTree = maxAttemptsToAddTree;
        this.maxOptimizationRoundsPerHyperparameter = maxOptimizationRoundsPerHyperparameter;
        this.maxTrees = maxTrees;
        this.numFolds = numFolds;
        this.numSplitsPerFeature = numSplitsPerFeature;
        this.softTreeDepthLimit = softTreeDepthLimit;
        this.softTreeDepthTolerance = softTreeDepthTolerance;
    }

    public Double getAlpha() {
        return alpha;
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

    public Double getGamma() {
        return gamma;
    }

    public Double getLambda() {
        return lambda;
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

    public Double getSoftTreeDepthLimit() {
        return softTreeDepthLimit;
    }

    public Double getSoftTreeDepthTolerance() {
        return softTreeDepthTolerance;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (alpha != null) {
            builder.field(ALPHA.getPreferredName(), alpha);
        }
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
        if (gamma != null) {
            builder.field(GAMMA.getPreferredName(), gamma);
        }
        if (lambda != null) {
            builder.field(LAMBDA.getPreferredName(), lambda);
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
        if (softTreeDepthLimit != null) {
            builder.field(SOFT_TREE_DEPTH_LIMIT.getPreferredName(), softTreeDepthLimit);
        }
        if (softTreeDepthTolerance != null) {
            builder.field(SOFT_TREE_DEPTH_TOLERANCE.getPreferredName(), softTreeDepthTolerance);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Hyperparameters that = (Hyperparameters) o;
        return Objects.equals(alpha, that.alpha)
            && Objects.equals(downsampleFactor, that.downsampleFactor)
            && Objects.equals(eta, that.eta)
            && Objects.equals(etaGrowthRatePerTree, that.etaGrowthRatePerTree)
            && Objects.equals(featureBagFraction, that.featureBagFraction)
            && Objects.equals(gamma, that.gamma)
            && Objects.equals(lambda, that.lambda)
            && Objects.equals(maxAttemptsToAddTree, that.maxAttemptsToAddTree)
            && Objects.equals(maxOptimizationRoundsPerHyperparameter, that.maxOptimizationRoundsPerHyperparameter)
            && Objects.equals(maxTrees, that.maxTrees)
            && Objects.equals(numFolds, that.numFolds)
            && Objects.equals(numSplitsPerFeature, that.numSplitsPerFeature)
            && Objects.equals(softTreeDepthLimit, that.softTreeDepthLimit)
            && Objects.equals(softTreeDepthTolerance, that.softTreeDepthTolerance);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            alpha,
            downsampleFactor,
            eta,
            etaGrowthRatePerTree,
            featureBagFraction,
            gamma,
            lambda,
            maxAttemptsToAddTree,
            maxOptimizationRoundsPerHyperparameter,
            maxTrees,
            numFolds,
            numSplitsPerFeature,
            softTreeDepthLimit,
            softTreeDepthTolerance
        );
    }
}
