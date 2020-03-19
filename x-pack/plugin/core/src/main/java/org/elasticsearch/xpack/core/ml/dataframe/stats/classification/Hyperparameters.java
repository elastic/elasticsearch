/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.classification;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class Hyperparameters implements ToXContentObject, Writeable {

    public static final ParseField CLASS_ASSIGNMENT_OBJECTIVE = new ParseField("class_assignment_objective");
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

    public static Hyperparameters fromXContent(XContentParser parser, boolean ignoreUnknownFields) {
        return createParser(ignoreUnknownFields).apply(parser, null);
    }

    private static ConstructingObjectParser<Hyperparameters, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Hyperparameters, Void> parser = new ConstructingObjectParser<>("classification_hyperparameters",
            ignoreUnknownFields,
            a -> new Hyperparameters(
                (String) a[0],
                (double) a[1],
                (double) a[2],
                (double) a[3],
                (double) a[4],
                (int) a[5],
                (int) a[6],
                (int) a[7],
                (int) a[8],
                (int) a[9],
                (double) a[10],
                (double) a[11],
                (double) a[12],
                (double) a[13],
                (double) a[14]
            ));

        parser.declareString(constructorArg(), CLASS_ASSIGNMENT_OBJECTIVE);
        parser.declareDouble(constructorArg(), DOWNSAMPLE_FACTOR);
        parser.declareDouble(constructorArg(), ETA);
        parser.declareDouble(constructorArg(), ETA_GROWTH_RATE_PER_TREE);
        parser.declareDouble(constructorArg(), FEATURE_BAG_FRACTION);
        parser.declareInt(constructorArg(), MAX_ATTEMPTS_TO_ADD_TREE);
        parser.declareInt(constructorArg(), MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER);
        parser.declareInt(constructorArg(), MAX_TREES);
        parser.declareInt(constructorArg(), NUM_FOLDS);
        parser.declareInt(constructorArg(), NUM_SPLITS_PER_FEATURE);
        parser.declareDouble(constructorArg(), REGULARIZATION_DEPTH_PENALTY_MULTIPLIER);
        parser.declareDouble(constructorArg(), REGULARIZATION_LEAF_WEIGHT_PENALTY_MULTIPLIER);
        parser.declareDouble(constructorArg(), REGULARIZATION_SOFT_TREE_DEPTH_LIMIT);
        parser.declareDouble(constructorArg(), REGULARIZATION_SOFT_TREE_DEPTH_TOLERANCE);
        parser.declareDouble(constructorArg(), REGULARIZATION_TREE_SIZE_PENALTY_MULTIPLIER);

        return parser;
    }

    private final String classAssignmentObjective;
    private final double downsampleFactor;
    private final double eta;
    private final double etaGrowthRatePerTree;
    private final double featureBagFraction;
    private final int maxAttemptsToAddTree;
    private final int maxOptimizationRoundsPerHyperparameter;
    private final int maxTrees;
    private final int numFolds;
    private final int numSplitsPerFeature;
    private final double regularizationDepthPenaltyMultiplier;
    private final double regularizationLeafWeightPenaltyMultiplier;
    private final double regularizationSoftTreeDepthLimit;
    private final double regularizationSoftTreeDepthTolerance;
    private final double regularizationTreeSizePenaltyMultiplier;

    public Hyperparameters(String classAssignmentObjective,
                           double downsampleFactor,
                           double eta,
                           double etaGrowthRatePerTree,
                           double featureBagFraction,
                           int maxAttemptsToAddTree,
                           int maxOptimizationRoundsPerHyperparameter,
                           int maxTrees,
                           int numFolds,
                           int numSplitsPerFeature,
                           double regularizationDepthPenaltyMultiplier,
                           double regularizationLeafWeightPenaltyMultiplier,
                           double regularizationSoftTreeDepthLimit,
                           double regularizationSoftTreeDepthTolerance,
                           double regularizationTreeSizePenaltyMultiplier) {
        this.classAssignmentObjective = Objects.requireNonNull(classAssignmentObjective);
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

    public Hyperparameters(StreamInput in) throws IOException {
        this.classAssignmentObjective = in.readString();
        this.downsampleFactor = in.readDouble();
        this.eta = in.readDouble();
        this.etaGrowthRatePerTree = in.readDouble();
        this.featureBagFraction = in.readDouble();
        this.maxAttemptsToAddTree = in.readVInt();
        this.maxOptimizationRoundsPerHyperparameter = in.readVInt();
        this.maxTrees = in.readVInt();
        this.numFolds = in.readVInt();
        this.numSplitsPerFeature = in.readVInt();
        this.regularizationDepthPenaltyMultiplier = in.readDouble();
        this.regularizationLeafWeightPenaltyMultiplier = in.readDouble();
        this.regularizationSoftTreeDepthLimit = in.readDouble();
        this.regularizationSoftTreeDepthTolerance = in.readDouble();
        this.regularizationTreeSizePenaltyMultiplier = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(classAssignmentObjective);
        out.writeDouble(downsampleFactor);
        out.writeDouble(eta);
        out.writeDouble(etaGrowthRatePerTree);
        out.writeDouble(featureBagFraction);
        out.writeVInt(maxAttemptsToAddTree);
        out.writeVInt(maxOptimizationRoundsPerHyperparameter);
        out.writeVInt(maxTrees);
        out.writeVInt(numFolds);
        out.writeVInt(numSplitsPerFeature);
        out.writeDouble(regularizationDepthPenaltyMultiplier);
        out.writeDouble(regularizationLeafWeightPenaltyMultiplier);
        out.writeDouble(regularizationSoftTreeDepthLimit);
        out.writeDouble(regularizationSoftTreeDepthTolerance);
        out.writeDouble(regularizationTreeSizePenaltyMultiplier);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLASS_ASSIGNMENT_OBJECTIVE.getPreferredName(), classAssignmentObjective);
        builder.field(DOWNSAMPLE_FACTOR.getPreferredName(), downsampleFactor);
        builder.field(ETA.getPreferredName(), eta);
        builder.field(ETA_GROWTH_RATE_PER_TREE.getPreferredName(), etaGrowthRatePerTree);
        builder.field(FEATURE_BAG_FRACTION.getPreferredName(), featureBagFraction);
        builder.field(MAX_ATTEMPTS_TO_ADD_TREE.getPreferredName(), maxAttemptsToAddTree);
        builder.field(MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER.getPreferredName(), maxOptimizationRoundsPerHyperparameter);
        builder.field(MAX_TREES.getPreferredName(), maxTrees);
        builder.field(NUM_FOLDS.getPreferredName(), numFolds);
        builder.field(NUM_SPLITS_PER_FEATURE.getPreferredName(), numSplitsPerFeature);
        builder.field(REGULARIZATION_DEPTH_PENALTY_MULTIPLIER.getPreferredName(), regularizationDepthPenaltyMultiplier);
        builder.field(REGULARIZATION_LEAF_WEIGHT_PENALTY_MULTIPLIER.getPreferredName(), regularizationLeafWeightPenaltyMultiplier);
        builder.field(REGULARIZATION_SOFT_TREE_DEPTH_LIMIT.getPreferredName(), regularizationSoftTreeDepthLimit);
        builder.field(REGULARIZATION_SOFT_TREE_DEPTH_TOLERANCE.getPreferredName(), regularizationSoftTreeDepthTolerance);
        builder.field(REGULARIZATION_TREE_SIZE_PENALTY_MULTIPLIER.getPreferredName(), regularizationTreeSizePenaltyMultiplier);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Hyperparameters that = (Hyperparameters) o;
        return Objects.equals(classAssignmentObjective, that.classAssignmentObjective)
            && downsampleFactor == that.downsampleFactor
            && eta == that.eta
            && etaGrowthRatePerTree == that.etaGrowthRatePerTree
            && featureBagFraction == that.featureBagFraction
            && maxAttemptsToAddTree == that.maxAttemptsToAddTree
            && maxOptimizationRoundsPerHyperparameter == that.maxOptimizationRoundsPerHyperparameter
            && maxTrees == that.maxTrees
            && numFolds == that.numFolds
            && numSplitsPerFeature == that.numSplitsPerFeature
            && regularizationDepthPenaltyMultiplier == that.regularizationDepthPenaltyMultiplier
            && regularizationLeafWeightPenaltyMultiplier == that.regularizationLeafWeightPenaltyMultiplier
            && regularizationSoftTreeDepthLimit == that.regularizationSoftTreeDepthLimit
            && regularizationSoftTreeDepthTolerance == that.regularizationSoftTreeDepthTolerance
            && regularizationTreeSizePenaltyMultiplier == that.regularizationTreeSizePenaltyMultiplier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            classAssignmentObjective,
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
