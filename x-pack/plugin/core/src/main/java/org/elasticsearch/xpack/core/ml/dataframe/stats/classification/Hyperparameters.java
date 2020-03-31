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
                (double) a[5],
                (double) a[6],
                (double) a[7],
                (int) a[8],
                (int) a[9],
                (int) a[10],
                (int) a[11],
                (int) a[12],
                (double) a[13],
                (double) a[14]
            ));

        parser.declareString(constructorArg(), CLASS_ASSIGNMENT_OBJECTIVE);
        parser.declareDouble(constructorArg(), ALPHA);
        parser.declareDouble(constructorArg(), DOWNSAMPLE_FACTOR);
        parser.declareDouble(constructorArg(), ETA);
        parser.declareDouble(constructorArg(), ETA_GROWTH_RATE_PER_TREE);
        parser.declareDouble(constructorArg(), FEATURE_BAG_FRACTION);
        parser.declareDouble(constructorArg(), GAMMA);
        parser.declareDouble(constructorArg(), LAMBDA);
        parser.declareInt(constructorArg(), MAX_ATTEMPTS_TO_ADD_TREE);
        parser.declareInt(constructorArg(), MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER);
        parser.declareInt(constructorArg(), MAX_TREES);
        parser.declareInt(constructorArg(), NUM_FOLDS);
        parser.declareInt(constructorArg(), NUM_SPLITS_PER_FEATURE);
        parser.declareDouble(constructorArg(), SOFT_TREE_DEPTH_LIMIT);
        parser.declareDouble(constructorArg(), SOFT_TREE_DEPTH_TOLERANCE);

        return parser;
    }

    private final String classAssignmentObjective;
    private final double alpha;
    private final double downsampleFactor;
    private final double eta;
    private final double etaGrowthRatePerTree;
    private final double featureBagFraction;
    private final double gamma;
    private final double lambda;
    private final int maxAttemptsToAddTree;
    private final int maxOptimizationRoundsPerHyperparameter;
    private final int maxTrees;
    private final int numFolds;
    private final int numSplitsPerFeature;
    private final double softTreeDepthLimit;
    private final double softTreeDepthTolerance;

    public Hyperparameters(String classAssignmentObjective,
                           double alpha,
                           double downsampleFactor,
                           double eta,
                           double etaGrowthRatePerTree,
                           double featureBagFraction,
                           double gamma,
                           double lambda,
                           int maxAttemptsToAddTree,
                           int maxOptimizationRoundsPerHyperparameter,
                           int maxTrees,
                           int numFolds,
                           int numSplitsPerFeature,
                           double softTreeDepthLimit,
                           double softTreeDepthTolerance) {
        this.classAssignmentObjective = Objects.requireNonNull(classAssignmentObjective);
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

    public Hyperparameters(StreamInput in) throws IOException {
        this.classAssignmentObjective = in.readString();
        this.alpha = in.readDouble();
        this.downsampleFactor = in.readDouble();
        this.eta = in.readDouble();
        this.etaGrowthRatePerTree = in.readDouble();
        this.featureBagFraction = in.readDouble();
        this.gamma = in.readDouble();
        this.lambda = in.readDouble();
        this.maxAttemptsToAddTree = in.readVInt();
        this.maxOptimizationRoundsPerHyperparameter = in.readVInt();
        this.maxTrees = in.readVInt();
        this.numFolds = in.readVInt();
        this.numSplitsPerFeature = in.readVInt();
        this.softTreeDepthLimit = in.readDouble();
        this.softTreeDepthTolerance = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(classAssignmentObjective);
        out.writeDouble(alpha);
        out.writeDouble(downsampleFactor);
        out.writeDouble(eta);
        out.writeDouble(etaGrowthRatePerTree);
        out.writeDouble(featureBagFraction);
        out.writeDouble(gamma);
        out.writeDouble(lambda);
        out.writeVInt(maxAttemptsToAddTree);
        out.writeVInt(maxOptimizationRoundsPerHyperparameter);
        out.writeVInt(maxTrees);
        out.writeVInt(numFolds);
        out.writeVInt(numSplitsPerFeature);
        out.writeDouble(softTreeDepthLimit);
        out.writeDouble(softTreeDepthTolerance);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLASS_ASSIGNMENT_OBJECTIVE.getPreferredName(), classAssignmentObjective);
        builder.field(ALPHA.getPreferredName(), alpha);
        builder.field(DOWNSAMPLE_FACTOR.getPreferredName(), downsampleFactor);
        builder.field(ETA.getPreferredName(), eta);
        builder.field(ETA_GROWTH_RATE_PER_TREE.getPreferredName(), etaGrowthRatePerTree);
        builder.field(FEATURE_BAG_FRACTION.getPreferredName(), featureBagFraction);
        builder.field(GAMMA.getPreferredName(), gamma);
        builder.field(LAMBDA.getPreferredName(), lambda);
        builder.field(MAX_ATTEMPTS_TO_ADD_TREE.getPreferredName(), maxAttemptsToAddTree);
        builder.field(MAX_OPTIMIZATION_ROUNDS_PER_HYPERPARAMETER.getPreferredName(), maxOptimizationRoundsPerHyperparameter);
        builder.field(MAX_TREES.getPreferredName(), maxTrees);
        builder.field(NUM_FOLDS.getPreferredName(), numFolds);
        builder.field(NUM_SPLITS_PER_FEATURE.getPreferredName(), numSplitsPerFeature);
        builder.field(SOFT_TREE_DEPTH_LIMIT.getPreferredName(), softTreeDepthLimit);
        builder.field(SOFT_TREE_DEPTH_TOLERANCE.getPreferredName(), softTreeDepthTolerance);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Hyperparameters that = (Hyperparameters) o;
        return Objects.equals(classAssignmentObjective, that.classAssignmentObjective)
            && alpha == that.alpha
            && downsampleFactor == that.downsampleFactor
            && eta == that.eta
            && etaGrowthRatePerTree == that.etaGrowthRatePerTree
            && featureBagFraction == that.featureBagFraction
            && gamma == that.gamma
            && lambda == that.lambda
            && maxAttemptsToAddTree == that.maxAttemptsToAddTree
            && maxOptimizationRoundsPerHyperparameter == that.maxOptimizationRoundsPerHyperparameter
            && maxTrees == that.maxTrees
            && numFolds == that.numFolds
            && numSplitsPerFeature == that.numSplitsPerFeature
            && softTreeDepthLimit == that.softTreeDepthLimit
            && softTreeDepthTolerance == that.softTreeDepthTolerance;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            classAssignmentObjective,
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
